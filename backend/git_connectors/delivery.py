"""
Idempotent webhook delivery pipeline.

A provider delivery is fixed by its delivery id (``X-GitHub-Delivery`` or a
digest of the raw body for GitLab), the installation/app identity and the
digest of the verified raw payload. The :class:`GitWebhookDelivery` ledger
drives replayable stages:

1. signature / token verification happens *before* any ledger row is written,
2. database effects (repository changes, deployments, previews) are committed
   together with the list of workflow starts they require,
3. Temporal starts are confirmed after commit; an ambiguous start response is
   resolved by querying the existing workflow, and unconfirmed starts are
   replayed on redelivery or API restart.

:class:`GitRefGeneration` arbitrates concurrent/out-of-order events so only
the current branch / pull request generation can publish.
"""

from dataclasses import asdict, dataclass
from datetime import timedelta
import hashlib
import threading
from typing import Any, Callable, Optional

from django.db import IntegrityError, connection, transaction

from compose.dtos import ComposeStackSnapshot
from temporal.client import SignalWorkflowArg, StartWorkflowArg, TemporalClient
from temporal.shared import (
    CancelDeploymentSignalInput,
    ComposeStackArchiveDetails,
    ComposeStackDeploymentDetails,
    DeploymentDetails,
    EnvironmentDetails,
)
from temporal.workflows import (
    ArchiveEnvWorkflow,
    CreateEnvNetworkWorkflow,
    DelayedArchiveEnvWorkflow,
    DeployComposeStackWorkflow,
    DeployDockerServiceWorkflow,
    DeployGitServiceWorkflow,
)
from zane_api.dtos import DeploymentChangeDto, ServiceSnapshot

from .models import ZERO_COMMIT_SHA, GitRefGeneration, GitWebhookDelivery


# --------------------------------------------------------------------------- #
# Workflow registry: stable names used in the serialized delivery ledger.
# --------------------------------------------------------------------------- #


def _deployment_details_from_dict(data: dict[str, Any]) -> DeploymentDetails:
    return DeploymentDetails(
        hash=data["hash"],
        slot=data["slot"],
        unprefixed_hash=data["unprefixed_hash"],
        queued_at=data["queued_at"],
        workflow_id=data["workflow_id"],
        service=ServiceSnapshot.from_dict(data["service"]),
        ignore_build_cache=data.get("ignore_build_cache", False),
        changes=[
            DeploymentChangeDto.from_dict(change) for change in data.get("changes", [])
        ],
        pause_at_step=data.get("pause_at_step", 0),
        network_alias=data.get("network_alias"),
        commit_sha=data.get("commit_sha"),
        image_tag=data.get("image_tag"),
    )


def _environment_details_from_dict(data: dict[str, Any]) -> EnvironmentDetails:
    return EnvironmentDetails(
        id=data["id"],
        name=data["name"],
        project_id=data["project_id"],
        compose_stacks=[
            ComposeStackArchiveDetails(
                stack=ComposeStackSnapshot.from_dict(item["stack"])
            )
            for item in data.get("compose_stacks", [])
        ],
    )


WORKFLOW_DEPLOY_GIT = "deploy-git-service-workflow"
WORKFLOW_DEPLOY_DOCKER = "deploy-docker-service-workflow"
WORKFLOW_DEPLOY_COMPOSE = "deploy-compose-stack"
WORKFLOW_CREATE_ENV = "create-env-network"
WORKFLOW_ARCHIVE_ENV = "archive-env"
WORKFLOW_DELAYED_ARCHIVE_ENV = "delayed-archive-env"

_WORKFLOW_NAME_BY_RUN: dict[Callable, str] = {
    DeployGitServiceWorkflow.run: WORKFLOW_DEPLOY_GIT,
    DeployDockerServiceWorkflow.run: WORKFLOW_DEPLOY_DOCKER,
    DeployComposeStackWorkflow.run: WORKFLOW_DEPLOY_COMPOSE,
    CreateEnvNetworkWorkflow.run: WORKFLOW_CREATE_ENV,
    ArchiveEnvWorkflow.run: WORKFLOW_ARCHIVE_ENV,
    DelayedArchiveEnvWorkflow.run: WORKFLOW_DELAYED_ARCHIVE_ENV,
}

_WORKFLOW_RUN_BY_NAME: dict[str, Callable] = {
    WORKFLOW_DEPLOY_GIT: DeployGitServiceWorkflow.run,
    WORKFLOW_DEPLOY_DOCKER: DeployDockerServiceWorkflow.run,
    WORKFLOW_DEPLOY_COMPOSE: DeployComposeStackWorkflow.run,
    WORKFLOW_CREATE_ENV: CreateEnvNetworkWorkflow.run,
    WORKFLOW_ARCHIVE_ENV: ArchiveEnvWorkflow.run,
    WORKFLOW_DELAYED_ARCHIVE_ENV: DelayedArchiveEnvWorkflow.run,
}

_PAYLOAD_FROM_DICT: dict[str, Callable[[dict[str, Any]], Any]] = {
    WORKFLOW_DEPLOY_GIT: _deployment_details_from_dict,
    WORKFLOW_DEPLOY_DOCKER: _deployment_details_from_dict,
    WORKFLOW_DEPLOY_COMPOSE: ComposeStackDeploymentDetails.from_dict,
    WORKFLOW_CREATE_ENV: _environment_details_from_dict,
    WORKFLOW_ARCHIVE_ENV: _environment_details_from_dict,
    WORKFLOW_DELAYED_ARCHIVE_ENV: _environment_details_from_dict,
}


# --------------------------------------------------------------------------- #
# Delivery identity / ledger
# --------------------------------------------------------------------------- #


def compute_payload_digest(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()


def resolve_delivery_id(
    provider_delivery_id: Optional[str], payload_digest: str
) -> str:
    # GitHub sends `X-GitHub-Delivery`; GitLab has no delivery header and older
    # GitHub deliveries may lack one, in which case the verified payload
    # digest is the stable identity (provider retries resend the same body).
    if provider_delivery_id:
        return provider_delivery_id
    return f"sha256:{payload_digest}"


def begin_delivery(
    *,
    provider: str,
    git_app_id: str,
    event_type: str,
    action: Optional[str],
    raw_body: bytes,
    provider_delivery_id: Optional[str],
    repository_url: str = "",
    ref_name: str = "",
    head_sha: Optional[str] = None,
    pr_number: Optional[int] = None,
) -> tuple[GitWebhookDelivery, bool]:
    """
    Fetch or create the stable ledger row for a *verified* delivery.

    Returns ``(delivery, created)``. The caller must have already validated
    the signature / token so invalid deliveries never reach this point.
    """
    payload_digest = compute_payload_digest(raw_body)
    delivery_id = resolve_delivery_id(provider_delivery_id, payload_digest)

    existing = GitWebhookDelivery.objects.filter(
        provider=provider,
        git_app_id=git_app_id,
        delivery_id=delivery_id,
    ).first()
    if existing is not None:
        return existing, False

    try:
        with transaction.atomic():
            delivery = GitWebhookDelivery.objects.create(
                provider=provider,
                git_app_id=git_app_id,
                delivery_id=delivery_id,
                event_type=event_type,
                action=action,
                payload_digest=payload_digest,
                repository_url=repository_url or "",
                ref_name=ref_name or "",
                head_sha=head_sha or None,
                pr_number=pr_number,
                stage=GitWebhookDelivery.Stage.VERIFIED,
            )
    except IntegrityError:
        # concurrent delivery with the same identity won the insert race
        delivery = GitWebhookDelivery.objects.get(
            provider=provider,
            git_app_id=git_app_id,
            delivery_id=delivery_id,
        )
        return delivery, False
    return delivery, True


def schedule_duplicate_replay(existing: GitWebhookDelivery) -> None:
    """
    Continue an original delivery whose effects were committed but whose
    workflow starts were never all confirmed (crash, restart, ambiguous
    response). Redelivery returns the original accepted result while the
    missing starts are replayed after commit.
    """
    if existing.stage == GitWebhookDelivery.Stage.EFFECTS_COMMITTED:
        delivery_id = existing.id
        starts = list(existing.starts)
        signals = list(existing.signals)
        transaction.on_commit(lambda: _run_after_commit(delivery_id, starts, signals))


# --------------------------------------------------------------------------- #
# Ref generation arbitration
# --------------------------------------------------------------------------- #


@dataclass
class ArbitrationResult:
    proceed: bool
    generation: Optional[GitRefGeneration]
    reason: str = ""


def _locked_get_or_create_generation(
    *,
    git_app_id: str,
    repository_url: str,
    kind: str,
    ref_name: str,
    pr_number: int = 0,
) -> GitRefGeneration:
    try:
        return GitRefGeneration.objects.select_for_update().get(
            git_app_id=git_app_id,
            repository_url=repository_url,
            kind=kind,
            ref_name=ref_name,
            pr_number=pr_number,
        )
    except GitRefGeneration.DoesNotExist:
        try:
            with transaction.atomic():
                return GitRefGeneration.objects.create(
                    git_app_id=git_app_id,
                    repository_url=repository_url,
                    kind=kind,
                    ref_name=ref_name,
                    pr_number=pr_number,
                )
        except IntegrityError:
            return GitRefGeneration.objects.select_for_update().get(
                git_app_id=git_app_id,
                repository_url=repository_url,
                kind=kind,
                ref_name=ref_name,
                pr_number=pr_number,
            )


def repository_is_connected(git_app, repository_url: str) -> bool:
    """
    A late event for a repository detached from the installation (e.g. repo
    removed from the GitHub app) must not resurrect state or deploy.
    """
    if not repository_url:
        return True
    if git_app.github_id is not None:
        return git_app.github.repositories.filter(url=repository_url).exists()
    if git_app.gitlab_id is not None:
        return git_app.gitlab.repositories.filter(url=repository_url).exists()
    return False


def arbitrate_branch_push(
    *,
    delivery: GitWebhookDelivery,
    git_app_id: str,
    repository_url: str,
    branch_name: str,
    before: Optional[str],
    after: str,
    forced: bool = False,
    deleted: bool = False,
) -> ArbitrationResult:
    generation = _locked_get_or_create_generation(
        git_app_id=git_app_id,
        repository_url=repository_url,
        kind=GitRefGeneration.RefKind.BRANCH,
        ref_name=branch_name,
    )

    if deleted:
        generation.state = GitRefGeneration.RefState.DELETED
        generation.head_sha = None
        generation.last_delivery = delivery
        generation.save()
        return ArbitrationResult(True, generation, "branch deleted")

    if generation.state == GitRefGeneration.RefState.DELETED:
        if before == ZERO_COMMIT_SHA:
            # branch re-created after deletion
            generation.state = GitRefGeneration.RefState.ACTIVE
            generation.head_sha = after
            generation.last_delivery = delivery
            generation.save()
            return ArbitrationResult(True, generation, "branch recreated")
        return ArbitrationResult(False, generation, "branch is deleted")

    if generation.head_sha is None:
        generation.head_sha = after
        generation.last_delivery = delivery
        generation.save()
        return ArbitrationResult(True, generation, "first known generation")

    if generation.head_sha == after:
        # the generation introduced by this event is already published
        return ArbitrationResult(False, generation, "generation already published")

    if forced or before == ZERO_COMMIT_SHA:
        generation.head_sha = after
        generation.last_delivery = delivery
        generation.save()
        return ArbitrationResult(True, generation, "forced push")

    if before == generation.head_sha:
        generation.head_sha = after
        generation.last_delivery = delivery
        generation.save()
        return ArbitrationResult(True, generation, "next generation")

    # this event does not extend the known head: it is older / out-of-order
    return ArbitrationResult(False, generation, "stale push for older generation")


def arbitrate_pull_request(
    *,
    delivery: GitWebhookDelivery,
    git_app_id: str,
    repository_url: str,
    branch_name: str,
    pr_number: int,
    head_sha: Optional[str],
    mode: str,
    oldrev: Optional[str] = None,
) -> ArbitrationResult:
    """
    ``mode`` is one of ``open``, ``sync`` (new commits), ``close`` and
    ``meta`` (title/base edits that never deploy).
    """
    generation = _locked_get_or_create_generation(
        git_app_id=git_app_id,
        repository_url=repository_url,
        kind=GitRefGeneration.RefKind.PULL_REQUEST,
        ref_name=branch_name,
        pr_number=pr_number,
    )

    if mode == "close":
        generation.state = GitRefGeneration.RefState.CLOSED
        generation.last_delivery = delivery
        generation.save()
        return ArbitrationResult(True, generation, "pull request closed")

    if mode == "meta":
        # metadata edits never create deployments/previews and never advance
        # the generation
        return ArbitrationResult(True, None, "metadata only")

    # open / sync: a closed pull request can never be reopened or redeployed
    # by late events
    if generation.state == GitRefGeneration.RefState.CLOSED:
        return ArbitrationResult(False, generation, "pull request already closed")

    if generation.head_sha is None:
        generation.head_sha = head_sha
        generation.last_delivery = delivery
        generation.save()
        return ArbitrationResult(True, generation, "first known generation")

    if generation.head_sha == head_sha:
        # The same commit is signalled again (e.g. a `synchronize` delivery
        # that does not carry a new head, or a redelivered open). The delivery
        # is a different one but it does not roll the ref back, so let the
        # existing per-effect guards (existing preview / deployment dedup)
        # decide; the generation itself must not move backwards.
        return ArbitrationResult(True, generation, "same generation republished")

    if mode == "sync" and oldrev is not None and oldrev != generation.head_sha:
        # the update does not build on top of the current head: it's older
        return ArbitrationResult(False, generation, "stale pull request update")

    generation.head_sha = head_sha
    generation.last_delivery = delivery
    generation.save()
    return ArbitrationResult(True, generation, "next generation")


# --------------------------------------------------------------------------- #
# Stage bookkeeping
# --------------------------------------------------------------------------- #


def _serialize_start(start: StartWorkflowArg) -> dict[str, Any]:
    workflow_name = _WORKFLOW_NAME_BY_RUN[start.workflow]
    start_delay = (
        start.start_delay.total_seconds() if start.start_delay is not None else None
    )
    return {
        "workflow": workflow_name,
        "workflow_id": start.workflow_id,
        "start_delay_seconds": start_delay,
        "status": "pending",
        "payload": asdict(start.payload),
    }


def _serialize_signal(signal: SignalWorkflowArg) -> dict[str, Any]:
    return {
        "workflow": _WORKFLOW_NAME_BY_RUN[signal.workflow],
        "workflow_id": signal.workflow_id,
        "deployment_hash": signal.input.deployment_hash,
    }


def persist_effects_and_schedule(
    delivery: GitWebhookDelivery,
    starts: list[StartWorkflowArg],
    signals: Optional[list[SignalWorkflowArg]] = None,
) -> None:
    """
    Pin the Deployment/Preview effects of the current transaction to the
    workflow starts they require, then replay the start stage after commit.
    """
    serialized_starts = [_serialize_start(start) for start in starts]
    serialized_signals = [_serialize_signal(signal) for signal in (signals or [])]
    delivery.starts = serialized_starts
    delivery.signals = serialized_signals
    delivery.stage = GitWebhookDelivery.Stage.EFFECTS_COMMITTED
    delivery.stale = False
    delivery.save(update_fields=["starts", "signals", "stage", "stale", "updated_at"])
    delivery_id = delivery.id
    transaction.on_commit(
        lambda: _run_after_commit(delivery_id, serialized_starts, serialized_signals)
    )


def mark_ignored(delivery: GitWebhookDelivery) -> None:
    delivery.stage = GitWebhookDelivery.Stage.IGNORED
    delivery.stale = True
    delivery.save(update_fields=["stage", "stale", "updated_at"])


# --------------------------------------------------------------------------- #
# Start stage: confirmation, ambiguity resolution, recovery
# --------------------------------------------------------------------------- #


def _confirm_start(item: dict[str, Any]) -> bool:
    workflow_run = _WORKFLOW_RUN_BY_NAME[item["workflow"]]
    payload = _PAYLOAD_FROM_DICT[item["workflow"]](item["payload"])
    start_delay = (
        timedelta(seconds=item["start_delay_seconds"])
        if item.get("start_delay_seconds") is not None
        else None
    )
    try:
        TemporalClient.start_workflow(
            workflow=workflow_run,
            arg=payload,
            id=item["workflow_id"],
            start_delay=start_delay,
        )
        return True
    except Exception:
        # the start response is uncertain (timeout / connection error):
        # query whether the original workflow actually started instead of
        # creating a second deployment
        try:
            return TemporalClient.workflow_exists(workflow_run, item["workflow_id"])
        except Exception:
            return False


def _send_signals(signals: list[dict[str, Any]]) -> None:
    # cancellation signals are best-effort (workflow may already be gone)
    for signal_item in signals:
        try:
            TemporalClient.workflow_signal(
                workflow=DeployGitServiceWorkflow.run,
                workflow_id=signal_item["workflow_id"],
                signal=DeployGitServiceWorkflow.cancel_deployment,  # type: ignore
                input=CancelDeploymentSignalInput(
                    deployment_hash=signal_item["deployment_hash"]
                ),
            )
        except Exception:
            pass


def run_pending_starts(
    starts: list[dict[str, Any]], signals: list[dict[str, Any]]
) -> bool:
    """
    Execute pending cancellation signals and workflow starts from serialized
    descriptors. Returns ``True`` when every start is confirmed. Safe to call
    multiple times (redelivery, restart): workflow ids are deterministic and
    Temporal rejects duplicate starts.
    """
    _send_signals(signals)
    all_confirmed = True
    for item in starts:
        if item.get("status") == "started":
            continue
        if _confirm_start(item):
            item["status"] = "started"
        else:
            all_confirmed = False
    return all_confirmed


def _persist_dispatch_result(
    delivery_id: str, starts: list[dict[str, Any]], all_confirmed: bool
) -> None:
    """
    Best-effort stage confirmation. Runs on the request connection after a
    real commit (and directly on restart recovery). When invoked from a
    non-request worker thread (e.g. tests) the row may not be visible: the
    next redelivery or restart simply retries.
    """
    try:
        delivery = GitWebhookDelivery.objects.filter(id=delivery_id).first()
        if delivery is None or delivery.stage in (
            GitWebhookDelivery.Stage.STARTED,
            GitWebhookDelivery.Stage.IGNORED,
        ):
            return
        delivery.starts = starts
        if all_confirmed:
            delivery.stage = GitWebhookDelivery.Stage.STARTED
            delivery.stale = False
            delivery.save(update_fields=["starts", "stage", "stale", "updated_at"])
        elif any(item.get("status") == "started" for item in starts):
            delivery.save(update_fields=["starts", "updated_at"])
    except Exception:
        pass
    finally:
        # this thread owns a throwaway connection when on_commit hooks are
        # executed outside the request thread (test executor / recovery).
        if threading.current_thread() is not threading.main_thread():
            try:
                connection.close()
            except Exception:
                pass


def _run_after_commit(
    delivery_id: str,
    starts: list[dict[str, Any]],
    signals: list[dict[str, Any]],
) -> bool:
    # start from the descriptors captured inside the transaction so this does
    # not depend on cross-transaction / cross-thread visibility of the row.
    all_confirmed = run_pending_starts(starts, signals)
    _persist_dispatch_result(delivery_id, starts, all_confirmed)
    return all_confirmed


def dispatch_delivery(delivery_id: str) -> bool:
    """
    Replay the start stage of a delivery loaded from the ledger.
    Used by restart recovery.
    """
    delivery = GitWebhookDelivery.objects.filter(id=delivery_id).first()
    if delivery is None or delivery.stage in (
        GitWebhookDelivery.Stage.STARTED,
        GitWebhookDelivery.Stage.IGNORED,
    ):
        return True
    starts = list(delivery.starts)
    all_confirmed = run_pending_starts(starts, list(delivery.signals))
    _persist_dispatch_result(delivery_id, starts, all_confirmed)
    return all_confirmed


def recover_pending_deliveries(limit: int = 100) -> int:
    """
    Restart-recovery: replay the start stage of deliveries whose transaction
    committed but whose workflow starts were never all confirmed.
    """
    pending_ids = list(
        GitWebhookDelivery.objects.filter(
            stage=GitWebhookDelivery.Stage.EFFECTS_COMMITTED
        )
        .order_by("created_at")
        .values_list("id", flat=True)[:limit]
    )
    recovered = 0
    try:
        for delivery_id in pending_ids:
            if dispatch_delivery(delivery_id):
                recovered += 1
    finally:
        if threading.current_thread() is not threading.main_thread():
            try:
                connection.close()
            except Exception:
                pass
    return recovered


# --------------------------------------------------------------------------- #
# Async stage execution / recovery (event-loop friendly, same-connection ORM)
# --------------------------------------------------------------------------- #


async def _aconfirm_start(item: dict[str, Any]) -> bool:
    workflow_run = _WORKFLOW_RUN_BY_NAME[item["workflow"]]
    payload = _PAYLOAD_FROM_DICT[item["workflow"]](item["payload"])
    start_delay = (
        timedelta(seconds=item["start_delay_seconds"])
        if item.get("start_delay_seconds") is not None
        else None
    )
    try:
        await TemporalClient.astart_workflow(
            workflow=workflow_run,
            arg=payload,
            id=item["workflow_id"],
            start_delay=start_delay,
        )
        return True
    except Exception:
        # ambiguous start response: query the existing workflow instead of
        # creating a second deployment
        try:
            return await TemporalClient.aworkflow_exists(
                workflow_run, item["workflow_id"]
            )
        except Exception:
            return False


async def arun_pending_starts(
    starts: list[dict[str, Any]], signals: list[dict[str, Any]]
) -> bool:
    for signal_item in signals:
        try:
            await TemporalClient.aworkflow_signal(
                workflow=DeployGitServiceWorkflow.run,
                workflow_id=signal_item["workflow_id"],
                signal=DeployGitServiceWorkflow.cancel_deployment,  # type: ignore
                arg=CancelDeploymentSignalInput(
                    deployment_hash=signal_item["deployment_hash"]
                ),
            )
        except Exception:
            pass

    all_confirmed = True
    for item in starts:
        if item.get("status") == "started":
            continue
        if await _aconfirm_start(item):
            item["status"] = "started"
        else:
            all_confirmed = False
    return all_confirmed


async def adispatch_delivery(delivery_id: str) -> bool:
    """Async counterpart of :func:`dispatch_delivery`."""
    delivery = await GitWebhookDelivery.objects.filter(id=delivery_id).afirst()
    if delivery is None or delivery.stage in (
        GitWebhookDelivery.Stage.STARTED,
        GitWebhookDelivery.Stage.IGNORED,
    ):
        return True
    starts = list(delivery.starts)
    all_confirmed = await arun_pending_starts(starts, list(delivery.signals))
    delivery.starts = starts
    if all_confirmed:
        delivery.stage = GitWebhookDelivery.Stage.STARTED
        delivery.stale = False
        await delivery.asave(update_fields=["starts", "stage", "stale", "updated_at"])
    elif any(item.get("status") == "started" for item in starts):
        await delivery.asave(update_fields=["starts", "updated_at"])
    return all_confirmed


async def arecover_pending_deliveries(limit: int = 100) -> int:
    """Async counterpart of :func:`recover_pending_deliveries`."""
    pending_ids = [
        delivery_id
        async for delivery_id in GitWebhookDelivery.objects.filter(
            stage=GitWebhookDelivery.Stage.EFFECTS_COMMITTED
        )
        .order_by("created_at")
        .values_list("id", flat=True)[:limit]
    ]
    recovered = 0
    for delivery_id in pending_ids:
        if await adispatch_delivery(delivery_id):
            recovered += 1
    return recovered
