"""
Workspace retirement service.

A workspace deletion is turned into a resumable retirement generation:

1. ``start_workspace_retirement`` runs inside the request transaction. It fixes
   a frozen snapshot, archives services exactly once (the historical archive
   behaviour), and writes a transactional outbox of cleanup tasks (one per
   service / compose stack / environment / project network) with stable
   workflow IDs and per-resource manifests. The workspace stays queryable and
   every project / deployment / membership write is rejected while it retires.
2. ``dispatch_workspace_retirement`` runs *after* the transaction commits and
   starts the single top-level retirement workflow. A failed or lost start
   leaves the row ``PENDING`` so the reconciler / a repeated delete can re-issue
   the exact same workflow ID (``WorkflowAlreadyStarted`` makes that safe).
3. The cleanup workflows only flip outbox rows / resources to ``CONFIRMED``.
   On partial failure, restart or lost response, exactly the unconfirmed units
   of the *same* generation are continued; database objects are never archived
   or deleted a second time.
4. ``finalize_workspace_retirement`` hides the workspace only after every unit
   is confirmed and deterministically converges memberships, owner relation,
   API tokens, invitations and (via stale-session fallback in ``HasWorkspace``)
   every user's current / default workspace selection.
"""

from __future__ import annotations

import logging
import uuid
from typing import Optional

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from temporal.shared import (
    ArchivedProjectDetails,
    ComposeStackArchiveDetails,
    EnvironmentDetails,
)

from .models import (
    Workspace,
    WorkspaceMembership,
    ArchivedProject,
    Service,
    PortConfiguration,
    URL,
    Volume,
    Config,
    ArchivedDockerService,
    ArchivedGitService,
    WorkspaceRetirement,
    WorkspaceCleanupTask,
    WorkspaceCleanupResource,
)

logger = logging.getLogger(__name__)


class RetirementIncomplete(Exception):
    """Raised when finalization is requested with unconfirmed cleanup units."""


def get_active_retirement(
    workspace_id: str | None = None,
    *,
    workspace: Optional[Workspace] = None,
) -> Optional[WorkspaceRetirement]:
    """Return the in-progress retirement for a workspace, if any."""
    pk = workspace.id if workspace is not None else workspace_id
    if pk is None:
        return None
    return (
        WorkspaceRetirement.objects.filter(
            workspace_id=pk,
            status=WorkspaceRetirement.RetirementStatus.RETIRING,
        )
        .prefetch_related("tasks__resources")
        .first()
    )


def is_workspace_retiring(workspace_id: str | None) -> bool:
    if workspace_id is None:
        return False
    return WorkspaceRetirement.objects.filter(
        workspace_id=workspace_id,
        status=WorkspaceRetirement.RetirementStatus.RETIRING,
    ).exists()


def retirement_workflow_id(generation) -> str:
    return f"workspace-retirement-{generation.hex if hasattr(generation, 'hex') else generation}"


def _unit_workflow_id(retirement: WorkspaceRetirement, unit_type: str, key: str) -> str:
    generation = retirement.generation.hex
    return f"workspace-retirement-{generation}-{unit_type.lower()}-{key}"


def _archive_project(
    project,
) -> tuple[ArchivedProject, ArchivedProjectDetails]:
    """
    Archive one project for retirement.

    This preserves the exact archival semantics of the historical workspace
    delete (and single project archive): services with deployments are snapshotted
    into ``Archived*Service`` rows, related rows are removed, and the services
    themselves are deleted. Projects / environments rows are kept frozen until
    the workspace is hidden at finalization.
    """
    archived_version = ArchivedProject.get_or_create_from_project(project)

    docker_service_list = (
        Service.objects.filter(Q(project=project))
        .select_related("project", "healthcheck")
        .prefetch_related(
            "volumes", "ports", "urls", "env_variables", "deployments"
        )
    )
    id_list = []
    for service in docker_service_list:
        if service.deployments.count() > 0:
            if service.type == Service.ServiceType.DOCKER_REGISTRY:
                ArchivedDockerService.create_from_service(service, archived_version)
            else:
                ArchivedGitService.create_from_service(service, archived_version)
            id_list.append(service.id)

    PortConfiguration.objects.filter(Q(service__id__in=id_list)).delete()
    URL.objects.filter(Q(service__id__in=id_list)).delete()
    Volume.objects.filter(Q(service__id__in=id_list)).delete()
    Config.objects.filter(Q(service__id__in=id_list)).delete()
    for service in docker_service_list:
        if service.healthcheck is not None:
            service.healthcheck.delete()
    # Delete Preview metadata before the services because they hold protected
    # references to the services
    for env in project.environments.filter().select_related("preview_metadata"):
        if env.preview_metadata is not None:
            env.preview_metadata.delete()
    docker_service_list.delete()

    details = ArchivedProjectDetails(
        id=archived_version.pk,
        original_id=archived_version.original_id,
        environments=[
            EnvironmentDetails(
                id=env.original_id,
                name=env.name,
                project_id=archived_version.original_id,
            )
            for env in archived_version.environments.all()
        ],
        compose_stacks=[
            ComposeStackArchiveDetails(stack=stack.snapshot)
            for stack in project.compose_stacks.filter(user_content__isnull=False)
            .prefetch_related("env_overrides")
            .all()
        ],
    )
    return archived_version, details


def _create_cleanup_outbox(
    retirement: WorkspaceRetirement,
    archived_projects: list[tuple[ArchivedProject, ArchivedProjectDetails, object]],
) -> None:
    """
    Write the stable cleanup outbox from the fixed archive snapshot.

    One task per cleanup unit, plus one resource row per external resource
    (containers, volumes, configs, URLs, healthcheck schedules, buildkit
    builders, preview environments, compose stacks, networks).
    """
    Resource = WorkspaceCleanupResource.ResourceType
    Unit = WorkspaceCleanupTask.UnitType

    for archived_version, details, project in archived_projects:
        original_project_id = archived_version.original_id

        # --- Services (containers / volumes / configs / URLs / schedules) --
        for archived_service in list(archived_version.docker_services.all()) + list(
            archived_version.git_services.all()
        ):
            unit_key = archived_service.original_id
            task = WorkspaceCleanupTask.objects.create(
                retirement=retirement,
                unit_type=Unit.SERVICE,
                unit_key=unit_key,
                workflow_id=_unit_workflow_id(retirement, Unit.SERVICE, unit_key),
                payload={
                    "original_id": unit_key,
                    "project_id": original_project_id,
                    "archived_project_id": archived_version.pk,
                },
            )
            resource_rows: list[WorkspaceCleanupResource] = []
            for archived_url in archived_service.urls.all():
                resource_rows.append(
                    WorkspaceCleanupResource(
                        task=task,
                        resource_type=Resource.URL,
                        resource_key=(
                            f"{archived_url.domain or ''}{archived_url.base_path or '/'}"
                        ),
                    )
                )
            for archived_volume in archived_service.volumes.all():
                resource_rows.append(
                    WorkspaceCleanupResource(
                        task=task,
                        resource_type=Resource.VOLUME,
                        resource_key=archived_volume.name,
                    )
                )
            for archived_config in archived_service.configs.all():
                resource_rows.append(
                    WorkspaceCleanupResource(
                        task=task,
                        resource_type=Resource.CONFIG,
                        resource_key=archived_config.name,
                    )
                )
            for deployment in archived_service.deployments:
                deployment_hash = deployment.get("hash")
                if not deployment_hash:
                    continue
                resource_rows.append(
                    WorkspaceCleanupResource(
                        task=task,
                        resource_type=Resource.CONTAINER,
                        resource_key=deployment_hash,
                    )
                )
                resource_rows.append(
                    WorkspaceCleanupResource(
                        task=task,
                        resource_type=Resource.HEALTHCHECK,
                        resource_key=f"monitor-{deployment_hash}",
                    )
                )
            WorkspaceCleanupResource.objects.bulk_create(resource_rows)

        # --- Environments (buildkit builders, preview environments) --------
        for env in archived_version.environments.all():
            live_env = project.environments.filter(id=env.original_id).first()
            task = WorkspaceCleanupTask.objects.create(
                retirement=retirement,
                unit_type=Unit.ENVIRONMENT,
                unit_key=env.original_id,
                workflow_id=_unit_workflow_id(
                    retirement, Unit.ENVIRONMENT, env.original_id
                ),
                payload={"id": env.original_id, "name": env.name},
            )
            WorkspaceCleanupResource.objects.create(
                task=task,
                resource_type=Resource.BUILDKIT_BUILDER,
                resource_key=env.original_id,
            )
            if live_env is not None and live_env.is_preview:
                WorkspaceCleanupResource.objects.create(
                    task=task,
                    resource_type=Resource.PREVIEW_ENVIRONMENT,
                    resource_key=env.original_id,
                )

        # --- Compose stacks ------------------------------------------------
        for stack in (
            project.compose_stacks.filter(user_content__isnull=False)
            .prefetch_related("env_overrides")
            .all()
        ):
            task = WorkspaceCleanupTask.objects.create(
                retirement=retirement,
                unit_type=Unit.COMPOSE_STACK,
                unit_key=stack.id,
                workflow_id=_unit_workflow_id(
                    retirement, Unit.COMPOSE_STACK, stack.id
                ),
                payload={"stack_id": stack.id},
            )
            WorkspaceCleanupResource.objects.create(
                task=task,
                resource_type=Resource.STACK,
                resource_key=stack.id,
            )

        # --- Project network, always cleaned last for the project ----------
        network_task = WorkspaceCleanupTask.objects.create(
            retirement=retirement,
            unit_type=Unit.PROJECT,
            unit_key=original_project_id,
            workflow_id=_unit_workflow_id(
                retirement, Unit.PROJECT, original_project_id
            ),
            payload={"archived_project_id": archived_version.pk},
        )
        WorkspaceCleanupResource.objects.create(
            task=network_task,
            resource_type=Resource.NETWORK,
            resource_key=original_project_id,
        )


def start_workspace_retirement(workspace: Workspace) -> WorkspaceRetirement:
    """
    Fix a retirement generation for the workspace.

    Must be called inside an atomic transaction. Idempotent: an in-progress
    retirement is returned untouched, so a repeated delete never re-archives or
    re-deletes database objects.
    """
    existing = get_active_retirement(workspace=workspace)
    if existing is not None:
        return existing

    generation = uuid.uuid4()
    retirement = WorkspaceRetirement.objects.create(
        workspace=workspace,
        workspace_original_id=workspace.id,
        generation=generation,
        workflow_id=retirement_workflow_id(generation),
    )

    archived_projects: list[
        tuple[ArchivedProject, ArchivedProjectDetails, object]
    ] = []
    snapshot_projects = []
    for project in workspace.projects.all().prefetch_related("environments"):
        archived_version, details = _archive_project(project)
        archived_projects.append((archived_version, details, project))
        snapshot_projects.append(
            {
                "id": project.id,
                "slug": project.slug,
                "archived_project_id": archived_version.pk,
                "environments": [
                    {"id": env.id, "name": env.name, "is_preview": env.is_preview}
                    for env in project.environments.all()
                ],
                "services": [
                    {
                        "id": service.original_id,
                        "slug": service.slug,
                        "deployments": service.deployments,
                        "urls": [
                            {"domain": url.domain, "base_path": url.base_path}
                            for url in service.urls.all()
                        ],
                        "volumes": [
                            {"name": volume.name}
                            for volume in service.volumes.all()
                        ],
                        "configs": [
                            {"name": config.name} for config in service.configs.all()
                        ],
                    }
                    for service in list(archived_version.docker_services.all())
                    + list(archived_version.git_services.all())
                ],
                "compose_stacks": [
                    {"id": stack.id, "slug": stack.slug}
                    for stack in project.compose_stacks.filter(
                        user_content__isnull=False
                    )
                ],
            }
        )

    _create_cleanup_outbox(retirement, archived_projects)

    retirement.snapshot = {
        "workspace": {"id": workspace.id, "name": workspace.name},
        "projects": snapshot_projects,
    }
    retirement.save(update_fields=["snapshot", "updated_at"])
    return retirement


def dispatch_workspace_retirement(
    retirement_id: int,
    *,
    workflow_id: str,
    generation: str,
) -> None:
    """
    Deliver the top-level retirement workflow after the outbox transaction has
    committed. Never raises: a delivery failure leaves the row PENDING and the
    reconciler / a repeated delete will retry with the same stable workflow ID.
    """
    # Imported here to avoid pulling the Temporal stack during model importing.
    from temporal.client import TemporalClient
    from temporal.workflows import WorkspaceRetirementWorkflow
    from temporal.shared import WorkspaceRetirementPayload

    try:
        TemporalClient.start_workflow(
            WorkspaceRetirementWorkflow.run,
            WorkspaceRetirementPayload(generation=generation),
            id=workflow_id,
        )
    except Exception:  # network error, temporal down, lost response ...
        # leave the row PENDING: the periodic reconciliation and a repeated
        # delete will retry the same stable workflow ID
        logger.warning(
            "Failed to deliver workspace retirement workflow "
            f"{workflow_id}; leaving row PENDING for retry",
            exc_info=True,
        )

    # Delivery bookkeeping is best-effort and independent from the start call:
    # `start_workflow` is idempotent on the stable workflow ID and the periodic
    # reconciliation reconciles the state from Temporal. Never let it break the
    # on-commit callback (which can run on a different DB connection).
    try:
        retirement = WorkspaceRetirement.objects.filter(pk=retirement_id).first()
        if retirement is None:
            return
        if retirement.delivery_status != WorkspaceRetirement.DeliveryStatus.CONFIRMED:
            retirement.delivery_status = WorkspaceRetirement.DeliveryStatus.STARTED
            retirement.started_at = retirement.started_at or timezone.now()
            retirement.save(
                update_fields=["delivery_status", "started_at", "updated_at"]
            )
    except Exception as exc:  # pragma: no cover - bookkeeping must not raise
        logger.warning(
            f"Retirement delivery bookkeeping failed for {workflow_id}: {exc!r}"
        )


def schedule_dispatch_after_commit(retirement: WorkspaceRetirement) -> None:
    # Capture everything the dispatcher needs in the closure: the on-commit
    # callback can run on a different thread / database connection than the
    # view, so it must not re-read the freshly committed retirement row.
    workflow_id = retirement.workflow_id
    generation = str(retirement.generation)
    retirement_id = retirement.pk
    transaction.on_commit(
        lambda: dispatch_workspace_retirement(
            retirement_id,
            workflow_id=workflow_id,
            generation=generation,
        ),
    )


def retirement_serialized(retirement: WorkspaceRetirement) -> dict:
    """Queryable progress of a retirement generation."""
    tasks = list(retirement.tasks.all())
    per_unit = {}
    for task in tasks:
        per_unit.setdefault(task.unit_type, 0)
        per_unit[task.unit_type] += 1
    progress = retirement.progress
    return {
        "generation": str(retirement.generation),
        "status": retirement.status,
        "delivery_status": retirement.delivery_status,
        "workflow_id": retirement.workflow_id,
        "created_at": retirement.created_at.isoformat(),
        "started_at": (
            retirement.started_at.isoformat() if retirement.started_at else None
        ),
        "finalized_at": (
            retirement.finalized_at.isoformat()
            if retirement.finalized_at
            else None
        ),
        "snapshot": retirement.snapshot,
        "progress": progress,
        "units": per_unit,
        "result": retirement.result,
    }


@transaction.atomic
def finalize_workspace_retirement(generation: str) -> Optional[dict]:
    """
    Hide the workspace once every cleanup unit of the generation is confirmed.

    Idempotent and generation-scoped: a completed generation returns its stored
    result, nothing is deleted twice, and other workspaces are never touched.
    """
    retirement = (
        WorkspaceRetirement.objects.select_for_update()
        .filter(generation=generation)
        .first()
    )
    if retirement is None:
        return None

    if retirement.status == WorkspaceRetirement.RetirementStatus.COMPLETED:
        return retirement.result

    unconfirmed = retirement.tasks.exclude(
        delivery_status=WorkspaceCleanupTask.DeliveryStatus.CONFIRMED
    )
    if unconfirmed.exists():
        pending = list(
            retirement.tasks.filter(
                delivery_status__in=[
                    WorkspaceCleanupTask.DeliveryStatus.PENDING,
                    WorkspaceCleanupTask.DeliveryStatus.FAILED,
                ]
            ).values_list("unit_type", "unit_key")
        )
        raise RetirementIncomplete(
            f"Cannot finalize retirement {generation}: "
            f"{unconfirmed.count()} cleanup unit(s) unconfirmed, "
            f"deliverable now: {pending}"
        )

    workspace = retirement.workspace
    workspace_id = retirement.workspace_original_id

    # Convergence snapshot taken *before* the cascade delete.
    member_user_ids = list(
        WorkspaceMembership.objects.filter(workspace_id=workspace_id).values_list(
            "user_id", flat=True
        )
    )
    # Deterministic current/default workspace fallbacks for every member: the
    # earliest remaining membership, or None if this was their last workspace.
    session_fallbacks: dict[int, Optional[str]] = {}
    for user_id in member_user_ids:
        fallback = (
            WorkspaceMembership.objects.filter(user_id=user_id)
            .exclude(workspace_id=workspace_id)
            .select_related("workspace")
            .order_by("created_at")
            .values_list("workspace_id", flat=True)
            .first()
        )
        session_fallbacks[str(user_id)] = fallback

    counts = {
        "memberships": WorkspaceMembership.objects.filter(
            workspace_id=workspace_id
        ).count(),
        "api_tokens": (
            workspace.api_tokens.count() if workspace is not None else 0
        ),
        "invitations": (
            workspace.invitations.count() if workspace is not None else 0
        ),
        "projects": (
            workspace.projects.count() if workspace is not None else 0
        ),
    }

    result = {
        "workspace_id": workspace_id,
        "generation": generation,
        "affected_user_ids": [str(uid) for uid in member_user_ids],
        "session_fallbacks": session_fallbacks,
        "converged": counts,
    }

    # The cascade removes memberships (incl. the unique owner relation), API
    # tokens, invitations, projects, environments, services and compose stacks.
    # The retirement row is detached by SET_NULL and kept as the generation
    # record.
    if workspace is not None:
        workspace.delete()

    retirement.refresh_from_db(fields=["workspace"])
    retirement.status = WorkspaceRetirement.RetirementStatus.COMPLETED
    retirement.delivery_status = WorkspaceRetirement.DeliveryStatus.CONFIRMED
    retirement.finalized_at = timezone.now()
    retirement.result = result
    retirement.save()
    return result
