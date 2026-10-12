"""
Activities backing the workspace retirement generation.

These activities only manipulate the retirement outbox and trigger
idempotent external cleanup through the existing swarm / compose activities.
They never re-archive or re-delete the database objects: those operations
happen exactly once, inside the API transaction that opens the generation.
"""

import logging
from typing import List

from asgiref.sync import sync_to_async
from django.utils import timezone
from temporalio.exceptions import ApplicationError

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from temporal.shared import (
        ArchivedProjectDetails,
        ComposeStackArchiveDetails,
        EnvironmentDetails,
        RetirementReconcileResult,
        RetirementResourceConfirmation,
        RetirementTaskRef,
        WorkspaceRetirementPayload,
    )
    from zane_api.models import (
        ArchivedProject,
        WorkspaceRetirement,
        WorkspaceCleanupTask,
        WorkspaceCleanupResource,
    )
    from zane_api.retirement import (
        RetirementIncomplete,
        finalize_workspace_retirement,
    )

logger = logging.getLogger(__name__)


async def _aget_task(ref: RetirementTaskRef) -> WorkspaceCleanupTask | None:
    return (
        await WorkspaceCleanupTask.objects.filter(
            retirement__generation=ref.generation,
            unit_type=ref.unit_type,
            unit_key=ref.unit_key,
        )
        .select_related("retirement")
        .afirst()
    )


async def _build_project_details(
    archived_project_id: int,
) -> ArchivedProjectDetails:
    archived = await ArchivedProject.objects.aget(pk=archived_project_id)

    environments: List[EnvironmentDetails] = []
    async for env in archived.environments.all():
        environments.append(
            EnvironmentDetails(
                id=env.original_id,
                name=env.name,
                project_id=archived.original_id,
            )
        )

    compose_stacks: List[ComposeStackArchiveDetails] = []
    live_project = archived.active_version
    if live_project is not None:
        async for stack in (
            live_project.compose_stacks.filter(user_content__isnull=False)
            .prefetch_related("env_overrides")
            .all()
        ):
            compose_stacks.append(ComposeStackArchiveDetails(stack=stack.snapshot))

    return ArchivedProjectDetails(
        id=archived.pk,
        original_id=archived.original_id,
        environments=environments,
        compose_stacks=compose_stacks,
    )


class WorkspaceRetirementActivities:
    @activity.defn
    async def get_retirement_projects(
        self,
        payload: WorkspaceRetirementPayload,
    ) -> List[ArchivedProjectDetails]:
        """
        Discover the archived project units of the generation, rebuilt from the
        fixed archive rows (never from re-archived live objects).
        """
        tasks = WorkspaceCleanupTask.objects.filter(
            retirement__generation=payload.generation,
            retirement__status=WorkspaceRetirement.RetirementStatus.RETIRING,
            unit_type=WorkspaceCleanupTask.UnitType.PROJECT,
        ).order_by("created_at")

        details: List[ArchivedProjectDetails] = []
        async for task in tasks:
            archived_project_id = task.payload.get("archived_project_id")
            if archived_project_id is None:
                continue
            details.append(await _build_project_details(archived_project_id))
        return details

    @activity.defn
    async def get_unconfirmed_unit_keys(
        self,
        query: RetirementTaskRef,
    ) -> List[str]:
        """
        Unit keys of this generation/type whose workflow result was not yet
        confirmed. Restart / redelivery resumes exactly those units.
        """
        qs = (
            WorkspaceCleanupTask.objects.filter(
                retirement__generation=query.generation,
                unit_type=query.unit_type,
            )
            .exclude(
                delivery_status=WorkspaceCleanupTask.DeliveryStatus.CONFIRMED
            )
            .values_list("unit_key", flat=True)
        )
        return [key async for key in qs]

    @activity.defn
    async def mark_cleanup_task_started(self, ref: RetirementTaskRef) -> None:
        task = await _aget_task(ref)
        if task is None:
            raise ApplicationError(
                f"Cleanup task {ref.unit_type}/{ref.unit_key} not found for "
                f"generation {ref.generation}",
                non_retryable=True,
            )
        if task.delivery_status == WorkspaceCleanupTask.DeliveryStatus.CONFIRMED:
            return
        task.delivery_status = WorkspaceCleanupTask.DeliveryStatus.STARTED
        task.started_at = task.started_at or timezone.now()
        task.attempts += 1
        await task.asave(
            update_fields=["delivery_status", "started_at", "attempts", "updated_at"]
        )

    @activity.defn
    async def mark_cleanup_task_failed(
        self, ref: RetirementTaskRef, error: str | None = None
    ) -> None:
        task = await _aget_task(ref)
        if task is None:
            return
        if task.delivery_status == WorkspaceCleanupTask.DeliveryStatus.CONFIRMED:
            return
        task.delivery_status = WorkspaceCleanupTask.DeliveryStatus.FAILED
        task.last_error = (error or "cleanup failed")[:10_000]
        await task.asave(
            update_fields=["delivery_status", "last_error", "updated_at"]
        )

    @activity.defn
    async def confirm_cleanup_resources(
        self,
        confirmation: RetirementResourceConfirmation,
    ) -> None:
        """Record per-resource external cleanup results (containers, volumes,
        URLs, configs, schedules, builders, networks, stacks)."""
        qs = WorkspaceCleanupResource.objects.filter(
            task__retirement__generation=confirmation.generation,
            task__unit_type=confirmation.unit_type,
            task__unit_key=confirmation.unit_key,
        )
        if confirmation.resource_types:
            qs = qs.filter(resource_type__in=confirmation.resource_types)
        await qs.aupdate(
            status=WorkspaceCleanupResource.ResourceStatus.CONFIRMED,
            confirmed_at=timezone.now(),
        )

    @activity.defn
    async def mark_cleanup_task_confirmed(self, ref: RetirementTaskRef) -> None:
        task = await _aget_task(ref)
        if task is None:
            raise ApplicationError(
                f"Cleanup task {ref.unit_type}/{ref.unit_key} not found for "
                f"generation {ref.generation}",
                non_retryable=True,
            )
        # every resource of a confirmed task is confirmed by definition
        await WorkspaceCleanupResource.objects.filter(task=task).aupdate(
            status=WorkspaceCleanupResource.ResourceStatus.CONFIRMED,
            confirmed_at=timezone.now(),
        )
        task.delivery_status = WorkspaceCleanupTask.DeliveryStatus.CONFIRMED
        task.confirmed_at = timezone.now()
        task.last_error = None
        await task.asave(
            update_fields=[
                "delivery_status",
                "confirmed_at",
                "last_error",
                "updated_at",
            ]
        )

    @activity.defn
    async def finalize_workspace_retirement(
        self,
        payload: WorkspaceRetirementPayload,
    ) -> dict:
        """
        Hide the workspace and converge memberships / tokens / sessions once
        every unit of the generation is confirmed. Unconfirmed units make this
        raise a retryable error, so only a fully confirmed generation finalizes.
        """
        try:
            result = await sync_to_async(finalize_workspace_retirement)(
                payload.generation
            )
        except RetirementIncomplete as exc:
            raise ApplicationError(str(exc), type="RetirementIncomplete")
        return result or {}

    @activity.defn
    async def reconcile_workspace_retirements(self) -> RetirementReconcileResult:
        return await areconcile_workspace_retirements()


async def areconcile_workspace_retirements() -> RetirementReconcileResult:
    """
    Restart recovery: for every in-progress generation, re-deliver the
    top-level workflow when its start was lost/failed (stable workflow ID makes
    a duplicate start a no-op), and finalize generations whose units are all
    confirmed. Database objects are never touched twice.

    Callable from the periodic schedule activity or the
    `reconcile_workspace_retirements` management command run at API boot.
    """
    result = RetirementReconcileResult()
    async for retirement in WorkspaceRetirement.objects.filter(
        status=WorkspaceRetirement.RetirementStatus.RETIRING
    ).prefetch_related("tasks"):
        unconfirmed = await retirement.tasks.exclude(
            delivery_status=WorkspaceCleanupTask.DeliveryStatus.CONFIRMED
        ).acount()

        if unconfirmed == 0 and await retirement.tasks.acount() > 0:
            try:
                await sync_to_async(finalize_workspace_retirement)(
                    str(retirement.generation)
                )
            except RetirementIncomplete:
                result.pending.append(str(retirement.generation))
            else:
                result.finalized.append(str(retirement.generation))
            continue

        if retirement.delivery_status in (
            WorkspaceRetirement.DeliveryStatus.PENDING,
            WorkspaceRetirement.DeliveryStatus.FAILED,
        ):
            # local import: temporal.workflows imports activities, so an
            # import at module load would be circular
            from temporal.client import TemporalClient
            from temporal.workflows import WorkspaceRetirementWorkflow

            try:
                await TemporalClient.astart_workflow(
                    WorkspaceRetirementWorkflow.run,
                    WorkspaceRetirementPayload(
                        generation=str(retirement.generation)
                    ),
                    id=retirement.workflow_id,
                )
            except Exception as exc:  # temporal unavailable; try next run
                logger.warning(
                    "Retirement reconcile could not redeliver "
                    f"{retirement.workflow_id}: {exc!r}"
                )
                result.pending.append(str(retirement.generation))
                continue
            retirement.delivery_status = WorkspaceRetirement.DeliveryStatus.STARTED
            retirement.started_at = retirement.started_at or timezone.now()
            await retirement.asave(
                update_fields=[
                    "delivery_status",
                    "started_at",
                    "updated_at",
                ]
            )
            result.redelivered.append(str(retirement.generation))
        else:
            # top-level workflow in flight; wait for the next reconcile tick
            result.pending.append(str(retirement.generation))

    return result
