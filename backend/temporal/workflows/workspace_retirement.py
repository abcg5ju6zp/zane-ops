"""
Workspace retirement workflows.

The top-level ``WorkspaceRetirementWorkflow`` drives one retirement generation:
for every archived project it fans out one stable child workflow per cleanup
unit (service / compose stack / environment), removes the project network
last, and finalizes the generation. Every child records its delivery state and
per-resource results in the retirement outbox, so a partial failure, a lost
start response or a service restart only ever continues the unconfirmed units
of the same generation.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        DockerSwarmActivities,
        ComposeStackActivities,
        GitActivities,
    )
    from ..activities.workspace_retirement import WorkspaceRetirementActivities
    from ..shared import (
        RetirementEnvironmentUnit,
        RetirementProjectUnit,
        RetirementResourceConfirmation,
        RetirementServiceUnit,
        RetirementStackUnit,
        RetirementTaskRef,
        WorkspaceRetirementPayload,
    )

# Must match WorkspaceCleanupTask.UnitType values
UNIT_SERVICE = "SERVICE"
UNIT_COMPOSE_STACK = "COMPOSE_STACK"
UNIT_ENVIRONMENT = "ENVIRONMENT"
UNIT_PROJECT = "PROJECT"

RESOURCE_URL = "URL"
RESOURCE_RUNTIME = ["CONTAINER", "VOLUME", "CONFIG", "HEALTHCHECK"]
RESOURCE_ENV = ["BUILDKIT_BUILDER", "PREVIEW_ENVIRONMENT"]
RESOURCE_NETWORK = ["NETWORK"]
RESOURCE_STACK = ["STACK"]


def _child_workflow_id(generation: str, unit_type: str, unit_key: str) -> str:
    return f"workspace-retirement-{generation}-{unit_type.lower()}-{unit_key}"


@workflow.defn(name="retire-workspace-service")
class RetireWorkspaceServiceWorkflow:
    """Unexpose URLs then remove one archived service's containers, volumes,
    configs, images and health schedules."""

    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )

    @workflow.run
    async def run(self, unit: RetirementServiceUnit) -> str:
        ref = RetirementTaskRef(
            generation=unit.generation,
            unit_type=UNIT_SERVICE,
            unit_key=unit.details.original_id,
        )
        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_started,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )

        try:
            await workflow.execute_activity_method(
                DockerSwarmActivities.unexpose_docker_service_from_http,
                unit.details,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.confirm_cleanup_resources,
                RetirementResourceConfirmation(
                    generation=ref.generation,
                    unit_type=ref.unit_type,
                    unit_key=ref.unit_key,
                    resource_types=[RESOURCE_URL],
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity_method(
                DockerSwarmActivities.cleanup_docker_service_resources,
                unit.details,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.confirm_cleanup_resources,
                RetirementResourceConfirmation(
                    generation=ref.generation,
                    unit_type=ref.unit_type,
                    unit_key=ref.unit_key,
                    resource_types=list(RESOURCE_RUNTIME),
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
        except Exception as exc:
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.mark_cleanup_task_failed,
                ref,
                str(exc),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            raise

        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_confirmed,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )
        return unit.details.original_id


@workflow.defn(name="retire-workspace-stack")
class RetireWorkspaceStackWorkflow:
    """Remove one compose stack: routes, CLI stack, containers and resources."""

    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )

    @workflow.run
    async def run(self, unit: RetirementStackUnit) -> str:
        stack_id = unit.stack.stack.id
        ref = RetirementTaskRef(
            generation=unit.generation,
            unit_type=UNIT_COMPOSE_STACK,
            unit_key=stack_id,
        )
        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_started,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )

        try:
            await workflow.execute_activity_method(
                ComposeStackActivities.unexpose_stack_services_from_http,
                unit.stack,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )

            stack_services = await workflow.execute_activity_method(
                ComposeStackActivities.get_services_in_stack,
                unit.stack,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity_method(
                ComposeStackActivities.remove_stack_with_cli,
                unit.stack,
                start_to_close_timeout=timedelta(minutes=2, seconds=30),
                retry_policy=self.retry_policy,
            )

            await asyncio.gather(
                *[
                    workflow.execute_activity_method(
                        ComposeStackActivities.wait_for_stack_service_containers_to_be_deleted,
                        service,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=self.retry_policy,
                    )
                    for service in stack_services
                ]
            )

            await workflow.execute_activity_method(
                ComposeStackActivities.delete_stack_resources,
                unit.stack,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.confirm_cleanup_resources,
                RetirementResourceConfirmation(
                    generation=ref.generation,
                    unit_type=ref.unit_type,
                    unit_key=ref.unit_key,
                    resource_types=list(RESOURCE_STACK),
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
        except Exception as exc:
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.mark_cleanup_task_failed,
                ref,
                str(exc),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            raise

        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_confirmed,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )
        return stack_id


@workflow.defn(name="retire-workspace-environment")
class RetireWorkspaceEnvironmentWorkflow:
    """Delete one environment's buildkit builder (preview environment
    resources are carried by its archived service / stack units)."""

    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )

    @workflow.run
    async def run(self, unit: RetirementEnvironmentUnit) -> str:
        ref = RetirementTaskRef(
            generation=unit.generation,
            unit_type=UNIT_ENVIRONMENT,
            unit_key=unit.environment.id,
        )
        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_started,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )
        try:
            await workflow.execute_activity_method(
                GitActivities.delete_buildkit_builder_for_env,
                unit.environment,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.confirm_cleanup_resources,
                RetirementResourceConfirmation(
                    generation=ref.generation,
                    unit_type=ref.unit_type,
                    unit_key=ref.unit_key,
                    resource_types=list(RESOURCE_ENV),
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
        except Exception as exc:
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.mark_cleanup_task_failed,
                ref,
                str(exc),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            raise

        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_confirmed,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )
        return unit.environment.id


@workflow.defn(name="retire-workspace-project")
class RetireWorkspaceProjectWorkflow:
    """Remove the project network. Always runs after every service / stack /
    environment unit of the project is confirmed."""

    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )

    @workflow.run
    async def run(self, unit: RetirementProjectUnit) -> str:
        ref = RetirementTaskRef(
            generation=unit.generation,
            unit_type=UNIT_PROJECT,
            unit_key=unit.project.original_id,
        )
        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_started,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )
        try:
            await workflow.execute_activity_method(
                DockerSwarmActivities.remove_project_networks,
                unit.project,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.confirm_cleanup_resources,
                RetirementResourceConfirmation(
                    generation=ref.generation,
                    unit_type=ref.unit_type,
                    unit_key=ref.unit_key,
                    resource_types=list(RESOURCE_NETWORK),
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
        except Exception as exc:
            await workflow.execute_activity_method(
                WorkspaceRetirementActivities.mark_cleanup_task_failed,
                ref,
                str(exc),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            raise

        await workflow.execute_activity_method(
            WorkspaceRetirementActivities.mark_cleanup_task_confirmed,
            ref,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )
        return unit.project.original_id


@workflow.defn(name="retire-workspace")
class WorkspaceRetirementWorkflow:
    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )
        # finalization must converge once everything else is confirmed, so it
        # retries for as long as the workflow execution is alive
        self.finalize_retry_policy = RetryPolicy(
            maximum_attempts=100, maximum_interval=timedelta(seconds=30)
        )

    async def _run_project(self, generation, project, unconfirmed) -> None:
        service_details = await workflow.execute_activity_method(
            DockerSwarmActivities.get_archived_project_services,
            project,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )

        unit_workflows = []

        for service in service_details:
            if service.original_id not in unconfirmed[UNIT_SERVICE]:
                continue
            unit_workflows.append(
                workflow.execute_child_workflow(
                    RetireWorkspaceServiceWorkflow.run,
                    RetirementServiceUnit(
                        generation=generation,
                        details=service,
                    ),
                    id=_child_workflow_id(
                        generation, UNIT_SERVICE, service.original_id
                    ),
                    retry_policy=self.retry_policy,
                )
            )

        for stack in project.compose_stacks:
            if stack.stack.id not in unconfirmed[UNIT_COMPOSE_STACK]:
                continue
            unit_workflows.append(
                workflow.execute_child_workflow(
                    RetireWorkspaceStackWorkflow.run,
                    RetirementStackUnit(generation=generation, stack=stack),
                    id=_child_workflow_id(
                        generation, UNIT_COMPOSE_STACK, stack.stack.id
                    ),
                    retry_policy=self.retry_policy,
                )
            )

        for env in project.environments:
            if env.id not in unconfirmed[UNIT_ENVIRONMENT]:
                continue
            unit_workflows.append(
                workflow.execute_child_workflow(
                    RetireWorkspaceEnvironmentWorkflow.run,
                    RetirementEnvironmentUnit(
                        generation=generation, environment=env
                    ),
                    id=_child_workflow_id(generation, UNIT_ENVIRONMENT, env.id),
                    retry_policy=self.retry_policy,
                )
            )

        # services, stacks and environment builders run concurrently ...
        await asyncio.gather(*unit_workflows)

        # ... the project network is always removed last
        if project.original_id in unconfirmed[UNIT_PROJECT]:
            await workflow.execute_child_workflow(
                RetireWorkspaceProjectWorkflow.run,
                RetirementProjectUnit(generation=generation, project=project),
                id=_child_workflow_id(
                    generation, UNIT_PROJECT, project.original_id
                ),
                retry_policy=self.retry_policy,
            )

    @workflow.run
    async def run(self, payload: WorkspaceRetirementPayload) -> dict:
        print(
            "Running workflow `WorkspaceRetirementWorkflow` "
            f"with generation={payload.generation}"
        )

        projects = await workflow.execute_activity_method(
            WorkspaceRetirementActivities.get_retirement_projects,
            payload,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=self.retry_policy,
        )

        # Only schedule units that were never confirmed: on replay / restart
        # / redelivery, completed units are skipped.
        unconfirmed = {}
        for unit_type in (
            UNIT_SERVICE,
            UNIT_COMPOSE_STACK,
            UNIT_ENVIRONMENT,
            UNIT_PROJECT,
        ):
            keys = await workflow.execute_activity_method(
                WorkspaceRetirementActivities.get_unconfirmed_unit_keys,
                RetirementTaskRef(
                    generation=payload.generation,
                    unit_type=unit_type,
                    unit_key="",
                ),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            unconfirmed[unit_type] = set(keys)

        for project in projects:
            await self._run_project(payload.generation, project, unconfirmed)

        return await workflow.execute_activity_method(
            WorkspaceRetirementActivities.finalize_workspace_retirement,
            payload,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=self.finalize_retry_policy,
        )
