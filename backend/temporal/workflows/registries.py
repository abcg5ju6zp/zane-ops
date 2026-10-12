import asyncio
from datetime import timedelta
from typing import cast, Optional
from temporalio import workflow
from temporalio.common import RetryPolicy


with workflow.unsafe.imports_passed_through():
    from ..activities import (
        create_build_registry_swarm_service,
        create_docker_configs_for_registry,
        create_docker_volume_for_registry,
        pull_registry_image,
        cleanup_docker_registry_service_resources,
        remove_service_registry_url,
        upsert_registry_url_in_proxy,
        cleanup_superseded_registry_configs,
        reconcile_build_registry_swarm_service,
        wait_for_registry_service_to_be_updated,
        acquire_registry_deploy_semaphore,
        release_registry_deploy_semaphore,
        ensure_registry_health_check_schedule,
        delete_registry_health_check_schedule,
        get_registry_generation_status,
        mark_registry_generation_stage,
        publish_registry_generation,
    )
    from ..schedules import MonitorRegistryDeploymentActivites
    from container_registry.models import BuildRegistry, BuildRegistryGeneration

from ..shared import (
    SwarmRegistryServiceDetails,
    DeleteSwarmRegistryServiceDetails,
    DeleteSwarmRegistryDomainDetails,
    RegistryHealthCheckResult,
    RegistryGenerationStageDetails,
    BuildRegistryGenerationPayload,
)


def _stage_details(payload: BuildRegistryGenerationPayload, stage: str):
    return RegistryGenerationStageDetails(
        generation_id=payload.generation_id,
        stage=stage,
    )


@workflow.defn(name="destroy-build-registry")
class DestroyBuildRegistryWorkflow:
    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )

    @workflow.run
    async def run(self, payload: DeleteSwarmRegistryServiceDetails):
        await asyncio.gather(
            workflow.execute_activity(
                remove_service_registry_url,
                DeleteSwarmRegistryDomainDetails(
                    service_alias=payload.service_alias,
                    domain=payload.domain,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=self.retry_policy,
            ),
            workflow.execute_activity(
                cleanup_docker_registry_service_resources,
                payload,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=self.retry_policy,
            ),
            workflow.execute_activity(
                delete_registry_health_check_schedule,
                payload,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            ),
        )


@workflow.defn(name="deploy-build-registry")
class DeployBuildRegistryWorkflow:
    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )

    @workflow.run
    async def run(self, payload: BuildRegistryGenerationPayload):
        target = payload.target
        await workflow.execute_activity(
            acquire_registry_deploy_semaphore,
            target.id,
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=self.retry_policy,
        )
        try:
            # Re-run after a failure: if the generation is already published
            # every remaining step is a no-op, simply short-circuit.
            generation_status = await workflow.execute_activity(
                get_registry_generation_status,
                payload.generation_id,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            if generation_status != BuildRegistryGeneration.GenerationStatus.STAGING:
                return

            # Stage resources for the target version; failures never touch the
            # resources of a previously active version because the resource
            # names embed the generation version.
            volume, config_data = await asyncio.gather(
                workflow.execute_activity(
                    create_docker_volume_for_registry,
                    target,
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=self.retry_policy,
                ),
                workflow.execute_activity(
                    create_docker_configs_for_registry,
                    target,
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=self.retry_policy,
                ),
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "staged"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity(
                pull_registry_image,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            swarm_details = SwarmRegistryServiceDetails(
                registry=target,
                configs={
                    cast(str, config.id): config for config in config_data.configs
                },
                volume=volume,
                alias=target.service_alias,
                swarm_id=target.swarm_service_name,
                generation_id=payload.generation_id,
            )

            await workflow.execute_activity(
                create_build_registry_swarm_service,
                swarm_details,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity(
                wait_for_registry_service_to_be_updated,
                target,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "swarm"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity(
                upsert_registry_url_in_proxy,
                target,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "proxy"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity(
                ensure_registry_health_check_schedule,
                target,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "schedule"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            # Only publish once Swarm, the proxy entry and the health schedule
            # are all confirmed for the target generation.
            await workflow.execute_activity(
                publish_registry_generation,
                payload.generation_id,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity(
                cleanup_superseded_registry_configs,
                payload.generation_id,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )
        finally:
            await workflow.execute_activity(
                release_registry_deploy_semaphore,
                target.id,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )


@workflow.defn(name="update-build-registry")
class UpdateBuildRegistryWorkflow:
    def __init__(self):
        self.retry_policy = RetryPolicy(
            maximum_attempts=5, maximum_interval=timedelta(seconds=30)
        )

    @workflow.run
    async def run(self, payload: BuildRegistryGenerationPayload):
        target = payload.target
        await workflow.execute_activity(
            acquire_registry_deploy_semaphore,
            target.id,
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=self.retry_policy,
        )

        await workflow.execute_activity_method(
            MonitorRegistryDeploymentActivites.save_registry_health_check_status,
            RegistryHealthCheckResult(
                id=target.id,
                version=target.version,
                status=BuildRegistry.RegistryHealthStatus.RESTARTING,
                reason="Redeploying registry...",
            ),
            retry_policy=self.retry_policy,
            start_to_close_timeout=timedelta(seconds=10),
        )

        healthcheck: Optional[RegistryHealthCheckResult] = None
        try:
            # A retried/outdated workflow reconciles through the generation:
            # only the workflow of the current staging generation proceeds.
            generation_status = await workflow.execute_activity(
                get_registry_generation_status,
                payload.generation_id,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=self.retry_policy,
            )
            if generation_status != BuildRegistryGeneration.GenerationStatus.STAGING:
                # generation already active or superseded by a newer change
                return

            # Prepare the new config & credentials in versioned staging
            # resources; the active version keeps running untouched.
            volume, config_data = await asyncio.gather(
                workflow.execute_activity(
                    create_docker_volume_for_registry,
                    target,
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=self.retry_policy,
                ),
                workflow.execute_activity(
                    create_docker_configs_for_registry,
                    target,
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=self.retry_policy,
                ),
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "staged"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity(
                pull_registry_image,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            swarm_details = SwarmRegistryServiceDetails(
                registry=target,
                configs={
                    cast(str, config.id): config for config in config_data.configs
                },
                volume=volume,
                alias=target.service_alias,
                swarm_id=target.swarm_service_name,
                generation_id=payload.generation_id,
            )

            # Verify the actual service state before/after the update so an
            # uncertain Docker response never causes a duplicate update.
            await workflow.execute_activity(
                reconcile_build_registry_swarm_service,
                swarm_details,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )

            healthcheck = await workflow.execute_activity(
                wait_for_registry_service_to_be_updated,
                target,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "swarm"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            # Proxy and health schedule are independent, separately persisted
            # stages: each can be retried on its own while the old proxy entry
            # keeps pointing at a usable service version.
            await workflow.execute_activity(
                upsert_registry_url_in_proxy,
                target,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "proxy"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            await workflow.execute_activity(
                ensure_registry_health_check_schedule,
                target,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )
            await workflow.execute_activity(
                mark_registry_generation_stage,
                _stage_details(payload, "schedule"),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )

            publish_status = await workflow.execute_activity(
                publish_registry_generation,
                payload.generation_id,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self.retry_policy,
            )

            if publish_status != BuildRegistryGeneration.GenerationStatus.ACTIVE:
                # A newer generation won the arbitration; this workflow must
                # not mark health nor reap resources.
                healthcheck = None
                return

            # Reference-based reaping of the previous generation's configs,
            # only after the new generation is live.
            await workflow.execute_activity(
                cleanup_superseded_registry_configs,
                payload.generation_id,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )
        except Exception as e:
            healthcheck = RegistryHealthCheckResult(
                id=target.id,
                version=target.version,
                status=BuildRegistry.RegistryHealthStatus.UNHEALTHY,
                reason=f"An error occured: {e}",
            )
            raise
        finally:
            await workflow.execute_activity(
                release_registry_deploy_semaphore,
                target.id,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=self.retry_policy,
            )
            if healthcheck is not None:
                await workflow.execute_activity_method(
                    MonitorRegistryDeploymentActivites.save_registry_health_check_status,
                    healthcheck,
                    retry_policy=self.retry_policy,
                    start_to_close_timeout=timedelta(seconds=10),
                )
