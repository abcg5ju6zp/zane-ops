import asyncio
from datetime import timedelta
from typing import Any, Literal, Protocol, cast
from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError

with workflow.unsafe.imports_passed_through():
    import docker
    import docker.errors
    import requests
    from asgiref.sync import sync_to_async
    from django.conf import settings
    from django.db import transaction
    from django.utils import timezone
    from zane_api.utils import Colors
    from zane_api.dtos import VolumeDto, ConfigDto
    from docker.types import (
        ConfigReference,
        NetworkAttachmentConfig,
        UpdateConfig,
        RestartPolicy,
        Healthcheck,
        Mount,
    )
    from ..helpers import get_docker_client
    from ..proxy import ZaneProxyClient, ZaneProxyEtagError
    from ..semaphore import AsyncSemaphore
    from ..schedules import MonitorRegistrySwarmServiceWorkflow
    from container_registry.models import (
        BuildRegistry,
        BuildRegistryGeneration,
    )

from ..client import TemporalClient

from ..shared import (
    DeleteSwarmRegistryDomainDetails,
    SwarmRegistryServiceDetails,
    DeleteSwarmRegistryServiceDetails,
    CreateBuildRegistryConfigsDetails,
    RegistrySnaphot,
    RegistryHealthCheckResult,
    RegistryGenerationStageDetails,
)
from ..constants import (
    BUILD_REGISTRY_VOLUME_PATH,
    BUILD_REGISTRY_CONFIG_PATH,
    BUILD_REGISTRY_PASSWORD_PATH,
    BUILD_REGISTRY_IMAGE,
    BUILD_REGISTRY_DEPLOY_SEMAPHORE_KEY,
)
import platform
from zane_api.utils import DockerSwarmTask, DockerSwarmTaskState, find_item_in_sequence


# Maximum number of polls waiting for a swarm task to converge to a target
# version. Each poll waits DEFAULT_HEALTHCHECK_WAIT_INTERVAL seconds (time is
# skipped instantly in the Temporal test environment).
MAX_SWARM_CONVERGENCE_POLLS = 60

# Maps a workflow stage to the persisted timestamp on the generation row.
STAGE_FIELDS = {
    "staged": "staged_at",
    "swarm": "swarm_confirmed_at",
    "proxy": "proxy_confirmed_at",
    "schedule": "schedule_confirmed_at",
    "published": "published_at",
    "cleaned": "cleaned_at",
}


class SimpleRegistryPayloadLike(Protocol):
    @property
    def swarm_service_name(self) -> str: ...


class DeployRegistryPayloadLike(Protocol):
    swarm_service_name: str
    version: int


def get_resource_labels(details: SimpleRegistryPayloadLike, **kwargs: str):
    return {"zane-managed": "true", "parent": details.swarm_service_name, **kwargs}


def get_volume_name_for_registry(
    details: DeployRegistryPayloadLike,
) -> str:
    return f"vol-{details.swarm_service_name}"


def get_config_version_name(
    swarm_service_name: str,
    version: int,
    type: Literal["config", "credentials"],
):
    return f"cfg-{swarm_service_name}-{type}-v{version}"


def get_config_name_for_registry(
    details: DeployRegistryPayloadLike,
    type: Literal["config", "credentials"],
):
    return get_config_version_name(details.swarm_service_name, details.version, type)


async def _aget_generation(
    generation_id: str,
) -> BuildRegistryGeneration:
    """
    Return the generation or fail non-retryably when it is gone. A missing
    generation means the registry was deleted or the request belongs to an
    aborted change: external resources must never be (re)created in that case.
    """
    try:
        return await BuildRegistryGeneration.objects.aget(pk=generation_id)
    except BuildRegistryGeneration.DoesNotExist:
        raise ApplicationError(
            f"Build registry generation `{generation_id}` does not exist anymore, "
            "aborting the workflow without touching external resources.",
            non_retryable=True,
        )


async def _assert_generation_exists(generation_id: str | None):
    if generation_id is not None:
        await _aget_generation(generation_id)


def _newest_running_task(swarm_service: Any):
    tasks = swarm_service.tasks(filters={"desired-state": "running"})
    if len(tasks) == 0:
        return None
    return DockerSwarmTask.from_dict(
        max(tasks, key=lambda task: task["Version"]["Index"])
    )


def _get_task_version(task: Any) -> str | None:
    env_var = find_item_in_sequence(
        lambda env: env.startswith("__ZANE_VERSION="),
        task.Spec.ContainerSpec.Env,
    )
    if env_var is None:
        return None
    return cast(str, env_var).split("=", 1)[1]


def _get_service_spec_version(swarm_service: Any) -> str | None:
    labels = swarm_service.attrs.get("Spec", {}).get("Labels", {}) or {}
    return labels.get("version")


@activity.defn
async def acquire_registry_deploy_semaphore(registry_id: str):
    if settings.TESTING:
        return  # semaphores are causing issues in testing, blocking execution
    semaphore = AsyncSemaphore(
        key=f"{BUILD_REGISTRY_DEPLOY_SEMAPHORE_KEY}-{registry_id}",
        limit=1,
        semaphore_timeout=timedelta(minutes=20),
    )
    await semaphore.acquire()


@activity.defn
async def release_registry_deploy_semaphore(registry_id: str):
    if settings.TESTING:
        return  # semaphores are causing issues in testing, blocking execution
    semaphore = AsyncSemaphore(
        key=f"{BUILD_REGISTRY_DEPLOY_SEMAPHORE_KEY}-{registry_id}",
        limit=1,
        semaphore_timeout=timedelta(minutes=20),
    )
    await semaphore.release()


@activity.defn
async def get_registry_generation_status(generation_id: str) -> str:
    generation = await _aget_generation(generation_id)
    return cast(str, generation.status)


@activity.defn
async def mark_registry_generation_stage(details: RegistryGenerationStageDetails):
    generation = await _aget_generation(details.generation_id)
    field = STAGE_FIELDS[details.stage]
    if getattr(generation, field) is None:
        setattr(generation, field, timezone.now())
        await generation.asave(update_fields=[field])


@activity.defn
async def create_docker_volume_for_registry(payload: RegistrySnaphot):
    await _assert_generation_exists(payload.generation_id)
    client = get_docker_client()
    print(
        f"Creating docker volume for registry {Colors.ORANGE}{payload.name}{Colors.ENDC}..."
    )
    volume = VolumeDto(
        mode="READ_WRITE",
        container_path=BUILD_REGISTRY_VOLUME_PATH,
        id=get_volume_name_for_registry(payload),
    )
    try:
        client.volumes.get(volume.id)  # type: ignore
    except docker.errors.NotFound:
        client.volumes.create(
            name=volume.id,  # type: ignore
            driver="local",
            labels=get_resource_labels(payload),
        )
    print(
        f"Volume created succesfully for registry {Colors.ORANGE}{payload.name}{Colors.ENDC}  ✅",
    )
    return volume


@activity.defn
async def pull_registry_image() -> bool:
    client = get_docker_client()
    print(
        f"Pulling image {Colors.ORANGE}{BUILD_REGISTRY_IMAGE}{Colors.ENDC}...",
    )
    try:
        client.images.pull(repository=BUILD_REGISTRY_IMAGE)
    except docker.errors.ImageNotFound:
        print(
            f"Error when pulling image {Colors.ORANGE}{BUILD_REGISTRY_IMAGE}{Colors.ENDC} {Colors.GREY}this image either does not exists for this platform (linux/{platform.machine()}) or may require authentication credentials to be pulled ❌{Colors.ENDC}",
        )
        return False
    except docker.errors.APIError as e:
        print(
            f"Error when pulling image {Colors.ORANGE}{BUILD_REGISTRY_IMAGE}{Colors.ENDC} {Colors.GREY}{e.explanation} ❌{Colors.ENDC}",
        )
        return False
    else:
        print(
            f"Finished pulling image {Colors.ORANGE}{BUILD_REGISTRY_IMAGE}{Colors.ENDC} ✅",
        )
        return True


@activity.defn
async def create_docker_configs_for_registry(
    payload: RegistrySnaphot,
) -> CreateBuildRegistryConfigsDetails:
    import bcrypt

    # The configs carry the target generation version in their stable identity:
    # staging a new generation never touches the resources of the active one.
    await _assert_generation_exists(payload.generation_id)
    client = get_docker_client()
    print(
        f"Creating docker swarm configs for registry {Colors.ORANGE}{payload.name}{Colors.ENDC}..."
    )

    configFile = ConfigDto(
        mount_path=BUILD_REGISTRY_CONFIG_PATH,
        language="yaml",
        contents=payload.config.to_yaml(),
        id=get_config_name_for_registry(payload, type="config"),
    )

    print(f"[configFile] {configFile.id}.contents={configFile.contents}")

    try:
        client.configs.get(configFile.id)  # type: ignore
    except docker.errors.NotFound:
        client.configs.create(
            name=configFile.id,  # type: ignore
            labels=get_resource_labels(payload),
            data=configFile.contents.encode("utf-8"),
        )

    credentialsFile = ConfigDto(
        mount_path=BUILD_REGISTRY_PASSWORD_PATH,
        language="dotenv",
        contents=f"{payload.username}:{
            bcrypt.hashpw(
                payload.password.encode('utf-8'),
                bcrypt.gensalt(),
            ).decode('utf-8')
        }",
        id=get_config_name_for_registry(payload, type="credentials"),
    )
    print(f"[credentialsFile] {credentialsFile.id}.contents={credentialsFile.contents}")
    try:
        client.configs.get(credentialsFile.id)  # type: ignore
    except docker.errors.NotFound:
        client.configs.create(
            name=credentialsFile.id,  # type: ignore
            labels=get_resource_labels(payload),
            data=credentialsFile.contents.encode("utf-8"),
        )

    print(
        f"Swarm Configs created succesfully for registry {Colors.ORANGE}{payload.name}{Colors.ENDC}  ✅",
    )
    return CreateBuildRegistryConfigsDetails(configs=[configFile, credentialsFile])


@activity.defn
async def cleanup_superseded_registry_configs(generation_id: str):
    """
    Reference-based reaping: remove the versioned docker configs of every
    SUPERSEDED generation whose version is strictly below the version currently
    published on the registry. Configs still referenced by the active service
    or by an in-flight generation are never deleted.
    """
    generation = await _aget_generation(generation_id)
    try:
        registry = await BuildRegistry.objects.aget(pk=generation.registry_id)
    except BuildRegistry.DoesNotExist:
        raise ApplicationError(
            f"Build registry `{generation.registry_id}` does not exist anymore, "
            "the destroy workflow owns resource cleanup.",
            non_retryable=True,
        )

    client = get_docker_client()
    superseded_generations = [
        gen
        async for gen in BuildRegistryGeneration.objects.filter(
            registry_id=registry.id,
            status=BuildRegistryGeneration.GenerationStatus.SUPERSEDED,
        ).order_by("version")
    ]

    for old_generation in superseded_generations:
        if old_generation.version >= registry.version:
            # reference guard: a live (or newer in-flight) service may still
            # reference these configs
            continue
        if old_generation.cleaned_at is not None:
            continue

        all_removed = True
        config_types: list[Literal["config", "credentials"]] = [
            "config",
            "credentials",
        ]
        for type in config_types:
            config_name = get_config_version_name(
                cast(str, registry.swarm_service_name),
                old_generation.version,
                type,
            )
            try:
                config = client.configs.get(config_name)
            except docker.errors.NotFound:
                continue
            try:
                config.remove()
                print(f"Deleted superseded swarm config `{config_name}` ✅")
            except docker.errors.APIError:
                # The config is likely still attached to a converging swarm
                # task; leave it for a later generation's cleanup.
                all_removed = False

        if all_removed:
            old_generation.cleaned_at = timezone.now()
            await old_generation.asave(update_fields=["cleaned_at"])


@activity.defn
async def remove_service_registry_url(payload: DeleteSwarmRegistryDomainDetails):
    ZaneProxyClient.remove_build_registry_url(
        payload.service_alias,
    )


@activity.defn
async def cleanup_docker_registry_service_resources(
    payload: DeleteSwarmRegistryServiceDetails,
):
    client = get_docker_client()
    try:
        swarm_service = client.services.get(payload.swarm_service_name)
    except docker.errors.NotFound:
        print(f"service `{payload.swarm_service_name}` not found")
        # we will assume the service has already been deleted
        pass
    else:
        swarm_service.remove()

    async def wait_for_service_containers_to_be_removed():
        print(
            f"waiting for containers for service {payload.swarm_service_name=} to be removed..."
        )
        container_list = client.containers.list(
            filters={"name": payload.swarm_service_name}
        )
        while len(container_list) > 0:
            print(
                f"service {payload.swarm_service_name=} is not removed yet, "
                + f"retrying in {settings.DEFAULT_HEALTHCHECK_WAIT_INTERVAL} seconds..."
            )
            await asyncio.sleep(settings.DEFAULT_HEALTHCHECK_WAIT_INTERVAL)
            container_list = client.containers.list(
                filters={"name": payload.swarm_service_name}
            )
            continue
        print(f"service {payload.swarm_service_name=} is removed, YAY !! 🎉")

    await wait_for_service_containers_to_be_removed()

    print("Removed service. YAY !! 🎉")
    print("deleting volume list...")
    docker_volume_list = client.volumes.list(
        filters={
            "label": [
                f"{key}={value}" for key, value in get_resource_labels(payload).items()
            ]
        }
    )

    for volume in docker_volume_list:
        volume.remove(force=True)
    print(f"Deleted {len(docker_volume_list)} volume(s), YAY !! 🎉")

    print("deleting config list...")
    docker_config_list = client.configs.list(
        filters={
            "label": [
                f"{key}={value}" for key, value in get_resource_labels(payload).items()
            ]
        }
    )

    for config in docker_config_list:
        config.remove()
    print(f"Deleted {len(docker_config_list)} config(s), YAY !! 🎉")


@activity.defn
async def upsert_registry_url_in_proxy(payload: RegistrySnaphot):
    # The proxy stage is independently recoverable: when the write response is
    # uncertain, we verify the actual route and only redo the write if the
    # target domain is not already served for the service alias.
    await _assert_generation_exists(payload.generation_id)
    alias = payload.service_alias

    def route_is_up_to_date(route) -> bool:
        return ZaneProxyClient.is_build_registry_route_up_to_date(
            route,
            alias,
            payload.domain,
        )

    try:
        current_route = ZaneProxyClient.get_build_registry_route(alias)
        if route_is_up_to_date(current_route):
            print(f"Proxy route already up to date for {alias}, skipping upsert.")
            return
        ZaneProxyClient.upsert_registry_url(
            registry_id=payload.id,
            registry_alias=alias,
            domain=payload.domain,
            is_secure=payload.is_secure,
        )
    except (requests.RequestException, ZaneProxyEtagError) as error:
        print(f"Uncertain proxy response for {alias}: {error}, reconciling...")
        try:
            actual_route = ZaneProxyClient.get_build_registry_route(alias)
        except requests.RequestException:
            raise ApplicationError(
                f"Proxy route state could not be confirmed: {error}"
            )
        if route_is_up_to_date(actual_route):
            return
        raise ApplicationError(f"Proxy route update was not confirmed: {error}")


def _update_swarm_service_spec(
    swarm_service: Any, service: SwarmRegistryServiceDetails
):
    client = get_docker_client()
    # Volumes
    mounts: list[Mount] = []
    if service.volume is not None:
        mounts.append(
            Mount(
                target=service.volume.container_path,
                source=service.volume.id,
                type="volume",
            )
        )

    # configs
    configs: list[ConfigReference] = []
    docker_config_list = client.configs.list(
        filters={
            "label": [
                f"{key}={value}"
                for key, value in get_resource_labels(service.registry).items()
            ]
        }
    )
    for config in docker_config_list:
        config_id = cast(str, config.id)
        # only attach the target generation's configs, never previous versions
        existing = service.configs.get(config.name)
        if existing is not None:
            configs.append(
                ConfigReference(
                    config_id=config_id,
                    config_name=config.name,
                    filename=service.configs[config.name].mount_path,
                )
            )

    print(
        f"Updating swarm service for registry {Colors.ORANGE}{service.registry.name}{Colors.ENDC}..."
    )
    swarm_service.update(
        image=BUILD_REGISTRY_IMAGE,
        mounts=mounts,
        labels=get_resource_labels(
            service.registry,
            type="registry",
            version=str(service.registry.version),
            status="active",
        ),
        env=[
            f"__ZANE_VERSION={service.registry.version}",
            "OTEL_TRACES_EXPORTER=none",
        ],
        networks=[
            NetworkAttachmentConfig(
                target="zane",
                aliases=[
                    cast(str, service.alias),
                    cast(str, service.alias).replace(
                        f".{settings.ZANE_INTERNAL_DOMAIN}", ""
                    ),
                ],
            ),
        ],
        update_config=UpdateConfig(
            order="start-first",
            parallelism=1,
        ),
        restart_policy=RestartPolicy(
            condition="any",
        ),
        healthcheck=Healthcheck(
            test=[
                "CMD",
                "wget",
                "--quiet",
                "--tries=1",
                "--spider",
                "http://localhost:5001/debug/health",
            ],
            # times are in nanoseconds, that's why they are mutiplied by `e9` (1 billion)
            interval=int(10e9),
            timeout=int(5e9),
            retries=3,
            start_period=int(5e9),
        ),
        stop_grace_period=int(30e9),
        configs=configs,
    )
    print(
        f"Succesfully updated swarm service for the registry {Colors.ORANGE}{service.registry.name}{Colors.ENDC} ✅",
    )


@activity.defn
async def reconcile_build_registry_swarm_service(
    service: SwarmRegistryServiceDetails,
):
    """
    Reconcile the swarm service against the target generation version:
      * if a running task already serves the target version, nothing is done;
      * if the target spec was already applied (task still converging after an
        uncertain response), the update is NOT issued a second time;
      * otherwise the service is updated exactly once.
    """
    await _assert_generation_exists(service.generation_id)
    client = get_docker_client()
    target_version = str(service.registry.version)

    try:
        swarm_service = client.services.get(service.swarm_id)
    except docker.errors.NotFound:
        raise ApplicationError("This registry has not been deployed yet")

    def running_task_is_at_target() -> bool:
        task = _newest_running_task(swarm_service)
        if task is None:
            return False
        return (
            task.state == DockerSwarmTaskState.RUNNING
            and _get_task_version(task) == target_version
        )

    if running_task_is_at_target():
        print(
            f"Swarm service `{service.swarm_id}` already runs v{target_version}, "
            "skipping update."
        )
        return

    if _get_service_spec_version(swarm_service) == target_version:
        print(
            f"Swarm service `{service.swarm_id}` already targets v{target_version}, "
            "waiting for the task to converge without re-issuing the update."
        )
        return

    try:
        _update_swarm_service_spec(swarm_service, service)
    except docker.errors.APIError as error:
        # The update response is uncertain: verify the real service state and
        # only proceed when the target version is confirmed.
        print(f"Uncertain swarm update response: {error}, reconciling...")
        try:
            refreshed_service = client.services.get(service.swarm_id)
        except docker.errors.NotFound:
            raise ApplicationError(
                f"Swarm service disappeared after an uncertain update: {error}"
            )
        if _get_service_spec_version(refreshed_service) == target_version:
            print("Reconciliation confirmed the target spec was applied.")
            return
        raise ApplicationError(
            f"Swarm service update to v{target_version} is not confirmed: {error}"
        )


@activity.defn
async def wait_for_registry_service_to_be_updated(
    payload: RegistrySnaphot,
) -> RegistryHealthCheckResult:
    await _assert_generation_exists(payload.generation_id)
    client = get_docker_client()
    target_version = str(payload.version)

    print(f"waiting for service {payload.swarm_service_name=} to run v{target_version}...")
    for _ in range(MAX_SWARM_CONVERGENCE_POLLS):
        try:
            swarm_service = client.services.get(payload.swarm_service_name)
        except docker.errors.NotFound:
            raise ApplicationError("This registry has not been deployed yet")

        current_service_task = _newest_running_task(swarm_service)
        current_version = (
            _get_task_version(current_service_task)
            if current_service_task is not None
            else None
        )

        if (
            current_service_task is not None
            and current_service_task.state == DockerSwarmTaskState.RUNNING
            and current_version == target_version
        ):
            print(
                f"service {swarm_service.name=} runs v{target_version}, YAY !! 🎉"
            )
            return RegistryHealthCheckResult(
                id=payload.id,
                version=payload.version,
                status=BuildRegistry.RegistryHealthStatus.HEALTHY,
                reason="Service updated successfully",
            )

        print(
            f"service {swarm_service.name=} is not at v{target_version} yet "
            f"(running v{current_version}), retrying in "
            f"{settings.DEFAULT_HEALTHCHECK_WAIT_INTERVAL} seconds..."
        )
        await asyncio.sleep(settings.DEFAULT_HEALTHCHECK_WAIT_INTERVAL)

    raise ApplicationError(
        f"Timed out waiting for the swarm service to run v{target_version}"
    )


@activity.defn
async def create_build_registry_swarm_service(
    service: SwarmRegistryServiceDetails,
):
    await _assert_generation_exists(service.generation_id)
    client = get_docker_client()

    try:
        client.services.get(service.swarm_id)
    except docker.errors.NotFound:
        # Volumes
        mounts: list[Mount] = []
        if service.volume is not None:
            mounts.append(
                Mount(
                    target=service.volume.container_path,
                    source=service.volume.id,
                    type="volume",
                )
            )

        # configs
        configs: list[ConfigReference] = []
        docker_config_list = client.configs.list(
            filters={
                "label": [
                    f"{key}={value}"
                    for key, value in get_resource_labels(service.registry).items()
                ]
            }
        )
        for config in docker_config_list:
            configs.append(
                ConfigReference(
                    config_id=cast(str, config.id),
                    config_name=config.name,
                    filename=service.configs[config.name].mount_path,
                )
            )

        print(
            f"Creating swarm service  for registry {Colors.ORANGE}{service.registry.name}{Colors.ENDC}..."
        )
        client.services.create(
            image=BUILD_REGISTRY_IMAGE,
            name=service.swarm_id,
            mounts=mounts,
            labels=get_resource_labels(
                service.registry,
                type="registry",
                version=str(service.registry.version),
                status="active",
            ),
            env=[
                f"__ZANE_VERSION={service.registry.version}",
                "OTEL_TRACES_EXPORTER=none",
            ],
            networks=[
                NetworkAttachmentConfig(
                    target="zane",
                    aliases=[
                        cast(str, service.alias),
                        cast(str, service.alias).replace(
                            f".{settings.ZANE_INTERNAL_DOMAIN}", ""
                        ),
                    ],
                ),
            ],
            update_config=UpdateConfig(
                order="start-first",
                parallelism=1,
            ),
            restart_policy=RestartPolicy(
                condition="any",
            ),
            healthcheck=Healthcheck(
                test=[
                    "CMD",
                    "wget",
                    "--quiet",
                    "--tries=1",
                    "--spider",
                    "http://localhost:5001/debug/health",
                ],
                # times are in nanoseconds, that's why they are mutiplied by `e9` (1 billion)
                interval=int(10e9),
                timeout=int(5e9),
                retries=3,
                start_period=int(5e9),
            ),
            stop_grace_period=int(30e9),
            configs=configs,
        )
        print(
            f"Swarm service created succesfully for the registry {Colors.ORANGE}{service.registry.name}{Colors.ENDC} ✅",
        )


@activity.defn
async def ensure_registry_health_check_schedule(registry: RegistrySnaphot):
    """
    Create or update the monitoring schedule so that scheduled runs always
    carry the current generation snapshot. Independently recoverable: upsert
    is idempotent and a failure here does not roll back swarm/proxy stages.
    """
    await _assert_generation_exists(registry.generation_id)
    await TemporalClient.aupsert_schedule(
        workflow=MonitorRegistrySwarmServiceWorkflow.run,
        args=registry,
        id=registry.monitor_schedule_id,
        interval=timedelta(seconds=30),
        task_queue=settings.TEMPORALIO_SCHEDULE_TASK_QUEUE,
    )


@activity.defn
async def delete_registry_health_check_schedule(
    registry: DeleteSwarmRegistryServiceDetails,
):
    try:
        await TemporalClient.adelete_schedule(registry.monitor_schedule_id)
    except RPCError:
        # Probably already deleted
        pass


def _publish_registry_generation_sync(generation_id: str) -> str:
    """
    Publish a staging generation as the active one.

    The publication is the single arbitration point: an older generation whose
    version is behind the live registry version (concurrent modification,
    retried outdated workflow) is marked SUPERSEDED without overwriting
    anything. Swarm/proxy/schedule confirmations are required beforehand.
    """
    with transaction.atomic():
        try:
            generation = BuildRegistryGeneration.objects.select_for_update().get(
                pk=generation_id
            )
        except BuildRegistryGeneration.DoesNotExist:
            raise ApplicationError(
                f"Build registry generation `{generation_id}` does not exist anymore.",
                non_retryable=True,
            )

        try:
            registry = BuildRegistry.objects.select_for_update().get(
                pk=generation.registry_id
            )
        except BuildRegistry.DoesNotExist:
            raise ApplicationError(
                f"Build registry `{generation.registry_id}` does not exist anymore.",
                non_retryable=True,
            )

        active = BuildRegistryGeneration.GenerationStatus.ACTIVE
        superseded = BuildRegistryGeneration.GenerationStatus.SUPERSEDED

        if generation.status == active:
            return cast(str, active)

        # A newer generation is already live (concurrent edits / late workflow).
        if registry.version > generation.version:
            generation.status = superseded
            generation.save(update_fields=["status"])
            return cast(str, superseded)

        snapshot = generation.snapshot
        snapshot_mismatch = any(
            getattr(registry, field) != snapshot.get(field)
            for field in BuildRegistryGeneration.DEPLOYMENT_FIELDS
        )
        if snapshot_mismatch:
            generation.status = superseded
            generation.save(update_fields=["status"])
            return cast(str, superseded)

        unconfirmed_stages = [
            stage
            for stage, field in (
                ("staged", "staged_at"),
                ("swarm", "swarm_confirmed_at"),
                ("proxy", "proxy_confirmed_at"),
                ("schedule", "schedule_confirmed_at"),
            )
            if getattr(generation, field) is None
        ]
        if len(unconfirmed_stages) > 0:
            raise ApplicationError(
                "Cannot publish the build registry generation, unconfirmed stages: "
                f"{unconfirmed_stages}",
                non_retryable=True,
            )

        generation.status = active
        generation.published_at = timezone.now()
        generation.save(update_fields=["status", "published_at"])

        BuildRegistry.objects.filter(pk=registry.pk).update(
            health_status=BuildRegistry.RegistryHealthStatus.HEALTHY,
            healthcheck_message="Service deployed successfully",
        )

        BuildRegistryGeneration.objects.filter(
            registry_id=registry.pk,
            status=active,
        ).exclude(pk=generation.pk).update(status=superseded)

        # Older in-flight attempts can never become active once a newer
        # generation is published.
        BuildRegistryGeneration.objects.filter(
            registry_id=registry.pk,
            status=BuildRegistryGeneration.GenerationStatus.STAGING,
            version__lt=generation.version,
        ).update(status=superseded)

        return cast(str, active)


@activity.defn
async def publish_registry_generation(generation_id: str) -> str:
    return await sync_to_async(
        _publish_registry_generation_sync,
        thread_sensitive=True,
    )(generation_id)
