from django.db import models
from typing import TYPE_CHECKING, Any, Self, cast
from zane_api.models.base import TimestampedModel
from shortuuid.django_fields import ShortUUIDField
from django.utils.translation import gettext_lazy as _
from django.utils.text import slugify
from zane_api.validators import validate_url_domain

if TYPE_CHECKING:
    from django.db.models.manager import RelatedManager
    from zane_api.models.main import Project, Service, Workspace


class SharedRegistryCredentials(TimestampedModel):
    ID_PREFIX = "reg_cred_"

    if TYPE_CHECKING:
        services: RelatedManager["Service"]

    workspace: models.ForeignKey["Workspace"] = models.ForeignKey(
        to="zane_api.Workspace",
        on_delete=models.CASCADE,
        related_name="shared_credentials",
    )

    id = ShortUUIDField(  # type: ignore[arg-type]
        length=20,
        max_length=255,
        primary_key=True,
        prefix=ID_PREFIX,
    )

    class RegistryType(models.TextChoices):
        DOCKER_HUB = "DOCKER_HUB", _("Docker Hub")
        GITHUB = "GITHUB", _("GitHub Container Registry")
        GITLAB = "GITLAB", _("GitLab Container Registry")
        GOOGLE_ARTIFACT = "GOOGLE_ARTIFACT", _("Google Artifact Registry")
        AWS_ECR = "AWS_ECR", _("AWS Elastic Container Registry")
        GENERIC = "GENERIC", _("Generic Docker Registry (v2 API)")

    url = models.URLField(blank=False)
    password = models.TextField()
    username = models.CharField(max_length=1024)
    registry_type = models.CharField(
        max_length=32,
        choices=RegistryType.choices,
        default=RegistryType.GENERIC,
    )
    slug = models.SlugField(unique=True)

    def __str__(self):
        return f"SharedRegistryCredentials(registry_type={self.RegistryType(self.registry_type).label}, url={self.url}, username={self.username})"

    class Meta:  # type: ignore
        ordering = ["created_at"]
        indexes = [models.Index(fields=["registry_type"])]


# Create your models here
class BuildRegistry(TimestampedModel):
    if TYPE_CHECKING:
        projects: RelatedManager[Project]

    ID_PREFIX = "build_reg_"
    id = ShortUUIDField(primary_key=True, prefix=ID_PREFIX, length=20)  # type: ignore[arg-type]
    name = models.CharField(max_length=200)
    is_default = models.BooleanField(default=True)

    registry_domain = models.CharField(
        max_length=2048, validators=[validate_url_domain]
    )
    registry_username = models.CharField(max_length=1024, blank=False)
    registry_password = models.TextField(blank=False)

    version = models.PositiveIntegerField(default=1)

    class StorageBackend(models.TextChoices):
        LOCAL = "LOCAL", _("Local Disk")
        S3 = "S3", _("S3")

    # Only meaningful for managed registries
    storage_backend = models.CharField(
        max_length=50,
        choices=StorageBackend.choices,
        default=StorageBackend.LOCAL,
    )
    swarm_service_name = models.CharField(null=True)
    service_alias = models.CharField(null=True)

    is_secure = models.BooleanField(default=True)

    # S3 Configuration (for registry storage)
    s3_credentials = models.JSONField(null=True)

    class RegistryHealthStatus(models.TextChoices):
        PREPARING = "PREPARING", _("Preparing")
        STARTING = "STARTING", _("Starting")
        RESTARTING = "RESTARTING", _("Restarting")
        HEALTHY = "HEALTHY", _("Healthy")
        UNHEALTHY = "UNHEALTHY", _("Unhealthy")

    health_status = models.CharField(
        max_length=50,
        choices=RegistryHealthStatus.choices,
        default=RegistryHealthStatus.PREPARING,
    )

    healthcheck_message = models.TextField(default="preparing")

    @property
    def workflow_id(self) -> str:
        return self.get_generation_workflow_id(self.id, self.version)

    @classmethod
    def get_generation_workflow_id(cls, registry_id: str, version: int) -> str:
        return f"deploy-{registry_id}-v{version}"

    @property
    def monitor_schedule_id(self) -> str:
        return f"monitor-{self.id}"

    @property
    def destroy_workflow_id(self) -> str:
        return f"destroy-{self.id}"

    @classmethod
    def generate_default_service_alias(cls, instance: Self) -> str:
        prefix = slugify(instance.name).lower()
        suffix = cast(str, instance.id).replace(instance.ID_PREFIX, "").lower()
        return f"zn-{prefix}-{suffix}"

    @classmethod
    def generate_default_swarm_service_name(cls, instance: Self) -> str:
        prefix = slugify(instance.name).lower()
        suffix = cast(str, instance.id).replace(instance.ID_PREFIX, "").lower()
        return f"srv-{prefix}-{suffix}"

    class Meta:  # type: ignore
        constraints = [
            models.UniqueConstraint(
                fields=["is_default"],
                condition=models.Q(is_default=True),
                name="unique_global_registry_per_instance",
            ),
        ]
        indexes = [models.Index(fields=["registry_domain"])]
        ordering = ["created_at"]


class BuildRegistryGeneration(TimestampedModel):
    """
    Immutable, queryable configuration generation for a build registry.

    A generation is created in ``STAGING`` with an immutable snapshot of the
    target configuration (domain, credentials, storage, ...). It only becomes
    ``ACTIVE`` once the Swarm service, the proxy route and the health check
    schedule have all been confirmed for its version. Previously active
    generations are kept as immutable snapshots and marked ``SUPERSEDED``;
    their external resources are reaped by reference after the new generation
    is published.
    """

    ID_PREFIX = "build_reg_gen_"

    if TYPE_CHECKING:
        from django.db.models.manager import RelatedManager

        registry: "BuildRegistry"

    id = ShortUUIDField(  # type: ignore[arg-type]
        length=20,
        max_length=255,
        primary_key=True,
        prefix=ID_PREFIX,
    )

    registry: models.ForeignKey["BuildRegistry"] = models.ForeignKey(
        to="container_registry.BuildRegistry",
        on_delete=models.CASCADE,
        related_name="generations",
    )

    version = models.PositiveIntegerField()

    # Immutable snapshot of the configuration target of this generation:
    # name, registry_domain, registry_username, registry_password, is_secure,
    # storage_backend, s3_credentials, service_alias, swarm_service_name,
    # monitor_schedule_id.
    snapshot = models.JSONField()

    class GenerationStatus(models.TextChoices):
        STAGING = "STAGING", _("Staging")
        ACTIVE = "ACTIVE", _("Active")
        SUPERSEDED = "SUPERSEDED", _("Superseded")

    status = models.CharField(
        max_length=20,
        choices=GenerationStatus.choices,
        default=GenerationStatus.STAGING,
    )

    # Persisted stage confirmations, used to recover without re-incrementing
    # the version and to only publish once every stage is confirmed.
    staged_at = models.DateTimeField(null=True)
    swarm_confirmed_at = models.DateTimeField(null=True)
    proxy_confirmed_at = models.DateTimeField(null=True)
    schedule_confirmed_at = models.DateTimeField(null=True)
    published_at = models.DateTimeField(null=True)
    cleaned_at = models.DateTimeField(null=True)

    # Fields of the snapshot that affect the deployed registry. Changes to
    # `is_default` do not require a new generation.
    DEPLOYMENT_FIELDS = (
        "name",
        "registry_domain",
        "registry_username",
        "registry_password",
        "is_secure",
        "storage_backend",
        "s3_credentials",
    )

    @property
    def workflow_id(self) -> str:
        return BuildRegistry.get_generation_workflow_id(
            self.registry_id, self.version
        )

    @classmethod
    def snapshot_from_registry(
        cls,
        registry: "BuildRegistry",
        *,
        version: int | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot = {
            "name": registry.name,
            "registry_domain": registry.registry_domain,
            "registry_username": registry.registry_username,
            "registry_password": registry.registry_password,
            "is_secure": registry.is_secure,
            "storage_backend": registry.storage_backend,
            "s3_credentials": registry.s3_credentials,
            "service_alias": registry.service_alias,
            "swarm_service_name": registry.swarm_service_name,
            "monitor_schedule_id": registry.monitor_schedule_id,
            "version": registry.version if version is None else version,
        }
        if overrides is not None:
            snapshot.update(overrides)
        return snapshot

    def deployment_fields_match(self, other_snapshot: dict[str, Any]) -> bool:
        return all(
            self.snapshot.get(field) == other_snapshot.get(field)
            for field in self.DEPLOYMENT_FIELDS
        )

    class Meta:  # type: ignore
        constraints = [
            models.UniqueConstraint(
                fields=["registry", "version"],
                name="unique_registry_generation_version",
            ),
        ]
        indexes = [
            models.Index(
                fields=["registry", "status"],
                name="container_r_gen_status_idx",
            ),
        ]
        ordering = ["version"]
