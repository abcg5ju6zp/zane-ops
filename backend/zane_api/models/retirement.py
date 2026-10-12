# type: ignore
"""
Workspace retirement (decommissioning) models.

A workspace deletion is a *queryable retirement generation* instead of an
immediate hard delete:

- :class:`WorkspaceRetirement` is the generation root. The workspace stays
  visible (writes frozen) while ``status == RETIRING`` and the row is kept
  (detached via ``SET_NULL``) after the workspace itself is hidden, so a
  repeated delete keeps returning the same generation instead of deleting
  database objects a second time.
- :class:`WorkspaceCleanupTask` is the transactional outbox: one row per
  cleanup unit (service, compose stack, environment, project network) with a
  stable workflow ID and a delivery status. Rows are created inside the
  retirement transaction, workflows are started *after* commit, and a lost
  start response / restart only re-issues rows that were never confirmed.
- :class:`WorkspaceCleanupResource` records the external resource result
  (container, volume, URL, config, healthcheck schedule, buildkit builder,
  preview environment, network, stack) independently, so a partial failure
  resumes exactly the resources that were never confirmed.
"""

import uuid

from django.db import models

from .base import TimestampedModel


class WorkspaceRetirement(TimestampedModel):
    class RetirementStatus(models.TextChoices):
        # Snapshot fixed, cleanup units running, workspace still queryable
        RETIRING = "RETIRING", "Retiring"
        # Every external resource and cleanup workflow confirmed, workspace hidden
        COMPLETED = "COMPLETED", "Completed"

    class DeliveryStatus(models.TextChoices):
        # In the outbox, workflow start has not been acknowledged
        PENDING = "PENDING", "Pending"
        # Workflow start acknowledged (or proven already started via stable ID)
        STARTED = "STARTED", "Started"
        # Workflow result confirmed
        CONFIRMED = "CONFIRMED", "Confirmed"
        # Start/execution failed; eligible for another delivery attempt
        FAILED = "FAILED", "Failed"

    # SET_NULL (not CASCADE): the retirement record must survive the workspace
    # deletion performed at finalization so completed generations stay
    # queryable and repeated deletes converge to the same result.
    workspace = models.OneToOneField(
        "Workspace",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="retirement",
    )
    # The original workspace id, preserved after the FK is detached.
    workspace_original_id = models.CharField(max_length=255)
    generation = models.UUIDField(default=uuid.uuid4, unique=True, db_index=True)
    status = models.CharField(
        max_length=20,
        choices=RetirementStatus.choices,
        default=RetirementStatus.RETIRING,
        db_index=True,
    )

    # Frozen workspace manifest: projects / environments / services / compose
    # stacks and the external resources that have to be cleaned up.
    snapshot = models.JSONField(default=dict)
    # Deterministic convergence summary written at finalization
    # (memberships, tokens, invitations counts and session fallbacks).
    result = models.JSONField(null=True, blank=True)

    # Stable workflow ID of the top-level retirement workflow.
    workflow_id = models.CharField(max_length=255, unique=True)
    delivery_status = models.CharField(
        max_length=20,
        choices=DeliveryStatus.choices,
        default=DeliveryStatus.PENDING,
    )

    started_at = models.DateTimeField(null=True, blank=True)
    finalized_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    @property
    def is_active(self) -> bool:
        return self.status == self.RetirementStatus.RETIRING

    @property
    def progress(self) -> dict:
        tasks = self.tasks.all()
        total = len(tasks)
        confirmed = sum(
            1
            for t in tasks
            if t.delivery_status == self.DeliveryStatus.CONFIRMED
        )
        failed = sum(
            1 for t in tasks if t.delivery_status == self.DeliveryStatus.FAILED
        )
        return {
            "total_units": total,
            "confirmed_units": confirmed,
            "failed_units": failed,
            "pending_units": total - confirmed - failed,
            "is_complete": total > 0 and confirmed == total,
        }

    def __str__(self):
        return f"WorkspaceRetirement({self.workspace_original_id}, {self.status})"


class WorkspaceCleanupTask(TimestampedModel):
    class UnitType(models.TextChoices):
        # Remove one archived service's swarm service, containers, volumes,
        # configs, URLs and health schedules
        SERVICE = "SERVICE", "Service"
        # Remove one compose stack (stack CLI rm + volumes/configs/routes)
        COMPOSE_STACK = "COMPOSE_STACK", "Compose stack"
        # Delete one environment's buildkit builder / preview resources
        ENVIRONMENT = "ENVIRONMENT", "Environment"
        # Remove the project docker network, always last for the project
        PROJECT = "PROJECT", "Project network"

    class DeliveryStatus(models.TextChoices):
        PENDING = "PENDING", "Pending"
        STARTED = "STARTED", "Started"
        CONFIRMED = "CONFIRMED", "Confirmed"
        FAILED = "FAILED", "Failed"

    retirement = models.ForeignKey(
        WorkspaceRetirement,
        on_delete=models.CASCADE,
        related_name="tasks",
    )
    unit_type = models.CharField(max_length=20, choices=UnitType.choices)
    # Original id of the unit (service / stack / environment / project).
    unit_key = models.CharField(max_length=255)
    # Deterministic workflow ID, stable for the whole generation: a re-issued
    # start with the same id is idempotent (WorkflowAlreadyStarted).
    workflow_id = models.CharField(max_length=255)
    # Serialized workflow payload (dataclass -> dict), enough to re-deliver
    # this single unit without touching the database objects again.
    payload = models.JSONField(default=dict)

    delivery_status = models.CharField(
        max_length=20,
        choices=DeliveryStatus.choices,
        default=DeliveryStatus.PENDING,
        db_index=True,
    )
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    confirmed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["retirement", "unit_type", "unit_key"],
                name="unique_cleanup_unit_per_retirement",
            ),
        ]
        indexes = [
            models.Index(fields=["delivery_status"]),
            models.Index(fields=["workflow_id"]),
        ]


class WorkspaceCleanupResource(TimestampedModel):
    class ResourceType(models.TextChoices):
        CONTAINER = "CONTAINER", "Container / swarm service"
        VOLUME = "VOLUME", "Docker volume"
        CONFIG = "CONFIG", "Docker config"
        URL = "URL", "HTTP route / URL"
        HEALTHCHECK = "HEALTHCHECK", "Healthcheck / metrics schedule"
        BUILDKIT_BUILDER = "BUILDKIT_BUILDER", "Buildkit builder"
        PREVIEW_ENVIRONMENT = "PREVIEW_ENVIRONMENT", "Preview environment"
        NETWORK = "NETWORK", "Docker network"
        STACK = "STACK", "Compose stack"

    class ResourceStatus(models.TextChoices):
        PENDING = "PENDING", "Pending"
        CONFIRMED = "CONFIRMED", "Confirmed"
        FAILED = "FAILED", "Failed"

    task = models.ForeignKey(
        WorkspaceCleanupTask,
        on_delete=models.CASCADE,
        related_name="resources",
    )
    resource_type = models.CharField(max_length=20, choices=ResourceType.choices)
    # Stable external key (service original id, volume name, domain/base_path,
    # builder name, network name ...).
    resource_key = models.CharField(max_length=1000)
    status = models.CharField(
        max_length=20,
        choices=ResourceStatus.choices,
        default=ResourceStatus.PENDING,
        db_index=True,
    )
    result = models.JSONField(null=True, blank=True)
    confirmed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["task", "resource_type", "resource_key"],
                name="unique_cleanup_resource_per_task",
            ),
        ]
