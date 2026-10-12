import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("zane_api", "0350_remove_workspace_api_token_revoked_at"),
    ]

    operations = [
        migrations.CreateModel(
            name="WorkspaceRetirement",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "workspace_original_id",
                    models.CharField(max_length=255),
                ),
                (
                    "generation",
                    models.UUIDField(
                        db_index=True, default=uuid.uuid4, unique=True
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("RETIRING", "Retiring"),
                            ("COMPLETED", "Completed"),
                        ],
                        db_index=True,
                        default="RETIRING",
                        max_length=20,
                    ),
                ),
                ("snapshot", models.JSONField(default=dict)),
                ("result", models.JSONField(blank=True, null=True)),
                ("workflow_id", models.CharField(max_length=255, unique=True)),
                (
                    "delivery_status",
                    models.CharField(
                        choices=[
                            ("PENDING", "Pending"),
                            ("STARTED", "Started"),
                            ("CONFIRMED", "Confirmed"),
                            ("FAILED", "Failed"),
                        ],
                        default="PENDING",
                        max_length=20,
                    ),
                ),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finalized_at", models.DateTimeField(blank=True, null=True)),
                (
                    "workspace",
                    models.OneToOneField(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="retirement",
                        to="zane_api.workspace",
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.CreateModel(
            name="WorkspaceCleanupTask",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "unit_type",
                    models.CharField(
                        choices=[
                            ("SERVICE", "Service"),
                            ("COMPOSE_STACK", "Compose stack"),
                            ("ENVIRONMENT", "Environment"),
                            ("PROJECT", "Project network"),
                        ],
                        max_length=20,
                    ),
                ),
                ("unit_key", models.CharField(max_length=255)),
                ("workflow_id", models.CharField(max_length=255)),
                ("payload", models.JSONField(default=dict)),
                (
                    "delivery_status",
                    models.CharField(
                        choices=[
                            ("PENDING", "Pending"),
                            ("STARTED", "Started"),
                            ("CONFIRMED", "Confirmed"),
                            ("FAILED", "Failed"),
                        ],
                        db_index=True,
                        default="PENDING",
                        max_length=20,
                    ),
                ),
                ("attempts", models.PositiveIntegerField(default=0)),
                ("last_error", models.TextField(blank=True, null=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("confirmed_at", models.DateTimeField(blank=True, null=True)),
                (
                    "retirement",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="tasks",
                        to="zane_api.workspaceretirement",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="WorkspaceCleanupResource",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "resource_type",
                    models.CharField(
                        choices=[
                            ("CONTAINER", "Container / swarm service"),
                            ("VOLUME", "Docker volume"),
                            ("CONFIG", "Docker config"),
                            ("URL", "HTTP route / URL"),
                            ("HEALTHCHECK", "Healthcheck / metrics schedule"),
                            ("BUILDKIT_BUILDER", "Buildkit builder"),
                            ("PREVIEW_ENVIRONMENT", "Preview environment"),
                            ("NETWORK", "Docker network"),
                            ("STACK", "Compose stack"),
                        ],
                        max_length=20,
                    ),
                ),
                ("resource_key", models.CharField(max_length=1000)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("PENDING", "Pending"),
                            ("CONFIRMED", "Confirmed"),
                            ("FAILED", "Failed"),
                        ],
                        db_index=True,
                        default="PENDING",
                        max_length=20,
                    ),
                ),
                ("result", models.JSONField(blank=True, null=True)),
                ("confirmed_at", models.DateTimeField(blank=True, null=True)),
                (
                    "task",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="resources",
                        to="zane_api.workspacecleanuptask",
                    ),
                ),
            ],
        ),
        migrations.AddIndex(
            model_name="workspacecleanuptask",
            index=models.Index(
                fields=["delivery_status"], name="zane_api_cl_deliver_60e8f9_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="workspacecleanuptask",
            index=models.Index(fields=["workflow_id"], name="zane_api_cl_workflo_3a2b1c_idx"),
        ),
        migrations.AddConstraint(
            model_name="workspacecleanuptask",
            constraint=models.UniqueConstraint(
                fields=("retirement", "unit_type", "unit_key"),
                name="unique_cleanup_unit_per_retirement",
            ),
        ),
        migrations.AddConstraint(
            model_name="workspacecleanupresource",
            constraint=models.UniqueConstraint(
                fields=("task", "resource_type", "resource_key"),
                name="unique_cleanup_resource_per_task",
            ),
        ),
    ]
