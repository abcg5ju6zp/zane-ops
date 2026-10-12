# Generated for recoverable, generation-based build registry commits.

import django.db.models.deletion
from django.db import migrations, models
from shortuuid.django_fields import ShortUUIDField


def create_initial_generations(apps, schema_editor):
    """
    Backfill one ACTIVE generation per existing registry so that retries and
    future updates always have an immutable active snapshot to reconcile
    against.
    """
    BuildRegistry = apps.get_model("container_registry", "BuildRegistry")
    BuildRegistryGeneration = apps.get_model(
        "container_registry", "BuildRegistryGeneration"
    )
    for registry in BuildRegistry.objects.all():
        BuildRegistryGeneration.objects.create(
            registry=registry,
            version=registry.version,
            status="ACTIVE",
            snapshot={
                "name": registry.name,
                "registry_domain": registry.registry_domain,
                "registry_username": registry.registry_username,
                "registry_password": registry.registry_password,
                "is_secure": registry.is_secure,
                "storage_backend": registry.storage_backend,
                "s3_credentials": registry.s3_credentials,
                "service_alias": registry.service_alias,
                "swarm_service_name": registry.swarm_service_name,
                "monitor_schedule_id": f"monitor-{registry.id}",
                "version": registry.version,
            },
        )


def reverse_initial_generations(apps, schema_editor):
    BuildRegistryGeneration = apps.get_model(
        "container_registry", "BuildRegistryGeneration"
    )
    BuildRegistryGeneration.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ("container_registry", "0044_remove_buildregistry_workspace"),
    ]

    operations = [
        migrations.CreateModel(
            name="BuildRegistryGeneration",
            fields=[
                (
                    "id",
                    ShortUUIDField(
                        length=20,
                        max_length=255,
                        prefix="build_reg_gen_",
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("version", models.PositiveIntegerField()),
                ("snapshot", models.JSONField()),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("STAGING", "Staging"),
                            ("ACTIVE", "Active"),
                            ("SUPERSEDED", "Superseded"),
                        ],
                        default="STAGING",
                        max_length=20,
                    ),
                ),
                ("staged_at", models.DateTimeField(null=True)),
                ("swarm_confirmed_at", models.DateTimeField(null=True)),
                ("proxy_confirmed_at", models.DateTimeField(null=True)),
                ("schedule_confirmed_at", models.DateTimeField(null=True)),
                ("published_at", models.DateTimeField(null=True)),
                ("cleaned_at", models.DateTimeField(null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "registry",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="generations",
                        to="container_registry.buildregistry",
                    ),
                ),
            ],
            options={
                "ordering": ["version"],
            },
        ),
        migrations.AddConstraint(
            model_name="buildregistrygeneration",
            constraint=models.UniqueConstraint(
                fields=("registry", "version"),
                name="unique_registry_generation_version",
            ),
        ),
        migrations.AddIndex(
            model_name="buildregistrygeneration",
            index=models.Index(
                fields=["registry", "status"],
                name="container_r_gen_status_idx",
            ),
        ),
        migrations.RunPython(
            create_initial_generations,
            reverse_initial_generations,
        ),
    ]
