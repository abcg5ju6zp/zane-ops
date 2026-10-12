from typing import cast
from rest_framework import serializers

from ..models import BuildRegistry, BuildRegistryGeneration
from temporal.workflows import DeployBuildRegistryWorkflow, UpdateBuildRegistryWorkflow
from temporal.client import TemporalClient
from temporal.shared import (
    BuildRegistryGenerationPayload,
    registry_snapshot_from_generation,
)
from django.db import transaction
import secrets
from django.db.models import Q, F, Value
from zane_api.validators import validate_url_domain
from rest_framework import pagination
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.client import Config
from zane_api.models import URL, DeploymentChange
from django.conf import settings


class BuildRegistryListPagination(pagination.PageNumberPagination):
    page_size = 10
    page_size_query_param = "per_page"
    page_query_param = "page"


class S3CredentialsSerializer(serializers.Serializer):
    region = serializers.CharField(
        max_length=50,
        default="us-east-1",
    )
    endpoint = serializers.URLField(required=False)
    secure = serializers.BooleanField(default=True)

    bucket = serializers.CharField(required=True)
    access_key = serializers.CharField(required=True)
    secret_key = serializers.CharField(write_only=True, required=False)

    def validate(self, attrs):
        parent_instance: BuildRegistry | None = self.context.get("parent_instance")

        # Get values with proper fallback to defaults or existing instance values
        if parent_instance and parent_instance.s3_credentials:
            # Fallback to existing credentials if not provided in update
            existing = parent_instance.s3_credentials
            region = attrs.get("region", existing.get("region", "us-east-1"))
            secure = attrs.get("secure", existing.get("secure", True))
            endpoint = attrs.get("endpoint", existing.get("endpoint"))
            bucket = attrs.get("bucket", existing.get("bucket"))
            access_key = attrs.get("access_key", existing.get("access_key"))
            secret_key = attrs.get("secret_key", existing.get("secret_key"))
        else:
            # Use field defaults for new credentials
            region = attrs.get("region", "us-east-1")
            secure = attrs.get("secure", True)
            endpoint = attrs.get("endpoint")
            bucket = attrs.get("bucket")
            access_key = attrs.get("access_key")
            secret_key = attrs.get("secret_key")

        # Validate required fields
        errors = {}
        if not bucket:
            errors["bucket"] = "This field is required"
        if not access_key:
            errors["access_key"] = "This field is required"
        if not secret_key:
            errors["secret_key"] = "This field is required"

        if errors:
            raise serializers.ValidationError(errors)

        # Update attrs with resolved values
        attrs["region"] = region
        attrs["secure"] = secure
        if endpoint is not None:
            attrs["endpoint"] = endpoint

        # Build boto3 config
        config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
        }

        if endpoint is not None:
            config["endpoint_url"] = endpoint
            if not secure:
                config["use_ssl"] = False

        # Test S3 connection
        try:
            s3 = boto3.client("s3", config=Config(signature_version="s3v4"), **config)
            s3.head_bucket(Bucket=bucket)
        except ClientError as e:
            error_code = str(e.response.get("Error", {}).get("Code", "Unknown"))
            errors = {}

            # Provide specific error messages based on error type
            if error_code == "404" or error_code == "NoSuchBucket":
                errors["bucket"] = (
                    f"Bucket '{bucket}' does not exist or is not accessible. "
                    f"Error details: {str(e)}"
                )
            elif error_code == "403" or error_code == "AccessDenied":
                errors["access_key"] = errors["secret_key"] = (
                    f"Access denied. Please verify your access key and secret key have "
                    f"the necessary permissions to access this bucket. "
                    f"Error details: {str(e)}"
                )
            elif error_code == "InvalidAccessKeyId":
                errors["access_key"] = (
                    f"Invalid access key ID. Please check that your access key is correct. "
                    f"Error details: {str(e)}"
                )
            elif error_code == "SignatureDoesNotMatch":
                errors["secret_key"] = (
                    f"Invalid secret key. The signature does not match. "
                    f"Please verify your secret key is correct. "
                    f"Error details: {str(e)}"
                )
            else:
                errors["bucket"] = errors["access_key"] = errors["secret_key"] = (
                    f"Unable to connect to S3 bucket. Please verify all credentials are correct. "
                    f"Error details: {str(e)}"
                )

            raise serializers.ValidationError(errors)
        except EndpointConnectionError:
            raise serializers.ValidationError(
                {
                    "endpoint": f"Cannot connect to endpoint '{endpoint}'. Please verify the endpoint URL is correct."
                }
            )
        except Exception as e:
            # Catch any other unexpected errors
            raise serializers.ValidationError(f"S3 validation failed: {str(e)}")

        attrs["region"] = region
        attrs["secure"] = secure
        attrs["endpoint"] = endpoint
        attrs["bucket"] = bucket
        attrs["access_key"] = access_key
        attrs["secret_key"] = secret_key
        return attrs


class BuildRegistryListCreateSerializer(serializers.ModelSerializer):
    is_default = serializers.BooleanField(required=True)

    registry_domain = serializers.CharField(validators=[validate_url_domain])
    registry_username = serializers.CharField(default="zane")
    registry_password = serializers.CharField(
        write_only=True,
        required=False,
        allow_blank=True,
    )

    s3_credentials = S3CredentialsSerializer(required=False, allow_null=True)

    def validate_registry_domain(self, domain: str):
        domain = domain.lower()

        if domain.startswith("*"):
            raise serializers.ValidationError(
                "Registry domain cannot use wildcards. Please specify an exact domain"
            )

        if domain == settings.ZANE_APP_DOMAIN:
            raise serializers.ValidationError(
                "Using the domain where ZaneOps is installed is not allowed."
            )

        assigned_to_service = URL.objects.filter(
            Q(domain=domain) | Q(domain=f"*.{domain}")
        ).exists()
        assigning_to_service = DeploymentChange.objects.filter(
            Q(
                field=DeploymentChange.ChangeField.URLS,
                applied=False,
            )
            & (Q(new_value__domain=domain) | Q(new_value__domain=f"*.{domain}")),
        ).exists()

        if assigned_to_service or assigning_to_service:
            raise serializers.ValidationError(
                "Cannot use this domain as it is already assigned to a service."
            )

        return domain

    def validate_is_default(self, is_default: bool):
        if (
            not is_default
            and not BuildRegistry.objects.filter(is_default=True).exists()
        ):
            raise serializers.ValidationError(
                "At least one build registry must be set as the default registry."
            )
        return is_default

    def validate(self, attrs: dict):
        password = attrs.get("registry_password")
        storage_backend = attrs.get("storage_backend")

        if not password:
            attrs["registry_password"] = secrets.token_hex()

        if storage_backend == BuildRegistry.StorageBackend.LOCAL:
            # remove s3 credentials
            attrs.pop("s3_credentials", None)

        if (
            storage_backend == BuildRegistry.StorageBackend.S3
            and attrs.get("s3_credentials") is None
        ):
            errors = {
                "bucket": "This field is required",
                "access_key": "This field is required",
                "secret_key": "This field is required",
            }

            raise serializers.ValidationError({"s3_credentials": errors})

        return attrs

    @transaction.atomic()
    def create(self, validated_data: dict):
        is_default = validated_data.get("is_default")

        if is_default:
            BuildRegistry.objects.update(is_default=False)

        registry = BuildRegistry.objects.create(**validated_data)

        registry.service_alias = BuildRegistry.generate_default_service_alias(registry)
        registry.swarm_service_name = BuildRegistry.generate_default_swarm_service_name(
            registry
        )
        registry.save()

        # The first immutable, queryable generation; it is published as active
        # only once the workflow confirms Swarm, proxy and health scheduling.
        generation = BuildRegistryGeneration.objects.create(
            registry=registry,
            version=registry.version,
            snapshot=BuildRegistryGeneration.snapshot_from_registry(registry),
        )

        def commit_callback():
            payload = BuildRegistryGenerationPayload(
                generation_id=generation.id,
                target=registry_snapshot_from_generation(generation),
            )

            TemporalClient.start_workflow(
                workflow=DeployBuildRegistryWorkflow.run,
                arg=payload,
                id=generation.workflow_id,
            )

        transaction.on_commit(commit_callback)

        return registry

    class Meta:
        model = BuildRegistry
        fields = [
            "id",
            "name",
            "is_default",
            "service_alias",
            "is_secure",
            "version",
            # credentials
            "registry_domain",
            "registry_username",
            "registry_password",
            # S3 credentials
            "s3_credentials",
            "storage_backend",
            "health_status",
            "healthcheck_message",
        ]
        extra_kwargs = {
            "id": {"read_only": True},
            "version": {"read_only": True},
            "service_alias": {"read_only": True},
            "health_status": {"read_only": True},
            "healthcheck_message": {"read_only": True},
        }


class BuildRegistryUpdateDetailsSerializer(serializers.ModelSerializer):
    is_default = serializers.BooleanField(required=True)

    s3_credentials = S3CredentialsSerializer(required=False, allow_null=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Pass instance to nested serializer via context
        if self.instance and "s3_credentials" in self.fields:
            self.fields["s3_credentials"].context["parent_instance"] = self.instance

    def validate_registry_domain(self, domain: str):
        if domain.startswith("*"):
            raise serializers.ValidationError(
                "Registry domain cannot use wildcards. Please specify an exact domain"
            )

        assigned_to_service = URL.objects.filter(
            Q(domain=domain) | Q(domain=f"*.{domain}")
        ).exists()
        assigning_to_service = DeploymentChange.objects.filter(
            Q(
                field=DeploymentChange.ChangeField.URLS,
                applied=False,
            )
            & (Q(new_value__domain=domain) | Q(new_value__domain=f"*.{domain}")),
        ).exists()

        if assigned_to_service or assigning_to_service:
            raise serializers.ValidationError(
                "Cannot use this domain as it is already assigned to a service."
            )

        return domain

    def validate_is_default(self, is_default: bool):
        self.instance = cast(BuildRegistry, self.instance)
        if (
            not is_default
            and not BuildRegistry.objects.filter(
                Q(is_default=True) & ~Q(pk=self.instance.id)
            ).exists()
        ):
            raise serializers.ValidationError(
                "At least one build registry must be set as the default registry."
            )
        return is_default

    def validate(self, attrs: dict):
        storage_backend = attrs.get("storage_backend", self.instance.storage_backend)
        s3_credentials = attrs.get("s3_credentials", self.instance.s3_credentials)

        if storage_backend == BuildRegistry.StorageBackend.LOCAL:
            # remove s3 credentials
            attrs.pop("s3_credentials", None)
        if (
            storage_backend == BuildRegistry.StorageBackend.S3
            and s3_credentials is None
        ):
            errors = {
                "bucket": "This field is required",
                "access_key": "This field is required",
                "secret_key": "This field is required",
            }

            raise serializers.ValidationError({"s3_credentials": errors})

        return attrs

    @transaction.atomic()
    def update(self, instance: BuildRegistry, validated_data: dict):
        registry_domain = validated_data.get(
            "registry_domain", instance.registry_domain
        )
        registry_username = validated_data.get(
            "registry_username", instance.registry_username
        )
        registry_password = validated_data.get(
            "registry_password", instance.registry_password
        )
        storage_backend = validated_data.get(
            "storage_backend", instance.storage_backend
        )
        s3_credentials = validated_data.get("s3_credentials", instance.s3_credentials)

        name = validated_data.get("name", instance.name)
        is_default = validated_data.get("is_default", instance.is_default)
        is_secure = validated_data.get("is_secure", instance.is_secure)

        if is_default:
            BuildRegistry.objects.exclude(id=instance.id).update(is_default=False)

        overrides = {
            "name": name,
            "registry_domain": registry_domain,
            "registry_username": registry_username,
            "registry_password": registry_password,
            "is_secure": is_secure,
            "storage_backend": storage_backend,
            "s3_credentials": s3_credentials,
        }
        target_snapshot = BuildRegistryGeneration.snapshot_from_registry(
            instance,
            version=instance.version + 1,
            overrides=overrides,
        )
        latest_generation = (
            instance.generations.order_by("-version").first()
            if instance.pk is not None
            else None
        )
        is_retry = (
            latest_generation is not None
            and latest_generation.status
            == BuildRegistryGeneration.GenerationStatus.STAGING
            and all(
                latest_generation.snapshot.get(field) == target_snapshot[field]
                for field in BuildRegistryGeneration.DEPLOYMENT_FIELDS
            )
        )

        if is_retry:
            # The previous request only partially applied: continue the same
            # generation, never increment the version a second time and never
            # prepare a parallel set of staging resources.
            generation = cast(BuildRegistryGeneration, latest_generation)
            BuildRegistry.objects.filter(pk=instance.id).update(
                name=name,
                is_default=is_default,
            )
        else:
            # New distinct change (or an update while no change is in flight):
            # bump the version and persist the immutable target snapshot of the
            # new generation before external resources are touched.
            BuildRegistry.objects.filter(pk=instance.id).update(
                version=F("version") + Value(1),
                registry_domain=registry_domain,
                name=name,
                is_default=is_default,
                is_secure=is_secure,
                registry_username=registry_username,
                registry_password=registry_password,
                storage_backend=storage_backend,
                s3_credentials=s3_credentials,
            )
            generation = BuildRegistryGeneration.objects.create(
                registry=instance,
                version=instance.version + 1,
                snapshot=target_snapshot,
            )

        def commit_callback():
            payload = BuildRegistryGenerationPayload(
                generation_id=generation.id,
                target=registry_snapshot_from_generation(generation),
            )

            TemporalClient.start_workflow(
                workflow=UpdateBuildRegistryWorkflow.run,
                arg=payload,
                id=generation.workflow_id,
            )

        transaction.on_commit(commit_callback)
        instance.refresh_from_db()

        return instance

    class Meta:
        model = BuildRegistry
        fields = [
            "id",
            "name",
            "is_default",
            "registry_domain",
            "service_alias",
            "registry_username",
            "registry_password",
            "version",
            "is_secure",
            "storage_backend",
            # S3 credentials
            "s3_credentials",
            "health_status",
            "healthcheck_message",
        ]
        extra_kwargs = {
            "id": {"read_only": True},
            "version": {"read_only": True},
            "service_alias": {"read_only": True},
            "registry_password": {"write_only": True},
            "health_status": {"read_only": True},
            "healthcheck_message": {"read_only": True},
        }


class BuildRegistryQuerySerializer(serializers.Serializer):
    cursor = serializers.CharField(required=False)
    per_page = serializers.IntegerField(default=25)


class BuildRegistryResponseSerializer(serializers.Serializer):
    cursor = serializers.CharField(allow_null=True)
    per_page = serializers.IntegerField(allow_null=True)
    results = serializers.ListField(child=serializers.CharField())
