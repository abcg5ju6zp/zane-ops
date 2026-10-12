from typing import cast

import requests
from asgiref.sync import sync_to_async
from unittest.mock import patch
from zane_api.tests.base import AuthAPITestCase, FakeDockerClient, FakeS3Client

import responses
from django.urls import reverse
from zane_api.utils import jprint, find_item_in_sequence
from rest_framework import status
from ..models import BuildRegistry, BuildRegistryGeneration
from zane_api.models import Deployment, DeploymentChange

from django.conf import settings
from temporal.proxy import ZaneProxyClient

from temporal.activities.registries import (
    get_config_name_for_registry,
    reconcile_build_registry_swarm_service,
    cleanup_superseded_registry_configs,
    _publish_registry_generation_sync,
)
from temporal.schedules.activities import MonitorRegistryDeploymentActivites
from temporal.shared import (
    RegistryHealthCheckResult,
    SwarmRegistryServiceDetails,
    registry_snapshot_from_generation,
)


class BuildRegistryViewTests(AuthAPITestCase):
    def test_update_default_registry_to_prevent_duplicates(self):
        self.loginUser()

        BuildRegistry.objects.create(
            name="My registry",
            registry_domain="registry.example.com",
            registry_username="hello",
            registry_password="world",
        )

        body = {
            "name": "My New registry",
            "is_default": True,
            "registry_domain": "registry.example.com",
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry_one = BuildRegistry.objects.get(name="My registry")
        registry_two = BuildRegistry.objects.get(name="My New registry")
        self.assertFalse(registry_one.is_default)
        self.assertTrue(registry_two.is_default)

    def test_create_default_registry_at_least_one_default_is_required(self):
        self.loginUser()

        body = {
            "name": "My registry",
            "is_default": False,
            "registry_domain": "registry.example.com",
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertIsNotNone(self.get_error_from_response(response, field="is_default"))

    @responses.activate()
    async def test_create_and_deploy_build_registry(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)
        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
            "registry_username": "fredkisss",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(
            BuildRegistry,
            await BuildRegistry.objects.afirst(),
        )
        self.assertIsNotNone(registry)

        # check that it has created credentials
        self.assertEqual("registry.127.0.0.0.1.sslip.io", registry.registry_domain)
        self.assertEqual("fredkisss", registry.registry_username)
        self.assertGreater(len(registry.registry_password), 0)

        swarm_service = cast(
            FakeDockerClient.FakeService,
            self.fake_docker_client.service_map.get(
                cast(str, registry.swarm_service_name)
            ),
        )
        self.assertIsNotNone(swarm_service)
        self.assertGreater(len(swarm_service.attached_volumes), 0)
        self.assertGreater(len(swarm_service.configs), 0)

        # check that it has been added to caddy
        response = requests.get(
            ZaneProxyClient.get_uri_for_build_registry(
                cast(str, registry.service_alias)
            )
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

    def test_create_new_registry_with_default_unset_the_current_default_registry(self):
        self.loginUser()

        old_registry = BuildRegistry.objects.create(
            name="default",
            registry_domain="registry.example.com",
            registry_username="zane",
            registry_password="password",
        )

        body = {
            "name": "New default",
            "is_default": True,
            "registry_domain": "registry.example.com",
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        new_registry = cast(
            BuildRegistry, BuildRegistry.objects.filter(name="New default").first()
        )
        self.assertIsNotNone(new_registry)

        old_registry.refresh_from_db()
        self.assertTrue(new_registry.is_default)
        self.assertFalse(old_registry.is_default)

    def test_delete_registry_cannot_delete_default_registry(self):
        self.loginUser()

        registry = BuildRegistry.objects.create(
            name="default",
            registry_domain="registry.example.com",
            registry_username="zane",
            registry_password="password",
        )

        # Delete registry
        response = self.client.delete(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
        )
        jprint(response.json())
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

    def test_update_service_cannot_reuse_registry_url(self):
        self.loginUser()
        BuildRegistry.objects.create(
            name="default", registry_domain="registry.example.com"
        )

        project, service = self.create_and_deploy_caddy_docker_service()
        body = {
            "field": DeploymentChange.ChangeField.URLS,
            "type": "ADD",
            "new_value": {
                "domain": "registry.example.com",
                "associated_port": 80,
            },
        }
        response = self.client.put(
            reverse(
                "zane_api:services.request_deployment_changes",
                kwargs=dict(
                    project_slug=project.slug,
                    env_slug="production",
                    service_slug=service.slug,
                ),
            ),
            data=body,
        )
        jprint(response.json())
        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)

    def test_create_registry_cannot_reuse_service_url(self):
        self.loginUser()
        self.create_and_deploy_caddy_docker_service(domain="registry.example.com")

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.example.com",
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertIsNotNone(self.get_error_from_response(response, "registry_domain"))

    def test_create_registry_cannot_use_wildcard_url(self):
        self.loginUser()

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "*.registry.example.com",
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertIsNotNone(self.get_error_from_response(response, "registry_domain"))

    def test_create_registry_cannot_reuse_zane_app_domain(self):
        self.loginUser()

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": settings.ZANE_APP_DOMAIN,
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertIsNotNone(self.get_error_from_response(response, "registry_domain"))

    @responses.activate()
    async def test_delete_build_registry_and_associated_service(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)

        registry = cast(
            BuildRegistry,
            await BuildRegistry.objects.afirst(),
        )
        self.assertIsNotNone(registry)

        # remove default status to prevent conflict error
        registry.is_default = False
        await registry.asave()

        # Delete registry
        response = await self.async_client.delete(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
        )
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)
        self.assertIsNone(await BuildRegistry.objects.afirst())

        swarm_service = cast(
            FakeDockerClient.FakeService,
            self.fake_docker_client.service_map.get(
                cast(str, registry.swarm_service_name)
            ),
        )
        self.assertIsNone(swarm_service)

        # check that it has been added to caddy
        response = requests.get(
            ZaneProxyClient.get_uri_for_build_registry(
                cast(str, registry.service_alias),
            )
        )
        self.assertEqual(status.HTTP_404_NOT_FOUND, response.status_code)

    @responses.activate()
    async def test_build_git_service_push_to_default_registry(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        fake_credentials = self.fake_docker_client.PRIVATE_IMAGE_CREDENTIALS
        registry = await BuildRegistry.objects.acreate(
            name="default",
            registry_domain="registry.example.com",
            registry_username=fake_credentials["username"],
            registry_password=fake_credentials["password"],
        )

        p, service = await self.acreate_git_service(
            repository_url="https://gitlab.com/fredkiss3/private-ac",
        )

        response = await self.async_client.put(
            reverse(
                "zane_api:services.git.deploy_service",
                kwargs={
                    "project_slug": p.slug,
                    "env_slug": "production",
                    "service_slug": service.slug,
                },
            ),
        )
        jprint(response.json())
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        first_deployment = cast(Deployment, await service.deployments.afirst())
        self.assertIsNotNone(first_deployment)
        self.assertEqual(Deployment.DeploymentStatus.HEALTHY, first_deployment.status)
        image_name = registry.registry_domain + "/" + await first_deployment.aimage_tag

        # image pushed to registry
        self.assertTrue(image_name in self.fake_docker_client.image_registry)

    def test_update_registry_set_default_override_all_default(self):
        self.loginUser()

        first_registry, second_registry = BuildRegistry.objects.bulk_create(
            [
                BuildRegistry(
                    name="My registry",
                    registry_domain="registry.127.0.0.0.1.sslip.io",
                    registry_username="hello",
                    registry_password="world",
                ),
                BuildRegistry(
                    name="My registry 2",
                    is_default=False,
                    registry_domain="registry.127.0.0.0.1.sslip.io",
                    registry_username="hello",
                    registry_password="world",
                ),
            ]
        )

        # Update registry
        body = {"is_default": True}
        response = self.client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs=dict(id=second_registry.id),
            ),
            data=body,
        )
        jprint(response.json())
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        second_registry.refresh_from_db()
        first_registry.refresh_from_db()

        # version should be incremented on each update
        self.assertTrue(second_registry.is_default)
        self.assertFalse(first_registry.is_default)

    @responses.activate()
    async def test_update_and_redeploy_registry_sucessfully(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)

        old_registry = cast(
            BuildRegistry,
            await BuildRegistry.objects.afirst(),
        )
        self.assertIsNotNone(old_registry)

        swarm_service = cast(
            FakeDockerClient.FakeService,
            self.fake_docker_client.service_map.get(
                cast(str, old_registry.swarm_service_name)
            ),
        )

        # Update registry
        body = {
            "name": "My registry 2",
            "registry_username": "batman",
            "registry_password": "supers3cr4tpassw4rd",
            "registry_domain": "registry.example.com",
        }
        response = await self.async_client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs=dict(id=old_registry.id),
            ),
            data=body,
        )
        jprint(response.json())
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        new_registry = await BuildRegistry.objects.aget(pk=old_registry.id)

        # version should be incremented on each update
        self.assertGreater(new_registry.version, old_registry.version)
        self.assertEqual(new_registry.name, "My registry 2")
        self.assertEqual(new_registry.registry_username, "batman")
        self.assertEqual(new_registry.registry_domain, "registry.example.com")
        self.assertEqual(new_registry.registry_password, "supers3cr4tpassw4rd")

        # old configs should be deleted while new ones are created
        new_credentials_config = cast(
            FakeDockerClient.FakeConfig,
            self.fake_docker_client.config_map.get(
                get_config_name_for_registry(new_registry, "credentials")  # type: ignore
            ),
        )
        new_config_file = cast(
            FakeDockerClient.FakeConfig,
            self.fake_docker_client.config_map.get(
                get_config_name_for_registry(new_registry, "config")  # type: ignore
            ),
        )
        self.assertIsNotNone(new_credentials_config)
        self.assertIsNotNone(new_config_file)

        old_credentials_config = self.fake_docker_client.config_map.get(
            get_config_name_for_registry(old_registry, "credentials")  # type: ignore
        )
        old_config_file = self.fake_docker_client.config_map.get(
            get_config_name_for_registry(old_registry, "config")  # type: ignore
        )
        self.assertIsNone(old_credentials_config)
        self.assertIsNone(old_config_file)

        # check that the service config data has updated
        swarm_service = cast(
            FakeDockerClient.FakeService,
            self.fake_docker_client.service_map.get(
                cast(str, old_registry.swarm_service_name)
            ),
        )

        self.assertIsNotNone(swarm_service)
        self.assertIsNotNone(
            find_item_in_sequence(
                lambda c: c["ConfigID"] == new_config_file.id, swarm_service.configs
            )
        )
        self.assertIsNotNone(
            find_item_in_sequence(
                lambda c: c["ConfigID"] == new_credentials_config.id,
                swarm_service.configs,
            )
        )

        # the domain should also be udpated in caddy
        response = requests.get(
            ZaneProxyClient.get_uri_for_build_registry(
                cast(str, old_registry.service_alias),
            )
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        data = response.json()
        jprint(response.json())
        self.assertEqual(new_registry.registry_domain, data["match"][0]["host"][0])

    @responses.activate()
    def test_create_registry_with_local_storage_do_not_persist_s3_credentials(
        self,
    ):
        self.loginUser()
        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
            "registry_username": "fredkisss",
            "storage_backend": BuildRegistry.StorageBackend.LOCAL,
            "s3_credentials": {
                "bucket": "registry-storage",
                "region": "eu-west-1",
                "access_key": "id_key",
                "secret_key": "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
                "endpoint": "https://s3.zaneops.dev",
            },
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(
            BuildRegistry,
            BuildRegistry.objects.first(),
        )
        self.assertIsNotNone(registry)

        self.assertIsNone(registry.s3_credentials)

    @responses.activate()
    def test_create_registry_with_s3_storage_require_s3_credentials(
        self,
    ):
        self.loginUser()
        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
            "registry_username": "fredkisss",
            "storage_backend": BuildRegistry.StorageBackend.S3,
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertIsNotNone(
            self.get_error_from_response(response, "s3_credentials.bucket")
        )
        self.assertIsNotNone(
            self.get_error_from_response(response, "s3_credentials.access_key")
        )
        self.assertIsNotNone(
            self.get_error_from_response(response, "s3_credentials.secret_key")
        )

    @responses.activate()
    def test_create_registry_with_non_existent_s3_bucket(self):
        self.loginUser()
        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
            "registry_username": "fredkisss",
            "storage_backend": BuildRegistry.StorageBackend.S3,
            "s3_credentials": {
                "bucket": FakeS3Client.NON_EXISTENT_BUCKET,
                "region": "eu-west-1",
                "access_key": "id_key",
                "secret_key": "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
                "endpoint": "https://s3.example.com",
            },
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertIsNotNone(
            self.get_error_from_response(response, "s3_credentials.bucket")
        )

    @responses.activate()
    def test_create_registry_with_valid_s3_credentials(self):
        self.loginUser()
        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
            "storage_backend": BuildRegistry.StorageBackend.S3,
            "registry_username": "fredkisss",
            "s3_credentials": {
                "bucket": "registry-backup",
                "region": "eu-west-1",
                "access_key": "id_key",
                "secret_key": "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
                "endpoint": "https://s3.example.com",
            },
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(
            BuildRegistry,
            BuildRegistry.objects.first(),
        )
        self.assertIsNotNone(registry)

        # check that it has created credentials
        self.assertEqual(BuildRegistry.StorageBackend.S3, registry.storage_backend)

        credentials = cast(dict, registry.s3_credentials)
        self.assertEqual("https://s3.example.com", credentials.get("endpoint"))
        self.assertEqual("registry-backup", credentials.get("bucket"))
        self.assertEqual("id_key", credentials.get("access_key"))
        self.assertEqual(
            "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
            credentials.get("secret_key"),
        )
        self.assertEqual(
            "eu-west-1",
            credentials.get("region"),
        )

    @responses.activate()
    def test_update_registry_with_invalid_s3_credentials(self):
        self.loginUser()

        registry = BuildRegistry.objects.create(
            name="My registry",
            registry_domain="registry.127.0.0.0.1.sslip.io",
            storage_backend=BuildRegistry.StorageBackend.LOCAL,
        )

        body = {
            "storage_backend": BuildRegistry.StorageBackend.S3,
            "s3_credentials": {
                "region": "eu-west-1",
                "bucket": "registry-backup",
                "access_key": "id_key",
                "secret_key": FakeS3Client.INVALID_SECRET_KEY,
                "endpoint": "https://s3.example.com",
            },
        }
        response = self.client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data=body,
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertIsNotNone(
            self.get_error_from_response(response, "s3_credentials.access_key")
        )
        self.assertIsNotNone(
            self.get_error_from_response(response, "s3_credentials.secret_key")
        )

    @responses.activate()
    def test_update_registry_with_valid_s3_credentials(self):
        self.loginUser()

        registry = BuildRegistry.objects.create(
            name="My registry",
            registry_domain="registry.127.0.0.0.1.sslip.io",
            storage_backend=BuildRegistry.StorageBackend.LOCAL,
        )

        body = {
            "storage_backend": BuildRegistry.StorageBackend.S3,
            "s3_credentials": {
                "region": "eu-west-1",
                "bucket": "registry-backup",
                "access_key": "id_key",
                "secret_key": "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
                "endpoint": "https://s3.example.com",
            },
        }
        response = self.client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data=body,
        )

        jprint(response.json())
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        registry.refresh_from_db()

        # check that it has updated credentials
        credentials = cast(dict, registry.s3_credentials)
        self.assertEqual("https://s3.example.com", credentials.get("endpoint"))
        self.assertEqual("registry-backup", credentials.get("bucket"))
        self.assertEqual("id_key", credentials.get("access_key"))
        self.assertEqual(
            "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
            credentials.get("secret_key"),
        )
        self.assertEqual(
            "eu-west-1",
            credentials.get("region"),
        )

    @responses.activate()
    async def test_create_and_deploy_registry_with_s3_credentials(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)
        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
            "storage_backend": BuildRegistry.StorageBackend.S3,
            "s3_credentials": {
                "bucket": "registry-backup",
                "region": "eu-west-1",
                "access_key": "id_key",
                "secret_key": "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
                "endpoint": "https://s3.example.com",
            },
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(
            BuildRegistry,
            await BuildRegistry.objects.afirst(),
        )
        self.assertIsNotNone(registry)

        swarm_service = cast(
            FakeDockerClient.FakeService,
            self.fake_docker_client.service_map.get(
                cast(str, registry.swarm_service_name)
            ),
        )
        self.assertIsNotNone(swarm_service)

        config_file = cast(
            FakeDockerClient.FakeConfig,
            self.fake_docker_client.config_map.get(
                get_config_name_for_registry(registry, "config")  # type: ignore
            ),
        )
        self.assertIsNotNone(config_file)

        print("==[config_file.data]==")
        print(config_file.data)
        print("==[end config_file.data]==")
        self.assertTrue("s3:" in config_file.data)

    @responses.activate()
    async def test_update_and_redeploy_registry_with_s3_credentials(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)

        registry = cast(
            BuildRegistry,
            await BuildRegistry.objects.afirst(),
        )
        self.assertIsNotNone(registry)

        body = {
            "storage_backend": BuildRegistry.StorageBackend.S3,
            "s3_credentials": {
                "region": "eu-west-1",
                "bucket": "registry-backup",
                "access_key": "id_key",
                "secret_key": "52ff73725cb0bc2ad4d048f8d62ac49dd598116286969658e0e6677dbfe1f376",
                "endpoint": "https://s3.example.com",
            },
        }
        response = await self.async_client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data=body,
        )

        jprint(response.json())
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        await registry.arefresh_from_db()

        swarm_service = cast(
            FakeDockerClient.FakeService,
            self.fake_docker_client.service_map.get(
                cast(str, registry.swarm_service_name)
            ),
        )
        self.assertIsNotNone(swarm_service)

        config_file = cast(
            FakeDockerClient.FakeConfig,
            self.fake_docker_client.config_map.get(
                get_config_name_for_registry(registry, "config")  # type: ignore
            ),
        )
        self.assertIsNotNone(config_file)

        print("==[config_file.data]==")
        print(config_file.data)
        print("==[end config_file.data]==")
        self.assertTrue("s3:" in config_file.data)


    def test_retry_of_partial_update_reuses_generation_and_keeps_version(self):
        self.loginUser()

        registry = BuildRegistry.objects.create(
            name="default",
            registry_domain="registry.example.com",
            registry_username="zane",
            registry_password="password",
        )

        body = {
            "is_default": True,
            "name": "updated",
            "registry_domain": "registry-new.example.com",
            "registry_username": "batman",
            "registry_password": "supersecret",
        }
        # first request: the workflow never runs in this test, so the
        # generation remains in flight (STAGING), mimicking a partial failure.
        response = self.client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data=body,
        )
        jprint(response.json())
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        registry.refresh_from_db()
        self.assertEqual(2, registry.version)

        staging_generation = cast(
            BuildRegistryGeneration,
            registry.generations.order_by("-version").first(),
        )
        self.assertEqual(2, staging_generation.version)
        self.assertEqual(
            BuildRegistryGeneration.GenerationStatus.STAGING,
            staging_generation.status,
        )

        # retrying the exact same change continues the same generation instead
        # of incrementing the version again
        response = self.client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data=body,
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        registry.refresh_from_db()
        self.assertEqual(2, registry.version)
        self.assertEqual(1, registry.generations.count())
        self.assertEqual(
            "registry-new.example.com",
            staging_generation.snapshot["registry_domain"],
        )
        self.assertEqual(
            BuildRegistryGeneration.GenerationStatus.STAGING,
            registry.generations.get().status,
        )
        self.assertEqual(
            f"deploy-{registry.id}-v2",
            registry.generations.get().workflow_id,
        )

    @responses.activate()
    async def test_generations_preserve_immutable_snapshots_and_publish_gate(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )
        self.assertEqual(status.HTTP_201_CREATED, response.status_code)

        registry = cast(BuildRegistry, await BuildRegistry.objects.afirst())

        body = {
            "is_default": True,
            "name": "My registry 2",
            "registry_username": "batman",
            "registry_password": "supers3cr4tpassw4rd",
            "registry_domain": "registry.example.com",
        }
        response = await self.async_client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data=body,
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        generations = [
            gen
            async for gen in registry.generations.order_by("version")
        ]
        self.assertEqual(2, len(generations))
        first, second = generations

        # queryable, immutable snapshots of both the old active config and the
        # new target config
        self.assertEqual(1, first.version)
        self.assertEqual(
            BuildRegistryGeneration.GenerationStatus.SUPERSEDED, first.status
        )
        self.assertEqual(
            "registry.127.0.0.0.1.sslip.io",
            first.snapshot["registry_domain"],
        )

        self.assertEqual(2, second.version)
        self.assertEqual(
            BuildRegistryGeneration.GenerationStatus.ACTIVE, second.status
        )
        self.assertEqual(
            "registry.example.com", second.snapshot["registry_domain"]
        )
        self.assertEqual("batman", second.snapshot["registry_username"])

        # every external stage was persisted before publication
        self.assertIsNotNone(second.staged_at)
        self.assertIsNotNone(second.swarm_confirmed_at)
        self.assertIsNotNone(second.proxy_confirmed_at)
        self.assertIsNotNone(second.schedule_confirmed_at)
        self.assertIsNotNone(second.published_at)

        await registry.arefresh_from_db()
        self.assertEqual(2, registry.version)
        self.assertEqual(
            BuildRegistry.RegistryHealthStatus.HEALTHY, registry.health_status
        )

        # the health schedule carries the new generation arguments
        schedule_handle = self.get_workflow_schedule_by_id(
            registry.monitor_schedule_id
        )
        self.assertIsNotNone(schedule_handle)
        self.assertEqual(2, schedule_handle.args.version)

    @responses.activate()
    async def test_late_health_result_from_old_generation_is_dropped(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )
        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(BuildRegistry, await BuildRegistry.objects.afirst())

        response = await self.async_client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data={
                "is_default": True,
                "registry_domain": "registry.example.com",
            },
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        monitor = MonitorRegistryDeploymentActivites()

        # a late v1 health result must not overwrite the v2 state
        await monitor.save_registry_health_check_status(
            RegistryHealthCheckResult(
                id=registry.id,
                version=1,
                status=BuildRegistry.RegistryHealthStatus.UNHEALTHY,
                reason="late result from the previous generation",
            )
        )
        await registry.arefresh_from_db()
        self.assertEqual(
            BuildRegistry.RegistryHealthStatus.HEALTHY, registry.health_status
        )

        # a current generation result still applies
        await monitor.save_registry_health_check_status(
            RegistryHealthCheckResult(
                id=registry.id,
                version=2,
                status=BuildRegistry.RegistryHealthStatus.UNHEALTHY,
                reason="current generation failure",
            )
        )
        await registry.arefresh_from_db()
        self.assertEqual(
            BuildRegistry.RegistryHealthStatus.UNHEALTHY,
            registry.health_status,
        )

    @responses.activate()
    async def test_publish_arbitration_never_lets_old_generation_win(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )
        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(BuildRegistry, await BuildRegistry.objects.afirst())

        response = await self.async_client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data={
                "is_default": True,
                "registry_domain": "registry.example.com",
            },
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        # an outdated v1 workflow reaching publish after v2 is live
        old_generation = await registry.generations.aget(version=1)
        old_generation.status = BuildRegistryGeneration.GenerationStatus.STAGING
        await old_generation.asave()

        publish_status = await sync_to_async(_publish_registry_generation_sync)(
            old_generation.id
        )
        self.assertEqual(
            BuildRegistryGeneration.GenerationStatus.SUPERSEDED, publish_status
        )

        await registry.arefresh_from_db()
        await old_generation.arefresh_from_db()
        self.assertEqual(2, registry.version)
        self.assertEqual("registry.example.com", registry.registry_domain)
        self.assertEqual(
            BuildRegistry.RegistryHealthStatus.HEALTHY, registry.health_status
        )
        self.assertEqual(
            BuildRegistryGeneration.GenerationStatus.SUPERSEDED,
            old_generation.status,
        )

    @responses.activate()
    async def test_swarm_reconcile_does_not_update_service_twice(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )
        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(BuildRegistry, await BuildRegistry.objects.afirst())

        response = await self.async_client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data={
                "is_default": True,
                "registry_domain": "registry.example.com",
            },
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        generation = await registry.generations.aget(version=2)
        snapshot = registry_snapshot_from_generation(generation)
        swarm_service = cast(
            FakeDockerClient.FakeService,
            self.fake_docker_client.service_map.get(
                cast(str, registry.swarm_service_name)
            ),
        )

        details = SwarmRegistryServiceDetails(
            alias=cast(str, registry.service_alias),
            swarm_id=cast(str, registry.swarm_service_name),
            configs={},
            registry=snapshot,
            generation_id=generation.id,
        )

        # the service already runs v2: reconcile must not issue an update
        with patch.object(
            FakeDockerClient.FakeService, "update"
        ) as update_spy:
            await reconcile_build_registry_swarm_service(details)
            update_spy.assert_not_called()

        # if the running task and the spec still target v1, exactly one update
        # is issued, and a second reconcile after that is a no-op
        swarm_service.labels["version"] = "1"
        swarm_service.attrs["Spec"]["Labels"]["version"] = "1"
        newest_task = swarm_service.swarm_tasks[-1]
        newest_task["Spec"]["ContainerSpec"]["Env"] = [
            env if not env.startswith("__ZANE_VERSION=") else "__ZANE_VERSION=1"
            for env in newest_task["Spec"]["ContainerSpec"]["Env"]
        ]
        with patch.object(
            FakeDockerClient.FakeService, "update", wraps=swarm_service.update
        ) as update_spy:
            await reconcile_build_registry_swarm_service(details)
            self.assertEqual(1, update_spy.call_count)
            await reconcile_build_registry_swarm_service(details)
            self.assertEqual(1, update_spy.call_count)

    @responses.activate()
    async def test_cleanup_never_reaps_active_generation_configs(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)

        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.127.0.0.0.1.sslip.io",
        }
        response = await self.async_client.post(
            reverse("container_registry:build_registries.list"), data=body
        )
        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(BuildRegistry, await BuildRegistry.objects.afirst())

        response = await self.async_client.patch(
            reverse(
                "container_registry:build_registries.details",
                kwargs={"id": registry.id},
            ),
            data={
                "is_default": True,
                "registry_domain": "registry.example.com",
            },
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        await registry.arefresh_from_db()
        active_generation = await registry.generations.aget(version=2)
        active_config = self.fake_docker_client.config_map.get(
            get_config_name_for_registry(registry, "config")  # type: ignore
        )
        active_credentials = self.fake_docker_client.config_map.get(
            get_config_name_for_registry(registry, "credentials")  # type: ignore
        )
        self.assertIsNotNone(active_config)
        self.assertIsNotNone(active_credentials)

        # re-running the reference-based cleanup keeps the live configs intact
        await cleanup_superseded_registry_configs(active_generation.id)
        self.assertIn(active_config.name, self.fake_docker_client.config_map)
        self.assertIn(active_credentials.name, self.fake_docker_client.config_map)


class BuildRegistryProxyTests(AuthAPITestCase):
    def test_certificate_for_build_registry_domain(self):
        self.loginUser()
        body = {
            "name": "My registry",
            "is_default": True,
            "registry_domain": "registry.zaneops.io",
            "registry_username": "fredkisss",
        }
        response = self.client.post(
            reverse("container_registry:build_registries.list"), data=body
        )

        jprint(response.json())

        self.assertEqual(status.HTTP_201_CREATED, response.status_code)
        registry = cast(
            BuildRegistry,
            BuildRegistry.objects.first(),
        )
        self.assertIsNotNone(registry)
        response = self.client.get(
            reverse(
                "zane_api:proxy.check_certificates",
                query={"domain": registry.registry_domain},
            ),
        )
        jprint(response.json())
        self.assertEqual(status.HTTP_200_OK, response.status_code)
