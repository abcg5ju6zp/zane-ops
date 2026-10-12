import re
from copy import deepcopy
from typing import cast
from unittest.mock import AsyncMock, patch

from asgiref.sync import sync_to_async
from django.conf import settings
from django.urls import reverse
from rest_framework import status

from zane_api.models import Deployment, GitApp, Workspace
from zane_api.tests.base import AuthAPITestCase
from zane_api.utils import generate_random_chars

import responses

from ..delivery import (
    adispatch_delivery,
    arecover_pending_deliveries,
    run_pending_starts,
)
from ..models import GitHubApp, GitRefGeneration, GitWebhookDelivery
from ..models import GitlabApp
from ..serializers import GithubWebhookEvent, GitlabWebhookEvent
from temporal.client import TemporalClient
from temporal.workflows import DeployGitServiceWorkflow
from .fixtures import (
    GITHUB_APP_MANIFEST_DATA,
    GITHUB_INSTALLATION_CREATED_WEBHOOK_DATA,
    GITHUB_PUSH_WEBHOOK_EVENT_DATA,
    GITHUB_PULL_REQUEST_WEBHOOK_EVENT_DATA,
    GITLAB_PUSH_WEBHOOK_EVENT_DATA,
    GITLAB_PROJECT_LIST,
    GITLAB_ACCESS_TOKEN_DATA,
    GITLAB_PROJECT_WEBHOOK_API_DATA,
    get_github_signed_event_headers,
    mock_github_comments_api,
)
from urllib.parse import urlencode

ZERO_SHA = "0" * 40
OLDER_SHA = "1" * 40
SHA_A = "a" * 40
SHA_B = "b" * 40


def make_push(after_sha: str, before_sha: str):
    data = deepcopy(GITHUB_PUSH_WEBHOOK_EVENT_DATA)
    data["before"] = before_sha
    data["after"] = after_sha
    data["head_commit"]["id"] = after_sha
    data["commits"][0]["id"] = after_sha
    return data


class GithubWebhookIdempotencyTests(AuthAPITestCase):
    async def asetup_github_app(self):
        await self.aLoginUser()
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)
        github_api_pattern = re.compile(
            r"^https://api\.github\.com/app/installations/.*",
            re.IGNORECASE,
        )
        responses.add(
            responses.POST,
            url=github_api_pattern,
            status=status.HTTP_200_OK,
            json={"token": generate_random_chars(32)},
        )
        gh_app = await GitHubApp.objects.acreate(
            webhook_secret=GITHUB_APP_MANIFEST_DATA["webhook_secret"],
            app_id=GITHUB_APP_MANIFEST_DATA["id"],
            name=GITHUB_APP_MANIFEST_DATA["name"],
            client_id=GITHUB_APP_MANIFEST_DATA["client_id"],
            client_secret=GITHUB_APP_MANIFEST_DATA["client_secret"],
            private_key=GITHUB_APP_MANIFEST_DATA["pem"],
            app_url=GITHUB_APP_MANIFEST_DATA["html_url"],
            installation_id=1,
        )
        git_app = await GitApp.objects.acreate(
            github=gh_app,
            workspace=await Workspace.objects.aearliest("created_at"),
        )
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=GITHUB_INSTALLATION_CREATED_WEBHOOK_DATA,
            headers=get_github_signed_event_headers(
                GithubWebhookEvent.INSTALLATION,
                GITHUB_INSTALLATION_CREATED_WEBHOOK_DATA,
                gh_app.webhook_secret,
            ),
        )
        assert response.status_code == status.HTTP_200_OK
        return git_app

    def github_headers(self, event: str, data: dict, secret: str, delivery_id=None):
        headers = get_github_signed_event_headers(event, data, secret)
        if delivery_id is not None:
            headers["X-GitHub-Delivery"] = delivery_id
        return headers

    @responses.activate
    async def test_same_delivery_id_redelivery_creates_a_single_deployment(self):
        gitapp = await self.asetup_github_app()
        secret = cast(GitHubApp, gitapp.github).webhook_secret
        await self.acreate_git_service(
            repository_url="https://github.com/Fredkiss3/private-ac",
            git_app_id=gitapp.id,
        )

        for _ in range(2):
            response = await self.async_client.post(
                reverse("git_connectors:github.webhook"),
                data=GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                headers=self.github_headers(
                    GithubWebhookEvent.PUSH,
                    GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                    secret,
                    delivery_id="72d2b7e0-1234-1234-1234-123456789abc",
                ),
            )
            self.assertEqual(status.HTTP_200_OK, response.status_code)

        self.assertEqual(
            1,
            await Deployment.objects.filter(
                trigger_method=Deployment.DeploymentTriggerMethod.AUTO
            ).acount(),
        )
        # the installation delivery also exists, but the push was delivered
        # exactly once
        self.assertEqual(
            1,
            await GitWebhookDelivery.objects.filter(
                provider=GitWebhookDelivery.Provider.GITHUB,
                event_type=GithubWebhookEvent.PUSH,
            ).acount(),
        )

    @responses.activate
    async def test_redelivery_without_delivery_header_is_deduped_by_digest(self):
        gitapp = await self.asetup_github_app()
        secret = cast(GitHubApp, gitapp.github).webhook_secret
        await self.acreate_git_service(
            repository_url="https://github.com/Fredkiss3/private-ac",
            git_app_id=gitapp.id,
        )

        for _ in range(2):
            response = await self.async_client.post(
                reverse("git_connectors:github.webhook"),
                data=GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                headers=self.github_headers(
                    GithubWebhookEvent.PUSH,
                    GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                    secret,
                ),
            )
            self.assertEqual(status.HTTP_200_OK, response.status_code)

        self.assertEqual(
            1,
            await Deployment.objects.filter(
                trigger_method=Deployment.DeploymentTriggerMethod.AUTO
            ).acount(),
        )
        self.assertEqual(
            1,
            await GitWebhookDelivery.objects.filter(
                event_type=GithubWebhookEvent.PUSH
            ).acount(),
        )

    def test_invalid_signature_does_not_create_any_state(self):
        self.loginUser()
        gh_app = GitHubApp.objects.create(
            webhook_secret=GITHUB_APP_MANIFEST_DATA["webhook_secret"],
            app_id=GITHUB_APP_MANIFEST_DATA["id"],
            name=GITHUB_APP_MANIFEST_DATA["name"],
            client_id=GITHUB_APP_MANIFEST_DATA["client_id"],
            client_secret=GITHUB_APP_MANIFEST_DATA["client_secret"],
            private_key=GITHUB_APP_MANIFEST_DATA["pem"],
            app_url=GITHUB_APP_MANIFEST_DATA["html_url"],
            installation_id=1,
        )
        GitApp.objects.create(
            github=gh_app, workspace=Workspace.objects.earliest("created_at")
        )

        response = self.client.post(
            reverse("git_connectors:github.webhook"),
            data=GITHUB_PUSH_WEBHOOK_EVENT_DATA,
            headers=self.github_headers(
                GithubWebhookEvent.PUSH,
                GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                "wrong-secret",
                delivery_id="72d2b7e0-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ),
        )
        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertEqual(0, GitWebhookDelivery.objects.count())
        self.assertEqual(0, GitRefGeneration.objects.count())

    def test_unsupported_event_is_rejected_before_any_state(self):
        self.loginUser()
        gh_app = GitHubApp.objects.create(
            webhook_secret=GITHUB_APP_MANIFEST_DATA["webhook_secret"],
            app_id=GITHUB_APP_MANIFEST_DATA["id"],
            name=GITHUB_APP_MANIFEST_DATA["name"],
            client_id=GITHUB_APP_MANIFEST_DATA["client_id"],
            client_secret=GITHUB_APP_MANIFEST_DATA["client_secret"],
            private_key=GITHUB_APP_MANIFEST_DATA["pem"],
            app_url=GITHUB_APP_MANIFEST_DATA["html_url"],
            installation_id=1,
        )
        GitApp.objects.create(
            github=gh_app, workspace=Workspace.objects.earliest("created_at")
        )

        headers = self.github_headers(
            "issues", GITHUB_PUSH_WEBHOOK_EVENT_DATA, gh_app.webhook_secret
        )
        # override the event header with an unsupported event
        headers["X-GitHub-Event"] = "issues"
        response = self.client.post(
            reverse("git_connectors:github.webhook"),
            data=GITHUB_PUSH_WEBHOOK_EVENT_DATA,
            headers=headers,
        )
        self.assertEqual(status.HTTP_400_BAD_REQUEST, response.status_code)
        self.assertEqual(0, GitWebhookDelivery.objects.count())

    @responses.activate
    async def test_late_older_push_cannot_rollback_newer_commit(self):
        gitapp = await self.asetup_github_app()
        secret = cast(GitHubApp, gitapp.github).webhook_secret
        _, service = await self.acreate_git_service(
            repository_url="https://github.com/Fredkiss3/private-ac",
            git_app_id=gitapp.id,
        )

        newer_push = make_push(after_sha=SHA_B, before_sha=SHA_A)
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=newer_push,
            headers=self.github_headers(
                GithubWebhookEvent.PUSH,
                newer_push,
                secret,
                delivery_id="delivery-b",
            ),
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        # the older push event arrives late (reordered redelivery)
        older_push = make_push(after_sha=SHA_A, before_sha=OLDER_SHA)
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=older_push,
            headers=self.github_headers(
                GithubWebhookEvent.PUSH,
                older_push,
                secret,
                delivery_id="delivery-a",
            ),
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        auto_deployments = [
            d
            async for d in Deployment.objects.filter(
                trigger_method=Deployment.DeploymentTriggerMethod.AUTO
            )
        ]
        self.assertEqual(1, len(auto_deployments))
        self.assertEqual(SHA_B, auto_deployments[0].commit_sha)

        stale = await GitWebhookDelivery.objects.aget(
            provider=GitWebhookDelivery.Provider.GITHUB,
            delivery_id="delivery-a",
        )
        self.assertTrue(stale.stale)
        self.assertEqual(GitWebhookDelivery.Stage.IGNORED, stale.stage)

    @responses.activate
    async def test_push_after_repository_removal_is_ignored(self):
        gitapp = await self.asetup_github_app()
        secret = cast(GitHubApp, gitapp.github).webhook_secret
        _, service = await self.acreate_git_service(
            repository_url="https://github.com/Fredkiss3/private-ac",
            git_app_id=gitapp.id,
        )
        self.assertEqual(
            0,
            await service.deployments.filter(
                trigger_method=Deployment.DeploymentTriggerMethod.AUTO
            ).acount(),
        )

        # repository removed from the GitHub installation
        removal_data = {
            "action": "removed",
            "installation": {"id": 1, "app_id": GITHUB_APP_MANIFEST_DATA["id"]},
            "repository_selection": "selected",
            "repositories_added": [],
            "repositories_removed": [
                {
                    "id": 4,
                    "full_name": "Fredkiss3/private-ac",
                    "private": True,
                }
            ],
        }
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=removal_data,
            headers=self.github_headers(
                GithubWebhookEvent.INSTALLATION_REPOS,
                removal_data,
                secret,
                delivery_id="delivery-removal",
            ),
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        # a late push for the detached repository must not deploy anything
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=GITHUB_PUSH_WEBHOOK_EVENT_DATA,
            headers=self.github_headers(
                GithubWebhookEvent.PUSH,
                GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                secret,
                delivery_id="delivery-late-push",
            ),
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual(
            0,
            await Deployment.objects.filter(
                trigger_method=Deployment.DeploymentTriggerMethod.AUTO
            ).acount(),
        )
        late_delivery = await GitWebhookDelivery.objects.aget(
            provider=GitWebhookDelivery.Provider.GITHUB,
            delivery_id="delivery-late-push",
        )
        self.assertEqual(GitWebhookDelivery.Stage.IGNORED, late_delivery.stage)

    @responses.activate
    async def test_synchronize_after_close_does_not_recreate_the_preview(self):
        mock_github_comments_api()
        gitapp = await self.asetup_github_app()
        secret = cast(GitHubApp, gitapp.github).webhook_secret

        await self.acreate_and_deploy_redis_docker_service()
        await self.acreate_and_deploy_git_service(
            slug="fredkiss-dev",
            repository="https://github.com/Fredkiss3/fredkiss.dev",
            git_app_id=gitapp.id,
        )

        # opened
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=GITHUB_PULL_REQUEST_WEBHOOK_EVENT_DATA,
            headers=self.github_headers(
                GithubWebhookEvent.PULL_REQUEST,
                GITHUB_PULL_REQUEST_WEBHOOK_EVENT_DATA,
                secret,
                delivery_id="pr-opened",
            ),
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual(1, await GitRefGeneration.objects.acount())

        # closed
        closed_data = deepcopy(GITHUB_PULL_REQUEST_WEBHOOK_EVENT_DATA)
        closed_data["action"] = "closed"
        closed_data["pull_request"]["state"] = "closed"
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=closed_data,
            headers=self.github_headers(
                GithubWebhookEvent.PULL_REQUEST,
                closed_data,
                secret,
                delivery_id="pr-closed",
            ),
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        # a late synchronize delivery (different delivery id, same head)
        late_sync = deepcopy(GITHUB_PULL_REQUEST_WEBHOOK_EVENT_DATA)
        late_sync["action"] = "synchronize"
        response = await self.async_client.post(
            reverse("git_connectors:github.webhook"),
            data=late_sync,
            headers=self.github_headers(
                GithubWebhookEvent.PULL_REQUEST,
                late_sync,
                secret,
                delivery_id="pr-late-sync",
            ),
        )
        self.assertEqual(status.HTTP_200_OK, response.status_code)

        self.assertEqual(
            0,
            await GitRefGeneration.objects.filter(
                state=GitRefGeneration.RefState.ACTIVE
            ).acount(),
        )
        stale = await GitWebhookDelivery.objects.aget(
            provider=GitWebhookDelivery.Provider.GITHUB,
            delivery_id="pr-late-sync",
        )
        self.assertEqual(GitWebhookDelivery.Stage.IGNORED, stale.stage)

    @responses.activate
    async def test_committed_delivery_with_uncertain_start_is_resumed_on_recovery(
        self,
    ):
        gitapp = await self.asetup_github_app()
        _, service = await self.acreate_git_service(
            repository_url="https://github.com/Fredkiss3/private-ac",
            git_app_id=gitapp.id,
        )

        # share a live temporal environment between the failed delivery and
        # the restart-recovery
        async with self.workflowEnvironment():
            # the Temporal start response is uncertain: request failed, and
            # the workflow cannot be found yet
            with (
                patch(
                    "temporal.client.TemporalClient.astart_workflow",
                    new_callable=AsyncMock,
                    side_effect=RuntimeError("network timeout"),
                ),
                patch(
                    "temporal.client.TemporalClient.aworkflow_exists",
                    new_callable=AsyncMock,
                    return_value=False,
                ),
            ):
                response = await self.async_client.post(
                    reverse("git_connectors:github.webhook"),
                    data=GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                    headers=self.github_headers(
                        GithubWebhookEvent.PUSH,
                        GITHUB_PUSH_WEBHOOK_EVENT_DATA,
                        cast(GitHubApp, gitapp.github).webhook_secret,
                        delivery_id="delivery-unclear",
                    ),
                )
            self.assertEqual(status.HTTP_200_OK, response.status_code)

            delivery = await GitWebhookDelivery.objects.aget(
                provider=GitWebhookDelivery.Provider.GITHUB,
                delivery_id="delivery-unclear",
            )
            self.assertEqual(GitWebhookDelivery.Stage.EFFECTS_COMMITTED, delivery.stage)
            deployment = await service.deployments.filter(
                trigger_method=Deployment.DeploymentTriggerMethod.AUTO
            ).afirst()
            self.assertIsNotNone(deployment)

            # restart recovery resumes the original delivery without creating
            # a second deployment
            resumed = await adispatch_delivery(delivery.id)
            self.assertTrue(resumed)
            # bulk recovery converges: after draining, nothing is left pending
            await arecover_pending_deliveries()
            self.assertEqual(0, await arecover_pending_deliveries())

            delivery = await GitWebhookDelivery.objects.aget(id=delivery.id)
            self.assertEqual(GitWebhookDelivery.Stage.STARTED, delivery.stage)
            self.assertEqual(
                1,
                await Deployment.objects.filter(
                    trigger_method=Deployment.DeploymentTriggerMethod.AUTO
                ).acount(),
            )
            exists = await TemporalClient.aworkflow_exists(
                DeployGitServiceWorkflow.run, deployment.workflow_id
            )
            self.assertTrue(exists)


class GitlabWebhookIdempotencyTests(AuthAPITestCase):
    def create_gitlab_app(self):
        self.loginUser()
        body = {
            "app_id": generate_random_chars(10),
            "app_secret": generate_random_chars(40),
            "redirect_uri": f"https://{settings.ZANE_APP_DOMAIN}/api/connectors/gitlab/setup",
            "gitlab_url": "https://gitlab.com",
            "name": "foxylab",
        }
        response = self.client.post(reverse("git_connectors:gitlab.create"), data=body)
        assert response.status_code == status.HTTP_200_OK
        state = response.json()["state"]

        responses.add(
            responses.POST,
            url=re.compile(r"https://gitlab\.com/oauth/token/?", re.IGNORECASE),
            status=status.HTTP_200_OK,
            json=GITLAB_ACCESS_TOKEN_DATA,
        )
        project_pattern = re.compile(
            r"https://gitlab\.com/api/v4/projects/?", re.IGNORECASE
        )
        responses.add(
            responses.GET, url=project_pattern, status=200, json=GITLAB_PROJECT_LIST
        )
        responses.add(responses.GET, url=project_pattern, status=200, json=[])
        responses.add(
            responses.POST,
            url=re.compile(
                r"https://gitlab\.com/api/v4/projects/[0-9]+/hooks", re.IGNORECASE
            ),
            status=200,
            json=GITLAB_PROJECT_WEBHOOK_API_DATA,
        )

        response = self.client.get(
            reverse("git_connectors:gitlab.setup"),
            QUERY_STRING=urlencode(
                {"code": generate_random_chars(10), "state": state}, doseq=True
            ),
        )
        assert response.status_code == status.HTTP_303_SEE_OTHER
        return cast(
            GitApp,
            GitApp.objects.filter(gitlab__app_id=body["app_id"])
            .select_related("gitlab")
            .get(),
        )

    @responses.activate
    async def test_gitlab_push_redelivery_creates_a_single_deployment(self):
        responses.add_passthru(settings.CADDY_PROXY_ADMIN_HOST)
        responses.add_passthru(settings.LOKI_HOST)
        gitapp = await sync_to_async(self.create_gitlab_app)()
        gitlab = cast(GitlabApp, gitapp.gitlab)

        await self.acreate_git_service(
            repository_url="https://gitlab.com/fredkiss3/private-ac",
            git_app_id=gitapp.id,
        )

        for _ in range(2):
            response = await self.async_client.post(
                reverse("git_connectors:gitlab.webhook"),
                data=GITLAB_PUSH_WEBHOOK_EVENT_DATA,
                headers={
                    "X-Gitlab-Event": GitlabWebhookEvent.PUSH,
                    "X-Gitlab-Token": gitlab.webhook_secret,
                },
            )
            self.assertEqual(status.HTTP_200_OK, response.status_code)

        self.assertEqual(
            1,
            await Deployment.objects.filter(
                trigger_method=Deployment.DeploymentTriggerMethod.AUTO
            ).acount(),
        )
        self.assertEqual(
            1,
            await GitWebhookDelivery.objects.filter(
                provider=GitWebhookDelivery.Provider.GITLAB
            ).acount(),
        )


class PendingStartsDispatchTests(AuthAPITestCase):
    def test_uncertain_start_confirmed_by_existing_workflow(self):
        item = {
            "workflow": "create-env-network",
            "workflow_id": "create-env-project-env-1",
            "start_delay_seconds": None,
            "status": "pending",
            "payload": {
                "id": "project_env_1",
                "name": "preview-1",
                "project_id": "prj_1",
                "compose_stacks": [],
            },
        }
        with (
            patch(
                "git_connectors.delivery.TemporalClient.start_workflow",
                side_effect=RuntimeError("timeout"),
            ) as start_mock,
            patch(
                "git_connectors.delivery.TemporalClient.workflow_exists",
                return_value=True,
            ) as exists_mock,
        ):
            confirmed = run_pending_starts([item], [])

        self.assertTrue(confirmed)
        self.assertEqual("started", item["status"])
        start_mock.assert_called_once()
        exists_mock.assert_called_once()

    def test_uncertain_start_unresolved_is_left_pending(self):
        item = {
            "workflow": "create-env-network",
            "workflow_id": "create-env-project-env-2",
            "start_delay_seconds": None,
            "status": "pending",
            "payload": {
                "id": "project_env_2",
                "name": "preview-2",
                "project_id": "prj_2",
                "compose_stacks": [],
            },
        }
        with (
            patch(
                "git_connectors.delivery.TemporalClient.start_workflow",
                side_effect=RuntimeError("timeout"),
            ),
            patch(
                "git_connectors.delivery.TemporalClient.workflow_exists",
                return_value=False,
            ),
        ):
            confirmed = run_pending_starts([item], [])

        self.assertFalse(confirmed)
        self.assertEqual("pending", item["status"])
