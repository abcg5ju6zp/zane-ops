from datetime import timedelta

from asgiref.sync import sync_to_async
from django.contrib.auth.models import User
from django.db import transaction
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from temporalio.exceptions import ApplicationError

from temporal.shared import (
    ComposeStackHealthcheckResult,
    DeploymentResult,
)
from temporal.activities.workspace_retirement import (
    areconcile_workspace_retirements,
)
from temporal.schedules.activities import (
    MonitorComposeStackActivites,
    MonitorDockerDeploymentActivities,
)
from temporal.activities import DockerSwarmActivities

from compose.models import ComposeStack

from ..models import (
    Workspace,
    WorkspaceInvitation,
    WorkspaceMembership,
    WorkspaceRetirement,
    WorkspaceRole,
    Project,
    Environment,
    Deployment,
)
from ..retirement import (
    dispatch_workspace_retirement,
    start_workspace_retirement,
)
from ..constants import WORKSPACE_SESSION_KEY
from .base import AuthAPITestCase


def retire_workspace(workspace: Workspace) -> WorkspaceRetirement:
    """Open a retirement generation directly (no HTTP / temporal dispatch)."""
    with transaction.atomic():
        return start_workspace_retirement(workspace)


class WorkspaceRetirementFreezeTests(AuthAPITestCase):
    def _retire_default_workspace(self):
        self.loginUser()
        self.create_and_deploy_caddy_docker_service()
        response = self.client.delete(reverse("zane_api:workspace.details"))
        self.assertEqual(status.HTTP_202_ACCEPTED, response.status_code)
        return response.json()

    def test_workspace_is_still_queryable_while_retiring(self):
        body = self._retire_default_workspace()
        self.assertEqual("RETIRING", body["status"])

        response = self.client.get(reverse("zane_api:workspace.details"))
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        retirement = response.json()["retirement"]
        self.assertEqual(body["generation"], retirement["generation"])
        self.assertEqual("RETIRING", retirement["status"])

        # reads stay allowed
        response = self.client.get(reverse("zane_api:projects.list"))
        self.assertEqual(status.HTTP_200_OK, response.status_code)

    def test_project_and_deployment_writes_are_rejected(self):
        self._retire_default_workspace()

        response = self.client.post(
            reverse("zane_api:projects.list"),
            data={"slug": "another-project"},
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

    def test_member_changes_are_rejected(self):
        body = self._retire_default_workspace()

        other_user = User.objects.create_user(
            username="bobby", password="password123"
        )
        second_membership = WorkspaceMembership.objects.create(
            user=other_user,
            workspace=Workspace.objects.get(pk=body["snapshot"]["workspace"]["id"]),
            role=WorkspaceRole.MEMBER,
        )

        response = self.client.put(
            reverse(
                "zane_api:workspace.edit_membership_permissions",
                kwargs={"membership_id": second_membership.id},
            ),
            data={"role": WorkspaceRole.ADMIN, "accessible_project_ids": []},
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

        response = self.client.delete(
            reverse(
                "zane_api:workspace.membership_detail",
                kwargs={"membership_id": second_membership.id},
            ),
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

        response = self.client.post(reverse("zane_api:workspace.leave"))
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

        response = self.client.post(
            reverse("zane_api:workspace.transfer_ownership"),
            data={"new_owner_id": other_user.id},
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

        # the membership is untouched
        self.assertTrue(
            WorkspaceMembership.objects.filter(pk=second_membership.pk).exists()
        )

    def test_invitation_and_token_writes_are_rejected(self):
        self._retire_default_workspace()

        response = self.client.post(
            reverse("zane_api:workspace.invite_user"),
            data={
                "username": "newinvitee",
                "role": WorkspaceRole.MEMBER,
                "valid_for": 3,
                "accessible_project_ids": [],
            },
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

        response = self.client.post(
            reverse("zane_api:workspace.tokens"),
            data={"name": "ci", "role": WorkspaceRole.MEMBER, "scopes": []},
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

    def test_api_token_deploy_webhook_is_rejected(self):
        self.loginUser()
        project, service = self.create_and_deploy_caddy_docker_service()
        kwargs = self.deploy_token_kwargs()

        response = self.client.delete(reverse("zane_api:workspace.details"))
        self.assertEqual(status.HTTP_202_ACCEPTED, response.status_code)

        response = self.client.put(
            reverse(
                "zane_api:services.docker.webhook_deploy",
                kwargs={"deploy_token": service.deploy_token},
            ),
            data={},
            **kwargs,
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)

    def test_invitation_acceptance_is_rejected_for_retiring_workspace(self):
        body = self._retire_default_workspace()
        invitee = User.objects.create_user(
            username="invitee", password="password123"
        )
        invitation = WorkspaceInvitation.objects.create(
            workspace_id=body["snapshot"]["workspace"]["id"],
            username="invitee",
            token="0" * 32,
            role=WorkspaceRole.MEMBER,
            expires_at=timezone.now() + timedelta(days=1),
            invited_by=User.objects.get(username="Fredkiss3"),
        )

        self.client.force_login(invitee)
        response = self.client.post(
            reverse(
                "zane_api:workspace.review_invitation",
                kwargs={"token": invitation.token},
            ),
            data={"decision": "ACCEPT"},
        )
        self.assertEqual(status.HTTP_409_CONFLICT, response.status_code)
        self.assertFalse(
            WorkspaceMembership.objects.filter(
                user=invitee,
                workspace_id=body["snapshot"]["workspace"]["id"],
            ).exists()
        )

    def test_switching_workspace_stays_allowed(self):
        user = self.loginUser()
        body = self._retire_default_workspace()

        second_workspace = Workspace.objects.create(name="Second workspace")
        WorkspaceMembership.objects.create(
            user=user, workspace=second_workspace, role=WorkspaceRole.OWNER
        )

        response = self.client.post(
            reverse("zane_api:workspaces.switch"),
            data={"workspace_id": second_workspace.id},
        )
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)
        self.assertEqual(
            second_workspace.id,
            self.client.session.get(WORKSPACE_SESSION_KEY),
        )

        # can even switch back to the retiring workspace (read-only access)
        response = self.client.post(
            reverse("zane_api:workspaces.switch"),
            data={"workspace_id": body["snapshot"]["workspace"]["id"]},
        )
        self.assertEqual(status.HTTP_204_NO_CONTENT, response.status_code)

    def test_repeated_delete_returns_same_generation_without_new_archive(self):
        body = self._retire_default_workspace()

        from ..models import ArchivedDockerService

        archived_before = ArchivedDockerService.objects.count()
        tasks_before = WorkspaceRetirement.objects.get(
            generation=body["generation"]
        ).tasks.count()

        response = self.client.delete(reverse("zane_api:workspace.details"))
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual(body["generation"], response.json()["generation"])

        response = self.client.delete(reverse("zane_api:workspace.details"))
        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual(archived_before, ArchivedDockerService.objects.count())
        retirement = WorkspaceRetirement.objects.get(generation=body["generation"])
        self.assertEqual(tasks_before, retirement.tasks.count())


class WorkspaceRetirementLateCallbackTests(AuthAPITestCase):
    async def test_late_deployment_callbacks_cannot_resurret_resource(self):
        project, service = await sync_to_async(self.create_caddy_docker_service)()
        workspace = await Workspace.objects.aget(projects__pk=project.pk)
        deployment = await sync_to_async(service.prepare_new_docker_deployment)()
        deployment.is_current_production = True
        await deployment.asave(update_fields=["is_current_production"])

        # retire the workspace *without* going through the archive transaction:
        # the late callback must still validate the active generation
        await sync_to_async(WorkspaceRetirement.objects.create)(
            workspace=workspace,
            workspace_original_id=workspace.id,
            workflow_id="workspace-retirement-test-late",
        )

        monitor = MonitorDockerDeploymentActivities()
        await monitor.save_deployment_status(
            DeploymentResult(
                deployment_hash=deployment.hash,
                status=Deployment.DeploymentStatus.HEALTHY,
                service_id=service.id,
                reason="late callback",
            )
        )
        await deployment.arefresh_from_db()
        self.assertEqual(Deployment.DeploymentStatus.QUEUED, deployment.status)

        swarm = DockerSwarmActivities()
        try:
            await swarm.finish_and_save_deployment(
                DeploymentResult(
                    deployment_hash=deployment.hash,
                    status=Deployment.DeploymentStatus.HEALTHY,
                    service_id=service.id,
                    reason="late callback",
                )
            )
        except ApplicationError as exc:
            self.assertTrue(exc.non_retryable)
        else:
            self.fail("finish_and_save_deployment should reject the late callback")

        await deployment.arefresh_from_db()
        self.assertNotEqual(Deployment.DeploymentStatus.HEALTHY, deployment.status)

    async def test_late_stack_health_callback_does_not_overwrite_stack(self):
        project, _ = await sync_to_async(self.create_caddy_docker_service)()
        workspace = await Workspace.objects.aget(projects__pk=project.pk)
        env = await Environment.objects.aget(project=project)
        stack = await sync_to_async(ComposeStack.objects.create)(
            project=project,
            environment=env,
            slug="web",
            network_alias_prefix="web",
            user_content="services: {}",
            computed_content="services: {}",
        )

        await sync_to_async(WorkspaceRetirement.objects.create)(
            workspace=workspace,
            workspace_original_id=workspace.id,
            workflow_id="workspace-retirement-test-late-stack",
        )

        stack_monitor = MonitorComposeStackActivites()
        await stack_monitor.save_stack_health_check_status(
            ComposeStackHealthcheckResult(id=stack.id, services={})
        )
        await stack.arefresh_from_db()
        # the late write is dropped, the retiring stack keeps its frozen state
        self.assertEqual({}, stack.services)


class WorkspaceRetirementReconcileTests(AuthAPITestCase):
    async def test_full_retirement_then_reconcile_is_idempotent(self):
        await self.aLoginUser()
        await self.acreate_and_deploy_caddy_docker_service()

        response = await self.async_client.delete(
            reverse("zane_api:workspace.details")
        )
        self.assertEqual(status.HTTP_202_ACCEPTED, response.status_code)
        generation = response.json()["generation"]

        retirement = await WorkspaceRetirement.objects.aget(generation=generation)
        self.assertEqual(
            WorkspaceRetirement.RetirementStatus.COMPLETED, retirement.status
        )

        # running the restart-recovery reconciliation afterwards changes nothing
        result = await areconcile_workspace_retirements()
        self.assertEqual([], result.redelivered)
        self.assertEqual([], result.finalized)
        retirement = await WorkspaceRetirement.objects.aget(generation=generation)
        self.assertEqual(
            WorkspaceRetirement.RetirementStatus.COMPLETED, retirement.status
        )

    async def test_duplicated_delivery_is_idempotent(self):
        user = await self.aLoginUser()
        await self.acreate_and_deploy_caddy_docker_service()
        workspace = await Workspace.objects.aget(memberships__user=user)

        # first delivery runs through the on-commit callback to finalization
        response = await self.async_client.delete(
            reverse("zane_api:workspace.details")
        )
        self.assertEqual(status.HTTP_202_ACCEPTED, response.status_code)
        generation = response.json()["generation"]
        retirement = await WorkspaceRetirement.objects.aget(generation=generation)
        self.assertEqual(
            WorkspaceRetirement.RetirementStatus.COMPLETED, retirement.status
        )
        self.assertIsNone(await Workspace.objects.filter(pk=workspace.pk).afirst())

        # a duplicated / late start response (e.g. after a restart) must be a
        # safe no-op acknowledgement on the stable workflow ID
        await sync_to_async(dispatch_workspace_retirement, thread_sensitive=False)(
            retirement.pk,
            workflow_id=retirement.workflow_id,
            generation=str(retirement.generation),
        )
        retirement = await WorkspaceRetirement.objects.aget(generation=generation)
        self.assertEqual(
            WorkspaceRetirement.RetirementStatus.COMPLETED, retirement.status
        )

    async def test_reconcile_redelivers_pending_generation(self):
        user = await self.aLoginUser()
        async with self.workflowEnvironment():
            workspace = await Workspace.objects.aget(memberships__user=user)
            project = await sync_to_async(Project.objects.create)(
                slug="zaneops", workspace=workspace
            )
            await sync_to_async(project.environments.create)(name="production")

            retirement = await sync_to_async(retire_workspace)(workspace)
            # simulates a service restart: start response was lost, row stays
            # PENDING and nothing was ever delivered
            self.assertEqual(
                WorkspaceRetirement.DeliveryStatus.PENDING,
                retirement.delivery_status,
            )

            result = await areconcile_workspace_retirements()
            self.assertIn(str(retirement.generation), result.redelivered)

            await retirement.arefresh_from_db()
            self.assertEqual(
                WorkspaceRetirement.RetirementStatus.COMPLETED, retirement.status
            )
            self.assertIsNone(
                await Workspace.objects.filter(pk=workspace.pk).afirst()
            )
