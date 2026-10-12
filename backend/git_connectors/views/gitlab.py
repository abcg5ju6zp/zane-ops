from datetime import timedelta
import time
from typing import List, cast
from urllib.parse import urlencode, urlparse
import requests
from rest_framework.views import APIView
from rest_framework.generics import RetrieveAPIView
from rest_framework import exceptions
from ..serializers import (
    CreateGitlabAppRequestSerializer,
    CreateGitlabAppResponseSerializer,
    GitlabAppUpdateRequestSerializer,
    GitlabAppUpdateResponseSerializer,
    SetupGitlabAppQuerySerializer,
    GitlabAppSerializer,
    GitlabWebhookEventSerializer,
    GitlabWebhookPushEventRequestSerializer,
    GitlabWebhookEvent,
    GitlabWebhookMergeRequestEventRequestSerializer,
)
from faker import Faker
from drf_spectacular.utils import extend_schema, inline_serializer

from zane_api.views import BadRequest
from django.conf import settings
from temporal.client import StartWorkflowArg, SignalWorkflowArg
from temporal.workflows import (
    CreateEnvNetworkWorkflow,
    DeployDockerServiceWorkflow,
    DeployGitServiceWorkflow,
    DelayedArchiveEnvWorkflow,
    ArchiveEnvWorkflow,
    DeployComposeStackWorkflow,
)

from django.db import transaction
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.serializer_helpers import ReturnDict
from rest_framework import status, serializers
from zane_api.models import (
    GitApp,
    Service,
    Deployment,
    Environment,
    PreviewEnvMetadata,
    CloneEnvPreviewPayload,
)
from ..models import GitlabApp, GitWebhookDelivery, ZERO_COMMIT_SHA
from ..delivery import (
    arbitrate_branch_push,
    arbitrate_pull_request,
    begin_delivery,
    mark_ignored,
    persist_effects_and_schedule,
    repository_is_connected,
    schedule_duplicate_replay,
)
from django.core.cache import cache
from zane_api.utils import generate_random_chars
from rest_framework import permissions
from rest_framework.throttling import ScopedRateThrottle
from temporal.shared import (
    DeploymentDetails,
    CancelDeploymentSignalInput,
    EnvironmentDetails,
    ComposeStackDeploymentDetails,
)
from ..dtos import GitCommitInfo
from ..constants import GITLAB_NULL_COMMIT
from zane_api.permissions import HasWorkspace, IsWorkspaceAdmin, IsWorkspaceMember


class CreateGitlabAppAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    @extend_schema(
        request=CreateGitlabAppRequestSerializer,
        responses={200: CreateGitlabAppResponseSerializer},
        operation_id="createGitlabApp",
        summary="create a gitlab app",
    )
    def post(self, request: Request):
        form = CreateGitlabAppRequestSerializer(data=request.data)
        form.is_valid(raise_exception=True)

        data = cast(ReturnDict, form.data)

        url = urlparse(data["gitlab_url"])

        cache_id = f"{GitlabApp.SETUP_STATE_CACHE_PREFIX}:{generate_random_chars(32)}"
        cache_data = dict(data)
        cache_data["gitlab_url"] = f"{url.scheme}://{url.netloc}"
        cache.set(
            cache_id,
            cache_data,
            timeout=int(timedelta(minutes=10).total_seconds()),
        )

        serializer = CreateGitlabAppResponseSerializer(dict(state=cache_id))
        return Response(data=serializer.data)


class SetupGitlabAppAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    @transaction.atomic()
    @extend_schema(
        parameters=[SetupGitlabAppQuerySerializer],
        responses={303: None},
        operation_id="setupGitlabApp",
        summary="Set a gitlab app",
    )
    def get(self, request: Request):
        form = SetupGitlabAppQuerySerializer(data=request.query_params)
        form.is_valid(raise_exception=True)

        data = cast(ReturnDict, form.data)

        code = data["code"]
        state = data["state"]

        state_data: dict[str, str] = cache.get(state)

        # delete for preventing bad reuse
        cache.delete(state)
        match state:
            case state if isinstance(state, str) and state.startswith(
                GitlabApp.SETUP_STATE_CACHE_PREFIX
            ):
                response = requests.post(
                    f"{state_data['gitlab_url']}/oauth/token",
                    data=dict(
                        client_id=state_data["app_id"],
                        client_secret=state_data["app_secret"],
                        code=code,
                        grant_type="authorization_code",
                        redirect_uri=state_data["redirect_uri"],
                    ),
                )

                if not status.is_success(response.status_code):
                    raise BadRequest("invalid Gitlab app configuration")

                gitlab_token_data = response.json()

                gl_app = GitlabApp.objects.create(
                    gitlab_url=state_data["gitlab_url"],
                    name=state_data["name"],
                    app_id=state_data["app_id"],
                    secret=state_data["app_secret"],
                    redirect_uri=state_data["redirect_uri"],
                    refresh_token=gitlab_token_data["refresh_token"],
                )
                gl_app.fetch_all_repositories_from_gitlab()
                GitApp.objects.create(
                    gitlab=gl_app,
                    workspace=self.request.workspace,  # type: ignore
                )
            case state if isinstance(state, str) and state.startswith(
                GitlabApp.UPDATE_STATE_CACHE_PREFIX
            ):
                try:
                    git_app = (
                        GitApp.objects.filter(gitlab__app_id=state_data["app_id"])
                        .select_related("gitlab")
                        .get()
                    )
                except GitApp.DoesNotExist:
                    raise exceptions.NotFound(
                        "The referenced gitlab app does not exists anymore"
                    )

                gl_app = cast(GitlabApp, git_app.gitlab)
                response = requests.post(
                    f"{gl_app.gitlab_url}/oauth/token",
                    data=dict(
                        client_id=gl_app.app_id,
                        client_secret=state_data["app_secret"],
                        code=code,
                        grant_type="authorization_code",
                        redirect_uri=state_data["redirect_uri"],
                    ),
                )

                if not status.is_success(response.status_code):
                    raise BadRequest("invalid Gitlab app configuration")

                gitlab_token_data = response.json()
                gl_app.refresh_token = gitlab_token_data["refresh_token"]
                gl_app.secret = state_data["app_secret"]
                gl_app.redirect_uri = state_data["redirect_uri"]
                gl_app.save()

                gl_app.fetch_all_repositories_from_gitlab()
            case _:
                raise BadRequest("Invalid state token")

        base_url = ""
        if settings.ENVIRONMENT != settings.PRODUCTION_ENV:
            base_url = "http://localhost:5173"

        return Response(
            headers={"Location": f"{base_url}/workspace/settings/git-apps"},
            status=status.HTTP_303_SEE_OTHER,
        )


class TestGitlabAppAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    @extend_schema(
        responses={
            200: inline_serializer(
                "TestGitlabAppResponseSerializer",
                fields={"repositories_count": serializers.IntegerField()},
            ),
        },
        operation_id="testGitlabApp",
    )
    def get(self, request: Request, id: str):
        try:
            git_app = (
                GitApp.objects.filter(
                    gitlab__id=id,
                    workspace=self.request.workspace,  # type: ignore
                )
                .select_related("gitlab")
                .get()
            )

            gl_app = cast(GitlabApp, git_app.gitlab)
            access_token = GitlabApp.ensure_fresh_access_token(gl_app)
            url = f"{gl_app.gitlab_url}/api/v4/projects"
            params = {
                "membership": "true",
            }
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
            }
            response = requests.get(
                url + "?" + urlencode(params, doseq=True), headers=headers
            )

            response.raise_for_status()

            return Response(
                data={
                    # `x-total`` will not show if there is more than 10000+ repos,
                    #  so we send just 10 001 to signal it
                    "repositories_count": int(response.headers.get("x-total", 10_001)),
                }
            )
        except GitApp.DoesNotExist:
            raise exceptions.NotFound(f"Gitlab app with id {id} does not exist")
        except requests.exceptions.HTTPError as error:
            print(
                error.response.status_code,
                error.response.text,
            )
            raise BadRequest(
                "This GitLab app may not be correctly installed or it has been deleted on GitLab. If you're sure the app is installed in GitLab, edit the app to renew the app secret."
            )


class SyncRepositoriesAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    @transaction.atomic()
    @extend_schema(
        request=None,
        responses={
            200: inline_serializer(
                "SyncGitlabRepositoriesResponseSerializer",
                fields={"repositories_count": serializers.IntegerField()},
            ),
        },
        operation_id="syncGitlabRepos",
        summary="Sync GitLab repositories for a GitLab application",
    )
    def put(self, request: Request, id: str):
        try:
            gitapp = (
                GitApp.objects.filter(
                    gitlab__id=id,
                    workspace=self.request.workspace,  # type: ignore
                )
                .select_related("gitlab")
                .get()
            )
        except GitApp.DoesNotExist:
            raise exceptions.NotFound(
                "The referenced gitlab app does not exists on ZaneOps"
            )

        gitlab = cast(GitlabApp, gitapp.gitlab)
        gitlab.fetch_all_repositories_from_gitlab()
        return Response(
            data={
                "repositories_count": gitlab.repositories.count(),
            }
        )


class GitlabAppDetailsAPIView(RetrieveAPIView):
    serializer_class = GitlabAppSerializer
    lookup_field = "id"
    queryset = GitlabApp.objects.all()
    permission_classes = [HasWorkspace, IsWorkspaceMember]

    def get_queryset(self):
        return super().get_queryset().filter(gitapp__workspace=self.request.workspace)


class GitlabAppUpdateAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    @transaction.atomic()
    @extend_schema(
        request=GitlabAppUpdateRequestSerializer,
        responses={200: GitlabAppUpdateResponseSerializer},
    )
    def put(self, request: Request, id: str):
        try:
            git_app = (
                GitApp.objects.filter(
                    gitlab__id=id,
                    workspace=self.request.workspace,  # type: ignore
                )
                .select_related("gitlab")
                .get()
            )
        except GitApp.DoesNotExist:
            raise exceptions.NotFound(f"Gitlab app with id {id} does not exist")

        form = GitlabAppUpdateRequestSerializer(data=request.data)
        form.is_valid(raise_exception=True)

        data = cast(ReturnDict, form.data)

        gl_app = cast(GitlabApp, git_app.gitlab)
        gl_app.name = data["name"]
        gl_app.save()

        cache_id = f"{GitlabApp.UPDATE_STATE_CACHE_PREFIX}:{generate_random_chars(32)}"
        # Use provided app_secret if available and not empty, otherwise use existing secret
        app_secret = data.get("app_secret", gl_app.secret)
        cache_data = dict(
            app_id=gl_app.app_id,
            app_secret=app_secret,
            redirect_uri=data["redirect_uri"],
        )
        cache.set(
            cache_id,
            cache_data,
            timeout=int(timedelta(minutes=10).total_seconds()),
        )

        serializer = GitlabAppUpdateResponseSerializer(dict(state=cache_id))
        return Response(data=serializer.data)


@extend_schema(exclude=True)
class GitlabWebhookAPIView(APIView):
    permission_classes = [permissions.AllowAny]
    throttle_classes = [ScopedRateThrottle]
    throttle_scope = "gitapp_webhook"

    @transaction.atomic()
    def post(self, request: Request):
        request_body: bytes = request.body
        form = GitlabWebhookEventSerializer(
            data={
                "event": request.headers.get("x-gitlab-event"),
                "webhook_secret": request.headers.get("x-gitlab-token"),
            }
        )
        # unsupported events are rejected here, before any state is created
        form.is_valid(raise_exception=True)

        event = cast(ReturnDict, form.data)["event"]
        webhook_secret = cast(ReturnDict, form.data)["webhook_secret"]

        event_serializer_map = {
            GitlabWebhookEvent.PUSH: GitlabWebhookPushEventRequestSerializer,
            GitlabWebhookEvent.MERGE_REQUEST: GitlabWebhookMergeRequestEventRequestSerializer,
        }

        serializer_class = event_serializer_map[event]
        body = request.data

        form = serializer_class(data=body)
        form.is_valid(raise_exception=True)
        data = cast(ReturnDict, form.data)

        # token verification resolves the app identity BEFORE the event is
        # ledgered: invalid deliveries never create repository/deployment
        # state.
        try:
            gitapp = (
                GitApp.objects.filter(gitlab__webhook_secret=webhook_secret)
                .select_related("gitlab")
                .get()
            )
        except GitApp.DoesNotExist:
            raise exceptions.NotFound("Invalid webhook secret")

        gitlab = cast(GitlabApp, gitapp.gitlab)

        if event == GitlabWebhookEvent.PUSH:
            summary_repository_url = data["repository"]["git_http_url"]
            push_ref: str = data["ref"]
            summary_ref_name = (
                push_ref.replace("refs/heads/", "")
                if push_ref.startswith("refs/heads/")
                else ""
            )
            summary_head_sha = (
                None if data["after"] == GITLAB_NULL_COMMIT else data["after"]
            )
            summary_pr_number = None
            summary_action = None
        else:
            merge_request_summary = data["object_attributes"]
            summary_repository_url = merge_request_summary["source"]["git_http_url"]
            summary_ref_name = merge_request_summary["source_branch"]
            last_commit = merge_request_summary.get("last_commit")
            summary_head_sha = last_commit["id"] if last_commit else None
            summary_pr_number = merge_request_summary["iid"]
            summary_action = merge_request_summary["action"]

        delivery, created = begin_delivery(
            provider=GitWebhookDelivery.Provider.GITLAB,
            git_app_id=gitapp.id,
            event_type=event,
            action=summary_action,
            raw_body=request_body,
            # GitLab has no provider delivery id, the verified payload digest
            # is the stable identity.
            provider_delivery_id=None,
            repository_url=summary_repository_url,
            ref_name=summary_ref_name,
            head_sha=summary_head_sha,
            pr_number=summary_pr_number,
        )
        if not created:
            # duplicate delivery: return the original accepted result and
            # continue any unconfirmed workflow start from the first delivery.
            schedule_duplicate_replay(delivery)
            return Response(data={"success": True})

        workflows_to_run: List[StartWorkflowArg] = []
        workflows_signals: List[SignalWorkflowArg] = []
        delivery_ignored = False

        match form:
            case GitlabWebhookPushEventRequestSerializer():
                head_commit = data["commits"][-1] if len(data["commits"]) > 0 else None
                ref: str = data["ref"]
                is_branch_deleted = (
                    data["checkout_sha"] is None and data["after"] == GITLAB_NULL_COMMIT
                )

                # We only consider pushes to a branch
                # we ignore tags and other push events
                if ref.startswith("refs/heads/"):
                    head_branch_name = ref.replace("refs/heads/", "")
                    repository_url = data["repository"]["git_http_url"]

                    if not repository_is_connected(gitapp, repository_url):
                        # repository no longer attached to this app: the event
                        # is late and must not deploy anything.
                        delivery_ignored = True
                    elif is_branch_deleted:
                        arbitration = arbitrate_branch_push(
                            delivery=delivery,
                            git_app_id=gitapp.id,
                            repository_url=repository_url,
                            branch_name=head_branch_name,
                            before=data.get("before"),
                            after=GITLAB_NULL_COMMIT,
                            deleted=True,
                        )
                        if not arbitration.proceed:
                            delivery_ignored = True
                        else:
                            matching_preview_envs = Environment.objects.filter(
                                is_preview=True,
                                preview_metadata__source_trigger=Environment.PreviewSourceTrigger.API,
                                preview_metadata__head_repository_url=repository_url,
                                preview_metadata__git_app=gitapp,
                                preview_metadata__branch_name=head_branch_name,
                                preview_metadata__auto_teardown=True,
                            ).select_related("project", "preview_metadata")
                            for environment in matching_preview_envs:
                                workflows_to_run.append(
                                    StartWorkflowArg(
                                        workflow=ArchiveEnvWorkflow.run,
                                        payload=EnvironmentDetails.from_environment(
                                            environment
                                        ),
                                        workflow_id=environment.archive_workflow_id,
                                    )
                                )
                                environment.delete_resources()
                                environment.delete()
                    else:
                        head_sha = data["after"]
                        arbitration = arbitrate_branch_push(
                            delivery=delivery,
                            git_app_id=gitapp.id,
                            repository_url=repository_url,
                            branch_name=head_branch_name,
                            before=(data.get("before") or ZERO_COMMIT_SHA),
                            after=head_sha,
                        )
                        if not arbitration.proceed:
                            # older / out-of-order generation: never roll a
                            # service back to an older commit
                            delivery_ignored = True
                        else:
                            affected_services = (
                                Service.get_services_triggered_by_push_event(
                                    gitapp=gitapp,
                                    branch_name=head_branch_name,
                                    repository_url=repository_url,
                                )
                            )

                            deployments_to_cancel: list[Deployment] = []
                            changed_paths: set[str] = set()
                            for commit in data["commits"]:
                                changed_paths.update(
                                    commit["added"],
                                    commit["removed"],
                                    commit["modified"],
                                )
                            for service in affected_services:
                                # ignore service that don't match the paths
                                if not service.match_paths(changed_paths):
                                    continue

                                # idempotency guard: an auto deployment for
                                # this exact commit already exists (concurrent
                                # duplicate delivery)
                                if Deployment.objects.filter(
                                    service=service,
                                    commit_sha=head_sha,
                                    trigger_method=Deployment.DeploymentTriggerMethod.AUTO,
                                ).exists():
                                    continue

                                if service.cleanup_queue_on_auto_deploy:
                                    deployments_to_cancel.extend(
                                        Deployment.flag_deployments_for_cancellation(
                                            service,
                                            include_running_deployments=True,
                                        )
                                    )

                                commit = (
                                    GitCommitInfo(
                                        sha=head_commit["id"],
                                        message=head_commit["message"],
                                        author_name=head_commit["author"]["name"],
                                    )
                                    if head_commit is not None
                                    else None
                                )
                                new_deployment = service.prepare_new_git_deployment(
                                    commit=commit,
                                    trigger_method=Deployment.DeploymentTriggerMethod.AUTO,
                                )

                                payload = DeploymentDetails.from_deployment(
                                    deployment=new_deployment
                                )
                                workflows_to_run.append(
                                    StartWorkflowArg(
                                        workflow=DeployGitServiceWorkflow.run,
                                        payload=payload,
                                        workflow_id=payload.workflow_id,
                                    )
                                )

                            for dpl in deployments_to_cancel:
                                workflows_signals.append(
                                    SignalWorkflowArg(
                                        workflow=DeployGitServiceWorkflow.run,
                                        input=CancelDeploymentSignalInput(
                                            deployment_hash=dpl.hash
                                        ),
                                        signal=DeployGitServiceWorkflow.cancel_deployment,  # type: ignore
                                        workflow_id=dpl.workflow_id,
                                    )
                                )

            case GitlabWebhookMergeRequestEventRequestSerializer():
                merge_request = data["object_attributes"]
                base_repository_url = merge_request["target"]["git_http_url"]
                head_repository_url = merge_request["source"]["git_http_url"]
                head_branch_name = merge_request["source_branch"]
                base_branch_name = merge_request["target_branch"]
                mr_project_id = data["project"]["id"]

                is_fork = base_repository_url != head_repository_url

                mr_head_sha = (
                    merge_request["last_commit"]["id"]
                    if merge_request.get("last_commit")
                    else None
                )
                mr_action = merge_request["action"]
                # the repository attached to the app is always the target
                # repository (fork source repositories are never installed)
                mr_gate_repository_url = base_repository_url
                # ref generation arbitration decides whether this merge
                # request event is allowed to publish for the current head.
                if not repository_is_connected(gitapp, mr_gate_repository_url):
                    delivery_ignored = True
                elif mr_action == "open":
                    arbitration = arbitrate_pull_request(
                        delivery=delivery,
                        git_app_id=gitapp.id,
                        repository_url=head_repository_url,
                        branch_name=head_branch_name,
                        pr_number=merge_request["iid"],
                        head_sha=mr_head_sha,
                        mode="open",
                    )
                    if not arbitration.proceed:
                        delivery_ignored = True
                elif mr_action == "close" or mr_action == "merge":
                    arbitrate_pull_request(
                        delivery=delivery,
                        git_app_id=gitapp.id,
                        repository_url=head_repository_url,
                        branch_name=head_branch_name,
                        pr_number=merge_request["iid"],
                        head_sha=mr_head_sha,
                        mode="close",
                    )
                # "update" events are arbitrated below because they mix a
                # metadata edit with an optional new-push deploy.
                mr_publish_allowed = not delivery_ignored

                match merge_request["action"]:
                    case "open" if not mr_publish_allowed:
                        # late open for a closed/removed pull request
                        pass
                    case "open":
                        affected_services = (
                            Service.get_services_triggered_by_pull_request_event(
                                gitapp=gitapp,
                                repository_url=base_repository_url,
                            )
                        )

                        for current_service in affected_services:
                            existing_preview_envs_count = (
                                Environment.objects.filter(
                                    is_preview=True,
                                    preview_metadata__source_trigger=Environment.PreviewSourceTrigger.PULL_REQUEST,
                                    preview_metadata__head_repository_url=head_repository_url,
                                    preview_metadata__service=current_service,
                                    preview_metadata__git_app=gitapp,
                                    preview_metadata__pr_number=merge_request["iid"],
                                )
                                .select_related("project", "preview_metadata")
                                .count()
                            )

                            if existing_preview_envs_count > 0:
                                # ignored because the env already exist
                                continue

                            project = current_service.project
                            preview_template = project.default_preview_template

                            total_preview_env_for_template = (
                                project.environments.filter(
                                    is_preview=True,
                                    preview_metadata__template=preview_template,
                                ).count()
                            )
                            if (
                                total_preview_env_for_template
                                == preview_template.preview_env_limit
                            ):
                                continue  # ignore if we get to the limit of max previews

                            fake = Faker()
                            Faker.seed(time.monotonic())
                            env_name = f"preview-mr-{merge_request['iid']}-{current_service.slug}-{fake.slug()}".lower()
                            preview_meta = PreviewEnvMetadata.objects.create(
                                branch_name=head_branch_name,
                                source_trigger=Environment.PreviewSourceTrigger.PULL_REQUEST,
                                service=current_service,
                                template=preview_template,
                                auto_teardown=preview_template.auto_teardown,
                                external_url=merge_request["url"],
                                git_app=gitapp,
                                head_repository_url=head_repository_url,
                                ttl_seconds=preview_template.ttl_seconds,
                                auth_enabled=preview_template.auth_enabled,
                                auth_user=preview_template.auth_user,
                                pr_author=data["user"]["username"],
                                pr_base_repo_url=base_repository_url,
                                pr_base_branch_name=base_branch_name,
                                auth_password=preview_template.auth_password,
                                deploy_state=(
                                    PreviewEnvMetadata.PreviewDeployState.PENDING
                                    if is_fork
                                    else PreviewEnvMetadata.PreviewDeployState.APPROVED
                                ),
                                pr_number=merge_request["iid"],
                                pr_title=merge_request["title"],
                            )

                            base_environment = cast(
                                Environment, preview_template.base_environment
                            )

                            new_environment = base_environment.clone(
                                env_name=env_name,
                                preview_data=CloneEnvPreviewPayload(
                                    template=preview_template, metadata=preview_meta
                                ),
                            )

                            if is_fork:
                                cloned_service = new_environment.services.get(
                                    slug=current_service.slug
                                )
                                # 1️⃣ Define the API endpoint for creating a comment
                                issue_number = merge_request[
                                    "iid"
                                ]  # issue or PR number

                                # create issue comment

                                url = f"{gitlab.gitlab_url}/api/v4/projects/{mr_project_id}/merge_requests/{issue_number}/notes"

                                # 2️⃣ Prepare the request
                                headers = {
                                    "Authorization": f"Bearer {GitlabApp.ensure_fresh_access_token(gitlab)}",
                                    "Accept": "application/vnd.github+json",
                                }
                                payload = {
                                    "body": preview_meta.get_pull_request_deployment_blocked_comment_body(
                                        cloned_service
                                    )
                                }

                                # 3️⃣ Make the POST request
                                response = requests.post(
                                    url, headers=headers, json=payload
                                )
                                # 4️⃣ Check the response
                                if response.status_code == status.HTTP_201_CREATED:
                                    comment_data = response.json()
                                    print(
                                        "Comment created:",
                                        url + f"/{comment_data['id']}",
                                    )
                                    print("Comment Body:\n", comment_data["body"])

                                    # Update Preview metadata with the comment ID
                                    preview_meta.pr_comment_id = comment_data["id"]
                                    preview_meta.save()
                                else:
                                    print(
                                        f"Error when trying to create a PR comment for the {preview_meta.service=} on the PR #{merge_request['iid']}({merge_request['url']}): ",
                                        response.status_code,
                                        response.text,
                                    )

                            else:
                                workflows_to_run.append(
                                    StartWorkflowArg(
                                        workflow=CreateEnvNetworkWorkflow.run,
                                        payload=EnvironmentDetails(
                                            id=new_environment.id,
                                            project_id=project.id,
                                            name=new_environment.name,
                                        ),
                                        workflow_id=new_environment.workflow_id,
                                    )
                                )

                                for stack in new_environment.compose_stacks.all():
                                    deployment = stack.deployments.create(
                                        commit_message="Deploy from pull request",
                                    )
                                    stack.apply_pending_changes(deployment)

                                    deployment.stack_snapshot = stack.snapshot.to_dict()  # type: ignore
                                    deployment.save()

                                    payload = (
                                        ComposeStackDeploymentDetails.from_deployment(
                                            deployment
                                        )
                                    )
                                    workflows_to_run.append(
                                        StartWorkflowArg(
                                            DeployComposeStackWorkflow.run,
                                            payload,
                                            payload.workflow_id,
                                        )
                                    )

                                for service in new_environment.services.all():
                                    if (
                                        service.type
                                        == Service.ServiceType.DOCKER_REGISTRY
                                    ):
                                        new_deployment = service.prepare_new_docker_deployment(
                                            trigger_method=Deployment.DeploymentTriggerMethod.AUTO
                                        )
                                    else:
                                        new_deployment = service.prepare_new_git_deployment(
                                            trigger_method=Deployment.DeploymentTriggerMethod.AUTO
                                        )

                                    payload = DeploymentDetails.from_deployment(
                                        deployment=new_deployment
                                    )
                                    workflows_to_run.append(
                                        StartWorkflowArg(
                                            workflow=(
                                                DeployDockerServiceWorkflow.run
                                                if service.type
                                                == Service.ServiceType.DOCKER_REGISTRY
                                                else DeployGitServiceWorkflow.run
                                            ),
                                            payload=payload,
                                            workflow_id=payload.workflow_id,
                                        )
                                    )

                                    if current_service.slug == service.slug:
                                        # 1️⃣ Define the API endpoint for creating a comment
                                        issue_number = merge_request[
                                            "iid"
                                        ]  # issue or PR number

                                        # create issue comment

                                        url = f"{gitlab.gitlab_url}/api/v4/projects/{mr_project_id}/merge_requests/{issue_number}/notes"

                                        # 2️⃣ Prepare the request
                                        headers = {
                                            "Authorization": f"Bearer {GitlabApp.ensure_fresh_access_token(gitlab)}",
                                            "Accept": "application/vnd.github+json",
                                        }
                                        payload = {
                                            "body": new_deployment.get_pull_request_deployment_comment_body()
                                        }

                                        # 3️⃣ Make the POST request
                                        response = requests.post(
                                            url, headers=headers, json=payload
                                        )
                                        # 4️⃣ Check the response
                                        if (
                                            response.status_code
                                            == status.HTTP_201_CREATED
                                        ):
                                            comment_data = response.json()
                                            print(
                                                "Comment created:",
                                                url + f"/{comment_data['id']}",
                                            )
                                            print(
                                                "Comment Body:\n",
                                                comment_data["body"],
                                            )

                                            # Update Preview metadata with the comment ID
                                            preview_meta.pr_comment_id = comment_data[
                                                "id"
                                            ]
                                            preview_meta.save()
                                        else:
                                            print(
                                                f"Error when trying to create a MR comment for the {service=} on the MR #{issue_number}({merge_request['url']}): ",
                                                response.status_code,
                                                response.text,
                                            )

                                if preview_template.ttl_seconds is not None:
                                    workflows_to_run.append(
                                        StartWorkflowArg(
                                            workflow=DelayedArchiveEnvWorkflow.run,
                                            payload=EnvironmentDetails(
                                                id=new_environment.id,
                                                project_id=new_environment.project.id,
                                                name=new_environment.name,
                                            ),
                                            workflow_id=new_environment.delayed_archive_workflow_id,
                                            start_delay=timedelta(
                                                seconds=preview_template.ttl_seconds
                                            ),
                                        )
                                    )
                    case "update" if merge_request["state"] == "opened":
                        matching_preview_envs = Environment.objects.filter(
                            is_preview=True,
                            preview_metadata__source_trigger=Environment.PreviewSourceTrigger.PULL_REQUEST,
                            preview_metadata__head_repository_url=head_repository_url,
                            preview_metadata__git_app=gitapp,
                            preview_metadata__branch_name=head_branch_name,
                            preview_metadata__auto_teardown=True,
                            preview_metadata__pr_number=merge_request["iid"],
                        ).select_related("project", "preview_metadata")

                        for environment in matching_preview_envs:
                            preview_metadata = cast(
                                PreviewEnvMetadata, environment.preview_metadata
                            )
                            preview_metadata.pr_title = merge_request["title"]
                            preview_metadata.pr_base_branch_name = base_branch_name
                            preview_metadata.save()

                        # if there was a push on the branch of the merge request
                        is_push_action = merge_request.get("oldrev") is not None
                        if is_push_action:
                            # arbitrate the new commit against the current MR
                            # generation before deploying preview services
                            sync_arbitration = arbitrate_pull_request(
                                delivery=delivery,
                                git_app_id=gitapp.id,
                                repository_url=head_repository_url,
                                branch_name=head_branch_name,
                                pr_number=merge_request["iid"],
                                head_sha=mr_head_sha,
                                mode="sync",
                                oldrev=merge_request.get("oldrev"),
                            )
                            if not sync_arbitration.proceed:
                                delivery_ignored = True
                            else:
                                affected_services = Service.get_services_triggered_by_pull_request_sync_event(
                                    gitapp=gitapp,
                                    repository_url=head_repository_url,
                                    pr_number=merge_request["iid"],
                                )

                                deployments_to_cancel: list[Deployment] = []
                                for service in affected_services:
                                    if service.cleanup_queue_on_auto_deploy:
                                        deployments_to_cancel.extend(
                                            Deployment.flag_deployments_for_cancellation(
                                                service,
                                                include_running_deployments=True,
                                            )
                                        )
                                    new_deployment = service.prepare_new_git_deployment(
                                        trigger_method=Deployment.DeploymentTriggerMethod.AUTO,
                                    )

                                    payload = DeploymentDetails.from_deployment(
                                        deployment=new_deployment
                                    )
                                    workflows_to_run.append(
                                        StartWorkflowArg(
                                            workflow=DeployGitServiceWorkflow.run,
                                            payload=payload,
                                            workflow_id=payload.workflow_id,
                                        )
                                    )

                                for dpl in deployments_to_cancel:
                                    workflows_signals.append(
                                        SignalWorkflowArg(
                                            workflow=DeployGitServiceWorkflow.run,
                                            input=CancelDeploymentSignalInput(
                                                deployment_hash=dpl.hash
                                            ),
                                            signal=DeployGitServiceWorkflow.cancel_deployment,  # type: ignore
                                            workflow_id=dpl.workflow_id,
                                        )
                                    )
                    case "close" | "merge" if not mr_publish_allowed:
                        # repository detached: late close/merge event
                        pass
                    case "close" | "merge":
                        matching_preview_envs = Environment.objects.filter(
                            is_preview=True,
                            preview_metadata__source_trigger=Environment.PreviewSourceTrigger.PULL_REQUEST,
                            preview_metadata__head_repository_url=head_repository_url,
                            preview_metadata__git_app=gitapp,
                            preview_metadata__branch_name=head_branch_name,
                            preview_metadata__auto_teardown=True,
                            preview_metadata__pr_number=merge_request["iid"],
                        ).select_related("project", "preview_metadata")

                        for environment in matching_preview_envs:
                            workflows_to_run.append(
                                StartWorkflowArg(
                                    workflow=ArchiveEnvWorkflow.run,
                                    payload=EnvironmentDetails.from_environment(
                                        environment
                                    ),
                                    workflow_id=environment.archive_workflow_id,
                                )
                            )
                            environment.delete_resources()
                            environment.delete()
                    case _:
                        # no need to implement other cases
                        pass

            case _:
                raise BadRequest("bad request")

        if delivery_ignored:
            # valid but superseded by a newer ref generation: acknowledge so
            # the provider stops retrying, but create no deployment/preview.
            mark_ignored(delivery)
        else:
            persist_effects_and_schedule(delivery, workflows_to_run, workflows_signals)

        return Response(data={"success": True})
