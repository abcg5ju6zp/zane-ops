from datetime import timedelta
import time
from typing import List, cast
from faker import Faker
import requests
from rest_framework.views import APIView
from rest_framework.generics import RetrieveUpdateAPIView
from rest_framework import exceptions, permissions
from rest_framework.throttling import ScopedRateThrottle
from ..serializers import (
    GithubWebhookEventSerializer,
    SetupGithubAppQuerySerializer,
    GithubAppSerializer,
    GithubWebhookPingRequestSerializer,
    GithubWebhookInstallationRequestSerializer,
    GithubWebhookEvent,
    GithubWebhookInstallationRepositoriesRequestSerializer,
    GithubWebhookPushRequestSerializer,
    GithubWebhookPullRequestSerializer,
)
from drf_spectacular.utils import extend_schema, inline_serializer

from zane_api.views import BadRequest
from django.conf import settings

from django.db import transaction
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.serializer_helpers import ReturnDict
from rest_framework import status, serializers
from zane_api.models import (
    GitApp,
    Service,
    Environment,
    Deployment,
    PreviewEnvMetadata,
    CloneEnvPreviewPayload,
)
from ..models import GitHubApp, GitRepository, GitWebhookDelivery, ZERO_COMMIT_SHA
from ..delivery import (
    arbitrate_branch_push,
    arbitrate_pull_request,
    begin_delivery,
    mark_ignored,
    persist_effects_and_schedule,
    repository_is_connected,
    schedule_duplicate_replay,
)
from temporal.shared import (
    DeploymentDetails,
    EnvironmentDetails,
    ComposeStackDeploymentDetails,
)
from temporal.client import StartWorkflowArg, SignalWorkflowArg
from temporal.workflows import (
    DeployGitServiceWorkflow,
    CancelDeploymentSignalInput,
    ArchiveEnvWorkflow,
    CreateEnvNetworkWorkflow,
    DeployDockerServiceWorkflow,
    DelayedArchiveEnvWorkflow,
    DeployComposeStackWorkflow,
)
from ..dtos import GitCommitInfo
from zane_api.permissions import (
    HasWorkspace,
    IsWorkspaceAdmin,
)


class SetupGithubAppAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    @transaction.atomic()
    @extend_schema(
        responses={status.HTTP_303_SEE_OTHER: None},
        operation_id="setupGithubApp",
        summary="setup github app",
        parameters=[SetupGithubAppQuerySerializer],
    )
    def get(self, request: Request):
        form = SetupGithubAppQuerySerializer(data=request.query_params)
        form.is_valid(raise_exception=True)

        data = cast(ReturnDict, form.data)

        code = data["code"]
        state = data["state"]
        match state:
            case state if isinstance(state, str) and state.startswith("install"):
                _, app_id = state.split(":")
                installation_id: int = data["installation_id"]

                try:
                    git_app = (
                        GitApp.objects.filter(
                            github__id=app_id,
                            workspace=self.request.workspace,  # type: ignore
                        )
                        .select_related("github")
                        .get()
                    )
                except GitApp.DoesNotExist:
                    raise exceptions.NotFound(
                        f"Github app with id {app_id} does not exist"
                    )

                gh_app: GitHubApp = cast(GitHubApp, git_app.github)
                gh_app.installation_id = installation_id
                gh_app.save()

            case "create":
                url = f"https://api.github.com/app-manifests/{code}/conversions"
                headers = {
                    "Accept": "application/json",
                    "X-GitHub-Api-Version": "2022-11-28",
                }
                response = requests.post(url, headers=headers)

                if not status.is_success(response.status_code):
                    raise BadRequest("invalid Github app installation code")

                github_manifest_data = response.json()

                github_app = GitHubApp.objects.filter(
                    app_id=github_manifest_data["id"]
                ).first()

                if github_app is None:
                    github_app = GitHubApp.objects.create(
                        app_id=github_manifest_data["id"],
                        client_id=github_manifest_data["client_id"],
                        client_secret=github_manifest_data["client_secret"],
                        webhook_secret=github_manifest_data["webhook_secret"],
                        app_url=github_manifest_data["html_url"],
                        private_key=github_manifest_data["pem"],
                        name=github_manifest_data["name"],
                    )

                git_app, _ = GitApp.objects.get_or_create(
                    github=github_app,
                    workspace=self.request.workspace,  # type: ignore
                )
            case _:
                raise BadRequest("Invalid state token")

        base_url = ""
        if settings.ENVIRONMENT != settings.PRODUCTION_ENV:
            base_url = "http://localhost:5173"

        return Response(
            headers={"Location": f"{base_url}/workspace/settings/git-apps"},
            status=status.HTTP_303_SEE_OTHER,
        )


class GithubAppDetailsAPIView(RetrieveUpdateAPIView):
    serializer_class = GithubAppSerializer
    queryset = GitHubApp.objects.all()
    lookup_field = "id"
    http_method_names = ["patch", "get"]
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    def get_queryset(self):
        return super().get_queryset().filter(gitapp__workspace=self.request.workspace)


class TestGithubAppAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]

    @extend_schema(
        responses={
            200: inline_serializer(
                "TestGithubAppResponseSerializer",
                fields={"repositories_count": serializers.IntegerField()},
            ),
        },
        operation_id="testGithubApp",
    )
    def get(self, request: Request, id: str):
        try:
            git_app = (
                GitApp.objects.filter(
                    github__id=id,
                    workspace=self.request.workspace,  # type: ignore
                )
                .select_related("github")
                .get()
            )
        except GitApp.DoesNotExist:
            raise exceptions.NotFound(f"Github app with id {id} does not exist")

        github_app: GitHubApp = git_app.github  # type: ignore
        access_token = github_app.get_access_token()
        url = "https://api.github.com/installation/repositories"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        response = requests.get(url, headers=headers)
        if not status.is_success(response.status_code):
            raise BadRequest(
                "This github app may not be correctly installed or it has been deleted on github"
            )

        result = response.json()

        return Response(
            data={
                "repositories_count": result["total_count"],
            }
        )


@extend_schema(exclude=True)
class GithubWebhookAPIView(APIView):
    permission_classes = [permissions.AllowAny]
    throttle_classes = [ScopedRateThrottle]
    throttle_scope = "gitapp_webhook"

    @staticmethod
    def _resolve_gitapp(event: str, data: ReturnDict) -> GitApp:
        # resolve the installation/app identity BEFORE signature verification
        # and before any event state is written.
        if event == GithubWebhookEvent.PING:
            queryset = GitApp.objects.filter(github__app_id=data["hook"]["app_id"])
        elif event in (
            GithubWebhookEvent.INSTALLATION,
            GithubWebhookEvent.INSTALLATION_REPOS,
        ):
            queryset = GitApp.objects.filter(
                github__app_id=data["installation"]["app_id"]
            )
        else:
            queryset = GitApp.objects.filter(
                github__installation_id=data["installation"]["id"]
            )
        try:
            return queryset.select_related("github", "gitlab").get()
        except GitApp.DoesNotExist:
            raise exceptions.NotFound(
                "This github app has not been registered in this ZaneOps instance"
            )

    @staticmethod
    def _summarize_event(
        event: str, data: ReturnDict
    ) -> tuple[str, str, str | None, int | None]:
        """Return (repository_url, ref_name, head_sha, pr_number) for an event."""
        if event == GithubWebhookEvent.PUSH:
            ref: str = data["ref"]
            branch_name = (
                ref.replace("refs/heads/", "") if ref.startswith("refs/heads/") else ""
            )
            repository_url = f"https://github.com/{data['repository']['full_name']}.git"
            head_commit = data["head_commit"]
            head_sha = data.get("after") or (
                head_commit["id"] if head_commit is not None else None
            )
            return repository_url, branch_name, head_sha or None, None
        if event == GithubWebhookEvent.PULL_REQUEST:
            pull_request = data["pull_request"]
            return (
                f"https://github.com/{pull_request['head']['repo']['full_name']}.git",
                pull_request["head"]["ref"],
                pull_request["head"]["sha"],
                pull_request["number"],
            )
        return "", "", None, None

    @transaction.atomic()
    def post(self, request: Request):
        request_body: bytes = request.body
        request_data = request.data
        form = GithubWebhookEventSerializer(
            data={
                "event": request.headers.get("x-github-event"),
                "signature256": request.headers.get("x-hub-signature-256"),
                "delivery_id": request.headers.get("x-github-delivery"),
            }
        )
        # unsupported events are rejected here, before any state is created
        form.is_valid(raise_exception=True)
        event = cast(ReturnDict, form.data)["event"]
        signature = cast(ReturnDict, form.data)["signature256"]
        provider_delivery_id = cast(ReturnDict, form.data).get("delivery_id")

        event_serializer_map = {
            GithubWebhookEvent.PING: GithubWebhookPingRequestSerializer,
            GithubWebhookEvent.INSTALLATION: GithubWebhookInstallationRequestSerializer,
            GithubWebhookEvent.INSTALLATION_REPOS: GithubWebhookInstallationRepositoriesRequestSerializer,
            GithubWebhookEvent.PUSH: GithubWebhookPushRequestSerializer,
            GithubWebhookEvent.PULL_REQUEST: GithubWebhookPullRequestSerializer,
        }

        serializer_class = event_serializer_map[event]
        form = serializer_class(data=request_data)
        form.is_valid(raise_exception=True)
        data = cast(ReturnDict, form.data)

        # identity resolution + signature verification happen BEFORE the event
        # is ledgered: invalid deliveries never create repository/deployment
        # state.
        gitapp = self._resolve_gitapp(event, data)
        github = cast(GitHubApp, gitapp.github)
        verified = github.verify_signature(
            payload_body=request_body,
            signature_header=signature,
        )
        if not verified:
            raise BadRequest("Invalid webhook signature")

        repository_url, ref_name, head_sha, pr_number = self._summarize_event(
            event, data
        )
        delivery, created = begin_delivery(
            provider=GitWebhookDelivery.Provider.GITHUB,
            git_app_id=gitapp.id,
            event_type=event,
            action=data.get("action"),
            raw_body=request_body,
            provider_delivery_id=provider_delivery_id,
            repository_url=repository_url,
            ref_name=ref_name,
            head_sha=head_sha,
            pr_number=pr_number,
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
            case GithubWebhookPingRequestSerializer():
                pass
            case GithubWebhookInstallationRequestSerializer():
                repositories = data["repositories"]

                def map_repository(repository: dict[str, str]):
                    return GitRepository(
                        path=repository["full_name"],
                        url=f"https://github.com/{repository['full_name']}.git",
                        private=repository["private"],
                    )

                mapped = [map_repository(repo) for repo in repositories]
                github.add_repositories(mapped)
            case GithubWebhookInstallationRepositoriesRequestSerializer():
                repositories_added = data["repositories_added"]
                repositories_removed = data["repositories_removed"]

                if len(repositories_added) > 0:

                    def map_repository(repository: dict[str, str]):
                        return GitRepository(
                            path=repository["full_name"],
                            url=f"https://github.com/{repository['full_name']}.git",
                            private=repository["private"],
                        )

                    mapped = [map_repository(repo) for repo in repositories_added]
                    github.add_repositories(mapped)
                if len(repositories_removed) > 0:
                    repos_to_delete = github.repositories.filter(
                        url__in=[
                            f"https://github.com/{repo['full_name']}.git"
                            for repo in repositories_removed
                        ]
                    )
                    # detach the relations between the repos and this app
                    github.repositories.remove(*repos_to_delete)

                    # cleanup orphan repositories
                    GitRepository.objects.filter(
                        gitlabapps__isnull=True, githubapps__isnull=True
                    ).delete()
            case GithubWebhookPushRequestSerializer():
                ref: str = data["ref"]
                head_commit: dict | None = data["head_commit"]
                branch_deleted = data["deleted"]
                # We only consider pushes to a branch
                # we ignore tags and other push events
                if ref.startswith("refs/heads/"):
                    branch_name = ref.replace("refs/heads/", "")
                    repository_url = (
                        f"https://github.com/{data['repository']['full_name']}.git"
                    )

                    if not repository_is_connected(gitapp, repository_url):
                        # repository removed from the installation: the event
                        # is late and must not deploy anything.
                        delivery_ignored = True
                    elif branch_deleted:
                        arbitration = arbitrate_branch_push(
                            delivery=delivery,
                            git_app_id=gitapp.id,
                            repository_url=repository_url,
                            branch_name=branch_name,
                            before=data.get("before"),
                            after=data.get("after") or ZERO_COMMIT_SHA,
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
                                preview_metadata__branch_name=branch_name,
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
                        head_sha = data.get("after") or (
                            head_commit["id"] if head_commit is not None else None
                        )
                        arbitration = arbitrate_branch_push(
                            delivery=delivery,
                            git_app_id=gitapp.id,
                            repository_url=repository_url,
                            branch_name=branch_name,
                            before=(data.get("before") or ZERO_COMMIT_SHA),
                            after=head_sha or ZERO_COMMIT_SHA,
                            forced=data["forced"],
                        )
                        if not arbitration.proceed:
                            # older / out-of-order generation: never roll a
                            # service back to an older commit
                            delivery_ignored = True
                        else:
                            affected_services = (
                                Service.get_services_triggered_by_push_event(
                                    gitapp=gitapp,
                                    branch_name=branch_name,
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
                                if (
                                    head_sha is not None
                                    and Deployment.objects.filter(
                                        service=service,
                                        commit_sha=head_sha,
                                        trigger_method=Deployment.DeploymentTriggerMethod.AUTO,
                                    ).exists()
                                ):
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

            case GithubWebhookPullRequestSerializer():
                pull_request = data["pull_request"]
                branch_name = pull_request["head"]["ref"]
                is_fork = pull_request["head"]["repo"]["fork"]
                pr_number = pull_request["number"]
                pr_head_sha = pull_request["head"]["sha"]

                base_repository_url = f"https://github.com/{pull_request['base']['repo']['full_name']}.git"
                head_repository_url = f"https://github.com/{pull_request['head']['repo']['full_name']}.git"

                action = data["action"]
                # the repository attached to the installation is always the
                # base repository (fork head repositories are never installed)
                gate_repository_url = base_repository_url
                if not repository_is_connected(gitapp, gate_repository_url):
                    delivery_ignored = True
                elif action == "opened":
                    arbitration = arbitrate_pull_request(
                        delivery=delivery,
                        git_app_id=gitapp.id,
                        repository_url=head_repository_url,
                        branch_name=branch_name,
                        pr_number=pr_number,
                        head_sha=pr_head_sha,
                        mode="open",
                    )
                    if not arbitration.proceed:
                        delivery_ignored = True
                elif action == "synchronize" and pull_request["state"] == "open":
                    arbitration = arbitrate_pull_request(
                        delivery=delivery,
                        git_app_id=gitapp.id,
                        repository_url=head_repository_url,
                        branch_name=branch_name,
                        pr_number=pr_number,
                        head_sha=pr_head_sha,
                        mode="sync",
                    )
                    if not arbitration.proceed:
                        delivery_ignored = True
                elif action == "closed":
                    arbitrate_pull_request(
                        delivery=delivery,
                        git_app_id=gitapp.id,
                        repository_url=head_repository_url,
                        branch_name=branch_name,
                        pr_number=pr_number,
                        head_sha=pr_head_sha,
                        mode="close",
                    )
                # "edited" only updates metadata and never advances the
                # generation.

                if not delivery_ignored:
                    match action:
                        case "opened":
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
                                        preview_metadata__pr_number=pull_request[
                                            "number"
                                        ],
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
                                env_name = f"preview-pr-{pull_request['number']}-{current_service.slug}-{fake.slug()}".lower()
                                preview_meta = PreviewEnvMetadata.objects.create(
                                    branch_name=branch_name,
                                    source_trigger=Environment.PreviewSourceTrigger.PULL_REQUEST,
                                    service=current_service,
                                    template=preview_template,
                                    auto_teardown=preview_template.auto_teardown,
                                    external_url=pull_request["html_url"],
                                    git_app=gitapp,
                                    head_repository_url=head_repository_url,
                                    ttl_seconds=preview_template.ttl_seconds,
                                    auth_enabled=preview_template.auth_enabled,
                                    auth_user=preview_template.auth_user,
                                    pr_author=pull_request["user"]["login"],
                                    pr_base_repo_url=base_repository_url,
                                    pr_base_branch_name=pull_request["base"]["ref"],
                                    auth_password=preview_template.auth_password,
                                    deploy_state=(
                                        PreviewEnvMetadata.PreviewDeployState.PENDING
                                        if is_fork
                                        else PreviewEnvMetadata.PreviewDeployState.APPROVED
                                    ),
                                    pr_number=pull_request["number"],
                                    pr_title=pull_request["title"],
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
                                    owner, repo = data["repository"]["full_name"].split(
                                        "/"
                                    )
                                    issue_number = pull_request[
                                        "number"
                                    ]  # issue or PR number

                                    # create issue comment
                                    url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments"

                                    # 2️⃣ Prepare the request
                                    headers = {
                                        "Authorization": f"Bearer {github.get_access_token()}",
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
                                            comment_data["html_url"],
                                        )
                                        print("Comment Body:\n", comment_data["body"])

                                        # Update Preview metadata with the comment ID
                                        preview_meta.pr_comment_id = comment_data["id"]
                                        preview_meta.save()
                                    else:
                                        print(
                                            f"Error when trying to create a PR comment for the {preview_meta.service=} on the PR #{pull_request['number']}({pull_request['html_url']}): ",
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

                                        payload = ComposeStackDeploymentDetails.from_deployment(
                                            deployment
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
                                            owner, repo = data["repository"][
                                                "full_name"
                                            ].split("/")
                                            issue_number = pull_request[
                                                "number"
                                            ]  # issue or PR number

                                            # create issue comment
                                            url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments"

                                            # 2️⃣ Prepare the request
                                            headers = {
                                                "Authorization": f"Bearer {github.get_access_token()}",
                                                "Accept": "application/vnd.github+json",
                                            }
                                            comment_payload = {
                                                "body": new_deployment.get_pull_request_deployment_comment_body()
                                            }

                                            # 3️⃣ Make the POST request
                                            response = requests.post(
                                                url,
                                                headers=headers,
                                                json=comment_payload,
                                            )
                                            # 4️⃣ Check the response
                                            if (
                                                response.status_code
                                                == status.HTTP_201_CREATED
                                            ):
                                                comment_data = response.json()
                                                print(
                                                    "Comment created:",
                                                    comment_data["html_url"],
                                                )
                                                print(
                                                    "Comment Body:\n",
                                                    comment_data["body"],
                                                )

                                                # Update Preview metadata with the comment ID
                                                preview_meta.pr_comment_id = (
                                                    comment_data["id"]
                                                )
                                                preview_meta.save()
                                            else:
                                                print(
                                                    f"Error when trying to create a PR comment for the {service=} on the PR #{pull_request['number']}({pull_request['html_url']}): ",
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

                        case "synchronize":
                            affected_services = Service.get_services_triggered_by_pull_request_sync_event(
                                gitapp=gitapp,
                                repository_url=head_repository_url,
                                pr_number=pull_request["number"],
                            )

                            deployments_to_cancel: list[Deployment] = []
                            for service in affected_services:
                                if service.cleanup_queue_on_auto_deploy:
                                    deployments_to_cancel.extend(
                                        Deployment.flag_deployments_for_cancellation(
                                            service, include_running_deployments=True
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
                        case "closed":
                            matching_preview_envs = Environment.objects.filter(
                                is_preview=True,
                                preview_metadata__source_trigger=Environment.PreviewSourceTrigger.PULL_REQUEST,
                                preview_metadata__head_repository_url=head_repository_url,
                                preview_metadata__git_app=gitapp,
                                preview_metadata__branch_name=branch_name,
                                preview_metadata__auto_teardown=True,
                                preview_metadata__pr_number=pull_request["number"],
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
                        case "edited":
                            matching_preview_envs = Environment.objects.filter(
                                is_preview=True,
                                preview_metadata__source_trigger=Environment.PreviewSourceTrigger.PULL_REQUEST,
                                preview_metadata__head_repository_url=head_repository_url,
                                preview_metadata__git_app=gitapp,
                                preview_metadata__branch_name=branch_name,
                                preview_metadata__auto_teardown=True,
                                preview_metadata__pr_number=pull_request["number"],
                            ).select_related("project", "preview_metadata")

                            for environment in matching_preview_envs:
                                preview_metadata = cast(
                                    PreviewEnvMetadata, environment.preview_metadata
                                )
                                preview_metadata.pr_title = pull_request["title"]
                                preview_metadata.pr_base_branch_name = pull_request[
                                    "base"
                                ]["ref"]
                                preview_metadata.save()
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
