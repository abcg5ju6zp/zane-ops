from typing import cast

from drf_spectacular.utils import extend_schema

from rest_framework.views import APIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.generics import (
    ListAPIView,
    CreateAPIView,
    RetrieveDestroyAPIView,
    RetrieveUpdateDestroyAPIView,
)

from rest_framework import status, permissions
from drf_spectacular.utils import OpenApiResponse

from ..retirement import (
    get_active_retirement,
    start_workspace_retirement,
    schedule_dispatch_after_commit,
    retirement_serialized,
)

from ..models import (
    Workspace,
    WorkspaceMembership,
    WorkspaceRole,
)

from ..constants import WORKSPACE_SESSION_KEY
from .serializers import (
    SwitchWorkspaceRequestSerializer,
    WorkspaceEditPermissionsRequestSerializer,
    WorkspaceRetirementProgressSerializer,
    WorkspaceTransferOwnershipResponseSerializer,
    WorkspaceTransferOwnershipRequestSerializer,
    WorkspaceMembershipFilterSet,
    WorkspaceLeaveResponseSerializer,
)
from rest_framework import exceptions
from ..serializers import (
    WorkspaceMembershipSerializer,
    WorkspaceSerializer,
    WorkspaceMemberSerializer,
)
from ..permissions import (
    IsInstanceOwner,
    HasWorkspace,
    IsWorkspaceOwner,
    IsWorkspaceAdmin,
    IsWorkspaceViewer,
    IsWorkspaceMember,
)

from django.db.models import QuerySet, Q
from .base import ResourceConflict, EMPTY_PAGINATED_RESPONSE
from django.db import transaction
from django_filters.rest_framework import DjangoFilterBackend
from ..licensing.gate import can_create_workspace
from .base import DefaultPageNumberPagination


class WorkspaceMemberDetailAPIView(RetrieveDestroyAPIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]
    serializer_class = WorkspaceMemberSerializer
    lookup_field = "pk"
    lookup_url_kwarg = "membership_id"

    def get_queryset(self) -> QuerySet[WorkspaceMembership]:  # type: ignore
        return (
            WorkspaceMembership.objects.filter(
                workspace=self.request.workspace  # type: ignore
            )
            .select_related("user")
            .prefetch_related("accessible_projects")
        )

    def perform_destroy(self, instance: WorkspaceMembership):
        current_membership = WorkspaceMembership.objects.get(
            workspace=self.request.workspace,  # type: ignore
            user=self.request.user,
        )

        if instance.user == self.request.user:
            raise ResourceConflict(
                "You cannot remove yourself from the workspace. Contact the owner to remove you from the workspace."
            )

        # We don't need to check for the case of removing another owner
        # as there are DB checks that enforces that a workspace can only
        # have one owner
        if (
            current_membership.role < WorkspaceRole.OWNER
            and instance.role >= WorkspaceRole.ADMIN
        ):
            raise ResourceConflict(
                "You cannot remove another admin or the owner of the workspace."
            )

        return super().perform_destroy(instance)


class EditWorkspaceMemberPermissionsAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceAdmin]
    serializer_class = WorkspaceMemberSerializer

    @transaction.atomic()
    @extend_schema(
        request=WorkspaceEditPermissionsRequestSerializer,
        operation_id="editWorkpacePermissions",
        summary="Edit workspace membership permissions",
    )
    def put(self, request: Request, membership_id: int):
        try:
            membership = (
                WorkspaceMembership.objects.filter(
                    workspace=self.request.workspace,  # type: ignore
                    id=membership_id,
                )
                .select_related("user")
                .prefetch_related("accessible_projects")
                .get()
            )
        except WorkspaceMembership.DoesNotExist:
            raise exceptions.NotFound()

        if membership.role >= WorkspaceRole.OWNER:
            raise ResourceConflict(
                "You cannot edit the permissions of the workspace owner."
            )
        if membership.user == self.request.user:
            raise ResourceConflict(
                "You cannot edit your own permissions in the workspace."
            )

        current_membership = WorkspaceMembership.objects.get(
            workspace=self.request.workspace,  # type: ignore
            user=self.request.user,
        )
        if (
            current_membership.role == WorkspaceRole.ADMIN
            and membership.role >= WorkspaceRole.ADMIN
        ):
            raise ResourceConflict(
                "You cannot edit the permissions of another admin of the workspace."
            )

        form = WorkspaceEditPermissionsRequestSerializer(
            data=request.data,
            context=dict(
                workspace=self.request.workspace  # type: ignore
            ),
        )
        form.is_valid(raise_exception=True)

        data = cast(dict, form.validated_data)

        if (
            current_membership.role < WorkspaceRole.OWNER
            and data["role"] >= WorkspaceRole.ADMIN
        ):
            raise ResourceConflict(
                "Only the workspace owner can promote a user to admin."
            )

        membership.role = data["role"]
        membership.save()

        membership.accessible_projects.clear()
        for project in data["accessible_project_ids"]:
            membership.accessible_projects.add(project)

        serializer = WorkspaceMemberSerializer(membership)
        return Response(serializer.data)


class ListWorkspaceMembersAPIView(ListAPIView):
    serializer_class = WorkspaceMemberSerializer
    filter_backends = [DjangoFilterBackend]
    pagination_class = DefaultPageNumberPagination
    filterset_class = WorkspaceMembershipFilterSet
    permission_classes = [HasWorkspace, IsWorkspaceMember]

    queryset = WorkspaceMembership.objects.all()  # just used for the openAPI docs

    @extend_schema(
        summary="List workspace members",
    )
    def get(self, request, *args, **kwargs):
        try:
            return super().get(request, *args, **kwargs)
        except exceptions.NotFound as e:
            if "Invalid page" in str(e.detail):
                return Response(EMPTY_PAGINATED_RESPONSE)
            raise e

    def get_queryset(self) -> QuerySet[WorkspaceMembership]:  # type: ignore
        return (
            WorkspaceMembership.objects.filter(
                workspace=self.request.workspace  # type: ignore
            )
            .select_related("user")
            .prefetch_related("accessible_projects")
        )


class WorkspaceMembershipListAPIView(ListAPIView):
    serializer_class = WorkspaceMembershipSerializer
    pagination_class = None
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self) -> QuerySet[WorkspaceMembership]:  # type: ignore
        return (
            WorkspaceMembership.objects.filter(user=self.request.user)
            .select_related("workspace")
            .order_by("pk")
        )


class WorkspaceDetailAPIView(RetrieveUpdateDestroyAPIView):
    serializer_class = WorkspaceSerializer
    http_method_names = ["put", "delete", "get"]

    # The DELETE handler is the idempotent entry point of the retirement
    # process itself, so it must run while the workspace is frozen; PUT stays
    # blocked (the hook only opens DELETE).
    @staticmethod
    def allow_retiring_workspace_writes(request: Request) -> bool:
        return request.method == "DELETE"

    def get_object(self) -> Workspace:  # type: ignore
        return self.request.workspace  # type: ignore

    def get_permissions(self):
        if self.request.method == "GET":
            return [HasWorkspace(), IsWorkspaceViewer()]
        return [HasWorkspace(), IsWorkspaceOwner()]

    def _repoint_owner_session(self, workspace_id: str) -> None:
        last_membership = (
            WorkspaceMembership.objects.filter(
                user=self.request.user,
            )
            .exclude(
                workspace_id=workspace_id,
            )
            .select_related("workspace")
            .first()
        )
        self.request.session[WORKSPACE_SESSION_KEY] = (
            last_membership.workspace.id if last_membership is not None else None
        )

    @extend_schema(
        responses={
            204: OpenApiResponse(description="Empty workspace deleted immediately"),
            202: WorkspaceRetirementProgressSerializer,
            200: WorkspaceRetirementProgressSerializer,
        },
        operation_id="deleteWorkspace",
        summary="Delete (retire) the current workspace",
    )
    def delete(self, request, *args, **kwargs):
        workspace = cast(Workspace, request.workspace)

        # Idempotent: a repeated delete returns the same generation progress
        # without archiving or deleting anything twice.
        retirement = get_active_retirement(workspace=workspace)
        if retirement is not None:
            return Response(
                retirement_serialized(retirement),
                status=status.HTTP_200_OK,
            )

        # Empty workspace: keep the historical one-shot delete behaviour.
        if not workspace.projects.exists():
            workspace_id = workspace.id
            workspace.delete()
            self._repoint_owner_session(workspace_id)
            return Response(status=status.HTTP_204_NO_CONTENT)

        # Non-empty workspace: fix the snapshot and cleanup outbox in one
        # transaction, deliver the retirement workflow after commit, keep the
        # workspace queryable until every external resource is confirmed.
        with transaction.atomic():
            retirement = start_workspace_retirement(workspace)
            schedule_dispatch_after_commit(retirement)
            payload = retirement_serialized(retirement)

        return Response(payload, status=status.HTTP_202_ACCEPTED)


class CreateWorkspaceAPIView(CreateAPIView):
    permission_classes = [IsInstanceOwner]
    serializer_class = WorkspaceSerializer

    def perform_create(self, serializer: WorkspaceSerializer):
        allowed, error = can_create_workspace(Workspace.objects.count())
        if not allowed:
            raise exceptions.PermissionDenied(error)

        super().perform_create(serializer)

        WorkspaceMembership.objects.create(
            user=self.request.user,
            workspace=serializer.instance,
            role=WorkspaceRole.OWNER,
        )

    @extend_schema(
        operation_id="createWorkspace",
        summary="Create a new workspace",
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class SwitchWorkspaceAPIView(APIView):
    # session-only change, allowed while a workspace is retiring
    allow_retiring_workspace_writes = True

    @extend_schema(
        request=SwitchWorkspaceRequestSerializer,
        responses={
            204: None,
        },
        operation_id="switchWorkspace",
        summary="Switch workspaces",
    )
    def post(self, request: Request) -> Response:
        form = SwitchWorkspaceRequestSerializer(data=request.data)
        form.is_valid(raise_exception=True)
        data = cast(dict, form.data)

        workspace_id = data.get("workspace_id")

        workspace = Workspace.objects.filter(
            memberships__user=request.user,
            id=workspace_id,
        ).first()

        if workspace is None:
            raise exceptions.NotFound(detail="Workspace not found")

        request.session[WORKSPACE_SESSION_KEY] = workspace.id

        return Response(status=status.HTTP_204_NO_CONTENT)


class WorkspaceLeaveAPIView(APIView):
    serializer_class = WorkspaceLeaveResponseSerializer

    @extend_schema(
        request=None,
        operation_id="leaveWorkspace",
        summary="Leave workspace",
    )
    def post(self, request: Request):
        membership = WorkspaceMembership.objects.get(
            user=self.request.user,
            workspace=self.request.workspace,  # type: ignore
        )

        if membership.role == WorkspaceRole.OWNER:
            raise ResourceConflict(
                "You cannot leave this workspace, to be able to do so, please transfer ownership to another member."
            )
        membership.delete()

        last_membership = (
            WorkspaceMembership.objects.filter(
                user=self.request.user,
            )
            .exclude(
                workspace=self.request.workspace  # type: ignore
            )
            .select_related("workspace")
            .first()
        )

        request.session[WORKSPACE_SESSION_KEY] = (
            last_membership.workspace.id if last_membership is not None else None
        )

        serializer = WorkspaceLeaveResponseSerializer({"success": True})

        return Response(serializer.data, status=status.HTTP_200_OK)


class WorkspaceTransferOwnershipAPIView(APIView):
    permission_classes = [HasWorkspace, IsWorkspaceOwner]
    serializer_class = WorkspaceTransferOwnershipResponseSerializer

    @transaction.atomic()
    @extend_schema(
        request=WorkspaceTransferOwnershipRequestSerializer,
        operation_id="transferWorkspaceOwnership",
        summary="Transfer workspace ownership",
    )
    def post(self, request: Request):
        form = WorkspaceTransferOwnershipRequestSerializer(data=request.data)
        form.is_valid(raise_exception=True)

        data = cast(dict, form.validated_data)

        try:
            new_owner_membership = WorkspaceMembership.objects.get(
                pk=data["new_owner_id"],
                workspace=self.request.workspace,  # type: ignore
            )
        except WorkspaceMembership.DoesNotExist:
            raise exceptions.NotFound("This user is not a member of this workspace.")

        if new_owner_membership.role < WorkspaceRole.ADMIN:
            raise ResourceConflict(
                "You cannot transfer ownership to a non-admin member. Promote them to admin first."
            )

        if new_owner_membership.user == self.request.user:
            raise ResourceConflict("You are already the owner of this workspace.")

        WorkspaceMembership.objects.filter(
            user=self.request.user,
            workspace=self.request.workspace,  # type: ignore
        ).update(role=WorkspaceRole.ADMIN)

        WorkspaceMembership.objects.filter(
            pk=data["new_owner_id"],
            workspace=self.request.workspace,  # type: ignore
        ).update(role=WorkspaceRole.OWNER)

        serializer = WorkspaceTransferOwnershipResponseSerializer({"success": True})
        return Response(serializer.data)
