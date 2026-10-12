from typing import cast

from drf_spectacular.utils import extend_schema

from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.generics import ListAPIView, RetrieveUpdateDestroyAPIView
from rest_framework.views import APIView

from rest_framework import exceptions, status

from zane_api.models import (
    Workspace,
    WorkspaceMembership,
    WorkspaceRole,
)
from zane_api.constants import WORKSPACE_SESSION_KEY
from zane_api.permissions import IsInstanceOwner
from zane_api.retirement import (
    get_active_retirement,
    retirement_serialized,
    schedule_dispatch_after_commit,
    start_workspace_retirement,
)

from .serializers import WorkspaceListFilterSet
from zane_api.views.serializers import WorkspaceRetirementProgressSerializer
from ..serializers import (
    WorkspaceDetailSerializer,
    WorkspaceTransferOwnershipSerializer,
    WorkspaceWithOwnerSerializer,
)
from zane_api.views.base import (
    DefaultPageNumberPagination,
    EMPTY_PAGINATED_RESPONSE,
    ResourceConflict,
)
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Q, QuerySet, Prefetch, Value

User = get_user_model()


class ListWorkspacesAPIView(ListAPIView):
    permission_classes = [IsInstanceOwner]
    serializer_class = WorkspaceWithOwnerSerializer
    queryset = Workspace.objects.all()
    pagination_class = DefaultPageNumberPagination
    filter_backends = [DjangoFilterBackend]
    filterset_class = WorkspaceListFilterSet

    def get_queryset(self) -> QuerySet[Workspace]:  # type: ignore
        owner_queryset = (
            WorkspaceMembership.objects.filter()
            .filter(role=WorkspaceRole.OWNER)
            .select_related("user")
        )
        return (
            Workspace.objects.all()
            .prefetch_related(
                Prefetch("memberships", queryset=owner_queryset, to_attr="owner")
            )
            .order_by("id")
        )

    @extend_schema(
        summary="List all workspaces in ZaneOps installation",
    )
    def get(self, request, *args, **kwargs):
        try:
            return super().get(request, *args, **kwargs)
        except exceptions.NotFound as e:
            if "Invalid page" in str(e.detail):
                return Response(EMPTY_PAGINATED_RESPONSE)
            raise e


class WorkspaceDetailAPIView(RetrieveUpdateDestroyAPIView):
    permission_classes = [IsInstanceOwner]
    serializer_class = WorkspaceDetailSerializer
    lookup_field = "pk"
    lookup_url_kwarg = "id"
    http_method_names = ["get", "put", "delete"]

    def get_queryset(self):  # type: ignore
        return Workspace.objects.prefetch_related(
            # No need to prefetch `memberships` as it's already implied
            # when doing `memberships__{table}`
            "memberships__user",
            "memberships__accessible_projects",
        )

    def get_object(self) -> Workspace:  # type: ignore
        return super().get_object()

    def perform_update(self, serializer):
        # The console bypasses `HasWorkspace`, so the retirement freeze is
        # enforced explicitly here: a frozen workspace cannot be renamed.
        if get_active_retirement(workspace=serializer.instance) is not None:
            raise ResourceConflict(
                "This workspace is being retired and cannot be edited."
            )
        return super().perform_update(serializer)

    @extend_schema(
        summary="Delete a workspace (admin)",
        responses={204: None, 202: WorkspaceRetirementProgressSerializer, 200: WorkspaceRetirementProgressSerializer},
    )
    def delete(self, request: Request, *args, **kwargs):
        workspace = self.get_object()

        # Idempotent: repeated delete returns the same generation progress.
        retirement = get_active_retirement(workspace=workspace)
        if retirement is not None:
            return Response(
                retirement_serialized(retirement),
                status=status.HTTP_200_OK,
            )

        # Empty workspace: historical one-shot delete behaviour.
        if not workspace.projects.exists():
            workspace_id = workspace.id
            workspace.delete()
            if request.session.get(WORKSPACE_SESSION_KEY) == workspace_id:
                last_membership = (
                    WorkspaceMembership.objects.filter(user=request.user)
                    .exclude(workspace_id=workspace_id)
                    .select_related("workspace")
                    .first()
                )
                request.session[WORKSPACE_SESSION_KEY] = (
                    last_membership.workspace.id
                    if last_membership is not None
                    else None
                )
            return Response(status=status.HTTP_204_NO_CONTENT)

        with transaction.atomic():
            retirement = start_workspace_retirement(workspace)
            schedule_dispatch_after_commit(retirement)
            payload = retirement_serialized(retirement)

        return Response(payload, status=status.HTTP_202_ACCEPTED)


class WorkspaceTransferOwnershipAPIView(APIView):
    permission_classes = [IsInstanceOwner]

    @transaction.atomic()
    @extend_schema(
        responses=WorkspaceDetailSerializer,
        request=WorkspaceTransferOwnershipSerializer,
        operation_id="consoleTransferWorkspaceOwnership",
        summary="Transfer workspace ownership (admin)",
    )
    def put(self, request: Request, id: str):
        try:
            workspace = Workspace.objects.filter(pk=id).get()
        except Workspace.DoesNotExist:
            raise exceptions.NotFound(f"Workspace with `id={id}` does not exist.")

        form = WorkspaceTransferOwnershipSerializer(data=request.data)
        form.is_valid(raise_exception=True)
        data = cast(dict, form.validated_data)

        new_owner = User.objects.get(pk=data["owner_id"])

        new_owner_membership = WorkspaceMembership.objects.filter(
            user=new_owner,
            workspace=workspace,
        ).first()

        if new_owner_membership is None:
            new_owner_membership = WorkspaceMembership(
                user=new_owner,
                workspace=workspace,
            )
        elif new_owner_membership.role == WorkspaceRole.OWNER:
            raise ResourceConflict("This user is already the owner of the workspace")

        # Update the previous owner
        WorkspaceMembership.objects.filter(
            workspace=workspace, role=WorkspaceRole.OWNER
        ).update(role=WorkspaceRole.ADMIN)

        # Change ownership
        new_owner_membership.role = WorkspaceRole.OWNER
        new_owner_membership.save()

        serializer = WorkspaceDetailSerializer(workspace)
        return Response(serializer.data)
