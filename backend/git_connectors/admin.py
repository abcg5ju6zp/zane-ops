from django.contrib import admin

from .models import GitRefGeneration, GitWebhookDelivery


@admin.register(GitWebhookDelivery)
class GitWebhookDeliveryAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "provider",
        "delivery_id",
        "git_app",
        "event_type",
        "action",
        "stage",
        "stale",
        "created_at",
    )
    list_filter = ("provider", "event_type", "stage", "stale")
    search_fields = ("delivery_id", "payload_digest", "repository_url")
    readonly_fields = ("created_at", "updated_at")


@admin.register(GitRefGeneration)
class GitRefGenerationAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "git_app",
        "repository_url",
        "kind",
        "ref_name",
        "pr_number",
        "head_sha",
        "state",
        "updated_at",
    )
    list_filter = ("kind", "state")
    search_fields = ("repository_url", "ref_name")
    readonly_fields = ("created_at", "updated_at")
