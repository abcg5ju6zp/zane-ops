import asyncio

from django.core.management.base import BaseCommand
from django.conf import settings

from console.models import SystemSettings
from temporal.schedules import (
    CleanupAppDataWorkflow,
    ReconcileWorkspaceRetirementsWorkflow,
)
from temporal.workflows.system import DockerSystemPruneWorkflow
from temporal.client import TemporalClient
from temporal.activities.workspace_retirement import (
    areconcile_workspace_retirements,
)
from zane_api.utils import Colors


async def noop(): ...


async def setup_jobs():
    system = await SystemSettings.aget_or_create()

    # Delete obsolete jobs
    print("Deleting obsolete schedules...🔄")
    await asyncio.gather(
        TemporalClient.adelete_schedule(
            settings.OLD_APP_DATA_CLEANUP_SCHEDULE_ID,
            prefix=None,
        ),
        TemporalClient.adelete_schedule(
            settings.OLD_DOCKER_SYSTEM_PRUNE_SCHEDULE_ID,
            prefix=None,
        ),
        return_exceptions=True,  # Don't raise exceptions
    )
    print("Obsolete schedules deleted ✅")

    # Create new jobs
    print(
        f"Creating or updating schedule {Colors.ORANGE}{settings.APP_DATA_CLEANUP_SCHEDULE_ID}{Colors.BLUE}...🔄{Colors.ENDC}"
    )
    schedules = [
        TemporalClient.create_or_update_schedule(
            schedule_id=settings.APP_DATA_CLEANUP_SCHEDULE_ID,
            workflow=CleanupAppDataWorkflow.run,
            schedule_cron=system.app_data_cleanup_cron_schedule,
        ),
        # restart recovery for in-progress workspace retirements, every minute
        TemporalClient.create_or_update_schedule(
            schedule_id=settings.WORKSPACE_RETIREMENT_RECONCILE_SCHEDULE_ID,
            workflow=ReconcileWorkspaceRetirementsWorkflow.run,
            schedule_cron="*/1 * * * *",
        ),
    ]
    if settings.ENVIRONMENT == settings.PRODUCTION_ENV:
        print(
            f"Creating or updating schedule {Colors.ORANGE}{settings.DOCKER_SYSTEM_PRUNE_SCHEDULE_ID}{Colors.BLUE}...🔄{Colors.ENDC}"
        )
        schedules.append(
            TemporalClient.create_or_update_schedule(
                schedule_id=settings.DOCKER_SYSTEM_PRUNE_SCHEDULE_ID,
                workflow=DockerSystemPruneWorkflow.run,
                schedule_cron=system.docker_system_prune_cron_schedule,
            )
        )

    await asyncio.gather(*schedules)
    print("Schedules created/updated successfully ✅")

    # Resume generations interrupted by the API restart (lost workflow start
    # responses, crashed finalization). Never block boot if Temporal is down.
    try:
        result = await areconcile_workspace_retirements()
        if result.redelivered or result.finalized:
            print(
                f"Resumed workspace retirements on boot: "
                f"redelivered={result.redelivered} finalized={result.finalized}"
            )
    except Exception as exc:  # pragma: no cover - boot resilience
        print(f"Workspace retirement reconcile on boot failed: {exc!r}")


class Command(BaseCommand):
    help = "Create or update default automated schedules"

    def handle(self, *args, **options):
        asyncio.run(setup_jobs())
