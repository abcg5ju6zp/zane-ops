import asyncio

from django.core.management.base import BaseCommand

from temporal.activities.workspace_retirement import (
    areconcile_workspace_retirements,
)


class Command(BaseCommand):
    help = (
        "Resume in-progress workspace retirement generations: redeliver lost "
        "cleanup workflows and finalize generations whose units are all "
        "confirmed. Only unconfirmed units of existing generations are touched."
    )

    def handle(self, *args, **options):
        result = asyncio.run(areconcile_workspace_retirements())
        self.stdout.write(
            f"Workspace retirement reconcile: redelivered={result.redelivered} "
            f"finalized={result.finalized} pending={result.pending}"
        )
