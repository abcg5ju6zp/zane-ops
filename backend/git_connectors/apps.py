import sys
import threading

from django.apps import AppConfig
from django.conf import settings


class GitConnectorsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "git_connectors"

    def ready(self):
        # Restart recovery: replay Temporal starts committed to the webhook
        # delivery ledger but never confirmed before the previous process
        # exited. Only run alongside an actual API server (not migrations,
        # shell or tests) and in a background thread so boot is never blocked.
        if getattr(settings, "TESTING", False):
            return
        argv = " ".join(sys.argv)
        is_api_process = "runserver" in argv or "gunicorn" in sys.argv[0]
        if not is_api_process:
            return

        def recover():
            try:
                from .delivery import recover_pending_deliveries

                recover_pending_deliveries()
            except Exception:
                # recovery must never crash server boot; the next redelivery
                # retries the same pending deliveries
                pass

        threading.Thread(target=recover, daemon=True).start()
