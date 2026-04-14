from __future__ import annotations

import logging


def pytest_sessionstart(session) -> None:
    # Prefect may start an ephemeral server during flow execution. Its shutdown INFO
    # log can race with pytest's stream teardown and emit noisy Rich handler errors.
    logging.raiseExceptions = False
    logging.getLogger("prefect").setLevel(logging.WARNING)
    try:
        from prefect.server.api import server as prefect_server

        prefect_server.subprocess_server_logger.disabled = True
    except Exception:
        pass
