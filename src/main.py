import asyncio
import signal
import sys
from logging.config import dictConfig

import uvicorn

from infrastructure.config.uvicorn_logging_settings import UVICORN_LOGGING


def run_api() -> None:
    uvicorn.run(
        "presentation.controllers.http_controllers.fast_api_controller:app",
        host="0.0.0.0",
        port=9090,
        reload=True,
        log_level="info",
        log_config=UVICORN_LOGGING
    )


async def run_worker() -> None:
    from presentation.controllers.event_controllers.kafka_event_controller import KafkaEventController

    max_conc = 8
    controller = KafkaEventController(max_conc)
    await controller.start()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # Señales (Unix). En Windows algunos signals no están disponibles: hacemos fallback.
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        await controller.stop()


def main() -> None:
    dictConfig(UVICORN_LOGGING)
    mode = "kafka"
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()

    if mode in ("api", "http", "server"):
        run_api()
    elif mode in ("worker", "event", "kafka"):
        asyncio.run(run_worker())
    else:
        sys.stderr.write(f"Modo desconocido: {mode}. Usa 'api' o 'worker'.\n")
        sys.exit(2)


if __name__ == "__main__":
    main()
