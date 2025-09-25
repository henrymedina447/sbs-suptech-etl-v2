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


def main() -> None:
    dictConfig(UVICORN_LOGGING)
    mode = "api"
    if mode == "api":
        print("Ejecutando modo api")
        run_api()


if __name__ == "__main__":
    main()
