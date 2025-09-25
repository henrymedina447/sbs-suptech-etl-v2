UVICORN_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "access": {
            "format": "%(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },

    },
    "handlers": {
        "default": {"class": "logging.StreamHandler", "formatter": "default"},
        "access": {"class": "logging.StreamHandler", "formatter": "access"},
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO"},
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        # 👇 tus loggers de app:
        "app": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "app.environment": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "app.workflows": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "": {"handlers": ["default"], "level": "INFO"},
    },
}
