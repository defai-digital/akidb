"""Entry point for the upload gateway service."""

import uvicorn

from gateway.config import settings


def main():
    """Run the upload gateway service."""
    uvicorn.run(
        "gateway:app",
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
