"""Entry point for the document parser service."""

import uvicorn

from parser.config import settings


def main():
    """Run the document parser service."""
    uvicorn.run(
        "parser:app",
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
