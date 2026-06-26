"""AkiDB Upload Gateway Service.

Receives document uploads and publishes events to NATS for processing.
"""

__version__ = "0.1.0"

from gateway.api import app
from gateway.config import settings

__all__ = ["app", "settings", "__version__"]
