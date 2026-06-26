"""NATS JetStream event publisher."""

import json
from typing import Any

import nats
from nats.js.api import StreamConfig
import structlog

from gateway.config import settings
from gateway.models import UploadEvent

logger = structlog.get_logger()


class EventPublisher:
    """Publisher for upload events to NATS JetStream."""

    def __init__(self):
        """Initialize publisher (connection is async)."""
        self.nc: nats.NATS | None = None
        self.js = None
        self.stream_name = settings.nats_stream
        self.subject = settings.nats_subject

    async def connect(self) -> bool:
        """Connect to NATS and ensure stream exists."""
        try:
            self.nc = await nats.connect(settings.nats_url)
            self.js = self.nc.jetstream()

            # Create or get stream
            try:
                await self.js.add_stream(
                    config=StreamConfig(
                        name=self.stream_name,
                        subjects=[f"minio.uploads.>"],
                        retention="workqueue",
                        max_msgs=1_000_000,
                        max_bytes=1024 * 1024 * 1024,  # 1GB
                    )
                )
                logger.info("stream_created", stream=self.stream_name)
            except nats.js.errors.BadRequestError:
                # Stream already exists
                logger.debug("stream_exists", stream=self.stream_name)

            logger.info("nats_connected", url=settings.nats_url, stream=self.stream_name)
            return True
        except Exception as e:
            logger.error("nats_connection_failed", url=settings.nats_url, error=str(e))
            return False

    async def disconnect(self):
        """Disconnect from NATS."""
        if self.nc:
            await self.nc.close()
            self.nc = None
            self.js = None
            logger.info("nats_disconnected")

    async def publish(self, event: UploadEvent) -> bool:
        """Publish an upload event to NATS.

        Args:
            event: The upload event to publish

        Returns:
            True if published successfully
        """
        if not self.js:
            logger.error("nats_not_connected")
            return False

        try:
            payload = event.model_dump_json().encode()
            ack = await self.js.publish(self.subject, payload)
            logger.info(
                "event_published",
                subject=self.subject,
                stream=ack.stream,
                seq=ack.seq,
                key=event.key,
            )
            return True
        except Exception as e:
            logger.error("event_publish_failed", subject=self.subject, error=str(e))
            return False

    def is_connected(self) -> bool:
        """Check if connected to NATS."""
        return self.nc is not None and self.nc.is_connected


# Global publisher instance
event_publisher: EventPublisher | None = None


async def get_event_publisher() -> EventPublisher:
    """Get or create the event publisher."""
    global event_publisher
    if event_publisher is None:
        event_publisher = EventPublisher()
        await event_publisher.connect()
    return event_publisher


async def close_event_publisher():
    """Close the event publisher."""
    global event_publisher
    if event_publisher:
        await event_publisher.disconnect()
        event_publisher = None
