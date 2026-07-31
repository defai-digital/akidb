"""NATS JetStream event publisher."""

import asyncio

import nats
import structlog
from nats.js.api import StreamConfig
from nats.js.errors import NotFoundError

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
        self._stream_ready = False

    def _stream_config(self) -> StreamConfig:
        subjects = ["minio.uploads", "minio.uploads.>"]
        if not (
            self.subject == "minio.uploads"
            or self.subject.startswith("minio.uploads.")
        ):
            subjects.append(self.subject)
        return StreamConfig(
            name=self.stream_name,
            subjects=subjects,
            retention="workqueue",
            max_msgs=1_000_000,
            max_bytes=1024 * 1024 * 1024,
            num_replicas=settings.nats_replicas,
        )

    async def _ensure_stream(self) -> None:
        """Create the stream or reconcile subjects and replication in place."""
        config = self._stream_config()
        try:
            info = await self.js.stream_info(self.stream_name)
        except NotFoundError:
            await self.js.add_stream(config=config)
            logger.info("stream_created", stream=self.stream_name)
            return

        current = info.config
        subjects = list(current.subjects or [])
        for subject in config.subjects or []:
            if subject not in subjects:
                subjects.append(subject)
        needs_update = (
            subjects != list(current.subjects or [])
            or current.num_replicas != config.num_replicas
        )
        if needs_update:
            updated = current.evolve(
                subjects=subjects,
                num_replicas=config.num_replicas,
            )
            await self.js.update_stream(config=updated)
            logger.info("stream_updated", stream=self.stream_name)

    async def connect(self) -> bool:
        """Connect to NATS and ensure stream exists."""
        await self.disconnect()
        try:
            self.nc = await nats.connect(settings.nats_url)
            self.js = self.nc.jetstream()

            await self._ensure_stream()
            self._stream_ready = True

            logger.info("nats_connected", url=settings.nats_url, stream=self.stream_name)
            return True
        except Exception as e:
            logger.error("nats_connection_failed", url=settings.nats_url, error=str(e))
            await self.disconnect()
            return False

    async def disconnect(self):
        """Disconnect from NATS."""
        self._stream_ready = False
        connection = self.nc
        self.nc = None
        self.js = None
        if connection:
            try:
                await connection.close()
                logger.info("nats_disconnected")
            except Exception as error:
                logger.warning("nats_disconnect_failed", error=str(error))

    async def publish(self, event: UploadEvent) -> bool:
        """Publish an upload event to NATS.

        Args:
            event: The upload event to publish

        Returns:
            True if published successfully
        """
        if not self.is_connected():
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
            self._stream_ready = False
            logger.error("event_publish_failed", subject=self.subject, error=str(e))
            return False

    def is_connected(self) -> bool:
        """Check that NATS and the configured JetStream stream are ready."""
        return (
            self._stream_ready
            and self.nc is not None
            and self.nc.is_connected
            and self.js is not None
        )


# Global publisher instance
event_publisher: EventPublisher | None = None
event_publisher_lock = asyncio.Lock()


async def get_event_publisher() -> EventPublisher:
    """Get or create the event publisher."""
    global event_publisher
    async with event_publisher_lock:
        if event_publisher is None:
            event_publisher = EventPublisher()
        if not event_publisher.is_connected():
            await event_publisher.connect()
        return event_publisher


async def close_event_publisher():
    """Close the event publisher."""
    global event_publisher
    async with event_publisher_lock:
        if event_publisher:
            await event_publisher.disconnect()
            event_publisher = None
