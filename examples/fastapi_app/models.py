"""Example domain model."""

from sqlalchemy import Text
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()


class WebhookEvent(Base):
    """Minimal domain row for the example app."""

    __tablename__ = "example_webhook_event"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
