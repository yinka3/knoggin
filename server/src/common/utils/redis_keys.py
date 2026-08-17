"""Redis key names shared by portable coordination helpers."""


def community_pubsub_channel() -> str:
    """Return the channel used for community event fan-out."""

    return "community:events"
