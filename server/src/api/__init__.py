"""Application-facing HTTP adapters.

The API package deliberately contains only the transport boundary.  Production
bootstrapping is supplied by an :class:`~api.app.ApplicationPort` implementation
at application startup; importing this package never creates a database or
model, or ``RuntimeResources`` instance.
"""

from api.app import ApplicationPort, create_app

__all__ = ["ApplicationPort", "create_app"]
