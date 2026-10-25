"""Syntax checks shared by agent web reads and saved agent sources."""

import ipaddress
from urllib.parse import urlsplit, urlunsplit


def normalize_public_web_url(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("url must be a non-blank absolute HTTP(S) URL")
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except ValueError as exc:
        raise ValueError("url has an invalid host or port") from exc

    scheme = parsed.scheme.lower()
    host = parsed.hostname
    if scheme not in {"http", "https"} or not parsed.netloc or not host:
        raise ValueError("url must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("url must not include credentials")
    if parsed.fragment:
        raise ValueError("url must not include a fragment")
    if port is not None and port not in {80, 443}:
        raise ValueError("url must use a standard HTTP(S) port")

    try:
        normalized_host = host.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("url hostname cannot be normalized") from exc
    if normalized_host == "localhost" or normalized_host.endswith(".localhost"):
        raise ValueError("url must not target localhost")

    try:
        literal_ip = ipaddress.ip_address(normalized_host)
    except ValueError:
        literal_ip = None
    if literal_ip is not None and not literal_ip.is_global:
        raise ValueError("url must not target a non-public IP address")

    authority = f"[{normalized_host}]" if ":" in normalized_host else normalized_host
    if port is not None:
        authority += f":{port}"
    return urlunsplit((scheme, authority, parsed.path or "/", parsed.query, ""))
