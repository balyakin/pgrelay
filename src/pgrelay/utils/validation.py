"""Validation helpers for queues, HTTP headers and targets."""

import asyncio
import ipaddress
import re
import socket
from collections.abc import Mapping
from urllib.parse import urlparse

from pgrelay.errors import ValidationError

QUEUE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9]*(?:[._-][A-Za-z0-9]+)*$")
HEADER_NAME_PATTERN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


def validate_queue_name(value: str) -> None:
    """Validate a PgRelay queue name."""
    if len(value) < 1 or len(value) > 128 or QUEUE_NAME_PATTERN.fullmatch(value) is None:
        raise ValidationError("Invalid queue name")


def validate_header_name(name: str) -> None:
    """Validate one HTTP header name."""
    if HEADER_NAME_PATTERN.fullmatch(name) is None:
        raise ValidationError("Invalid HTTP header name")


def validate_header_value(value: str) -> None:
    """Validate one HTTP header value."""
    if "\r" in value or "\n" in value:
        raise ValidationError("HTTP header value contains CRLF")


def validate_headers(headers: Mapping[str, str]) -> None:
    """Validate HTTP headers."""
    for name, value in headers.items():
        validate_header_name(name)
        validate_header_value(value)


def extract_url_hostname(url: str) -> str:
    """Return a normalized hostname from an HTTP URL."""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValidationError("URL scheme must be http or https")
    if not parsed.hostname:
        raise ValidationError("URL hostname is required")
    return parsed.hostname.lower()


def validate_url_syntax(url: str) -> str:
    """Validate URL syntax and return normalized hostname."""
    return extract_url_hostname(url)


def validate_allowed_host(hostname: str, allowed_hosts: set[str]) -> None:
    """Validate hostname against an optional allowlist."""
    if allowed_hosts and hostname.lower() not in allowed_hosts:
        raise ValidationError("HTTP target host is not allowed")


def is_blocked_ip_address(address: str) -> bool:
    """Return true when an address belongs to a blocked range."""
    ip_address = ipaddress.ip_address(address)
    return (
        ip_address.is_private
        or ip_address.is_loopback
        or ip_address.is_link_local
        or ip_address.is_multicast
        or ip_address.is_reserved
        or ip_address.is_unspecified
    )


async def validate_public_target(url: str) -> None:
    """Resolve a URL hostname and reject private network targets."""
    hostname = extract_url_hostname(url)
    port = urlparse(url).port
    loop = asyncio.get_running_loop()
    infos = await loop.run_in_executor(None, socket.getaddrinfo, hostname, port, 0, 0, 0, 0)
    for info in infos:
        sockaddr = info[4]
        address = str(sockaddr[0])
        if is_blocked_ip_address(address):
            raise ValidationError("HTTP target resolves to a blocked network address")
