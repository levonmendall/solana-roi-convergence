"""Fail-closed outbound-network guard for portable reproduction only.

Activate by prepending this directory to PYTHONPATH. Loopback/Unix-socket traffic
is allowed so the local ASGI process and deterministic replay servers can talk.
DNS lookups for arbitrary hosts and all non-loopback TCP/UDP destinations fail
before any external network request can leave the reproduction boundary.
"""
from __future__ import annotations

import ipaddress
import socket

_original_getaddrinfo = socket.getaddrinfo
_original_connect = socket.socket.connect
_original_connect_ex = socket.socket.connect_ex
_original_create_connection = socket.create_connection
_original_sendto = socket.socket.sendto


def _allowed_host(host: object) -> bool:
    if host is None:
        return True
    if isinstance(host, bytes):
        host = host.decode("ascii", "strict")
    text = str(host).strip().lower()
    if text == "localhost":
        return True
    try:
        return ipaddress.ip_address(text).is_loopback
    except ValueError:
        return False


def _allowed_address(address: object) -> bool:
    # AF_UNIX addresses are represented as filesystem strings and never leave the host.
    if isinstance(address, str):
        return True
    if not isinstance(address, tuple) or not address:
        return False
    return _allowed_host(address[0])


def _blocked(kind: str, target: object) -> OSError:
    return OSError(f"portable reproduction blocked live {kind}: {target!r}")


def _guarded_getaddrinfo(host, port, *args, **kwargs):
    if not _allowed_host(host):
        raise _blocked("DNS resolution", host)
    return _original_getaddrinfo(host, port, *args, **kwargs)


def _guarded_connect(sock: socket.socket, address: object):
    if not _allowed_address(address):
        raise _blocked("network destination", address)
    return _original_connect(sock, address)


def _guarded_connect_ex(sock: socket.socket, address: object):
    if not _allowed_address(address):
        raise _blocked("network destination", address)
    return _original_connect_ex(sock, address)


def _guarded_create_connection(address: object, *args, **kwargs):
    if not _allowed_address(address):
        raise _blocked("network destination", address)
    return _original_create_connection(address, *args, **kwargs)


def _guarded_sendto(sock: socket.socket, data, *args, **kwargs):
    # sendto(data, address) or sendto(data, flags, address)
    address = kwargs.get("address")
    if address is None and args:
        address = args[-1]
    if address is not None and not _allowed_address(address):
        raise _blocked("datagram destination", address)
    return _original_sendto(sock, data, *args, **kwargs)


socket.getaddrinfo = _guarded_getaddrinfo  # type: ignore[assignment]
socket.socket.connect = _guarded_connect  # type: ignore[assignment]
socket.socket.connect_ex = _guarded_connect_ex  # type: ignore[assignment]
socket.create_connection = _guarded_create_connection  # type: ignore[assignment]
socket.socket.sendto = _guarded_sendto  # type: ignore[assignment]
