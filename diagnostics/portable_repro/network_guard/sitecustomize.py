"""Fail-closed outbound-network guard for portable reproduction only.

Activate by prepending this directory to PYTHONPATH. Loopback/Unix-socket traffic
is allowed so the local ASGI process and deterministic replay server can talk.
All non-loopback TCP/UDP connection attempts raise immediately.
"""
from __future__ import annotations

import ipaddress
import socket

_original_connect = socket.socket.connect
_original_connect_ex = socket.socket.connect_ex
_original_create_connection = socket.create_connection


def _allowed_address(address: object) -> bool:
    if isinstance(address, str):
        return True
    if not isinstance(address, tuple) or not address:
        return False
    host = address[0]
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return ipaddress.ip_address(str(host)).is_loopback
    except ValueError:
        return False


def _guarded_connect(sock: socket.socket, address: object):
    if not _allowed_address(address):
        raise OSError(f"portable reproduction blocked live network destination: {address!r}")
    return _original_connect(sock, address)


def _guarded_connect_ex(sock: socket.socket, address: object):
    if not _allowed_address(address):
        raise OSError(f"portable reproduction blocked live network destination: {address!r}")
    return _original_connect_ex(sock, address)


def _guarded_create_connection(address: object, *args, **kwargs):
    if not _allowed_address(address):
        raise OSError(f"portable reproduction blocked live network destination: {address!r}")
    return _original_create_connection(address, *args, **kwargs)


socket.socket.connect = _guarded_connect  # type: ignore[assignment]
socket.socket.connect_ex = _guarded_connect_ex  # type: ignore[assignment]
socket.create_connection = _guarded_create_connection  # type: ignore[assignment]
