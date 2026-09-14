"""Fail-closed outbound-network guard for portable reproduction only.

Activate by prepending this directory to PYTHONPATH. Loopback/Unix-socket traffic
is allowed so the local ASGI process and deterministic replay servers can talk.
External DNS, reverse DNS, TCP/UDP destinations fail before escape and are recorded
as structured JSONL evidence when PORTABLE_REPRO_NETWORK_AUDIT is configured.
"""
from __future__ import annotations

import ipaddress
import json
import os
import socket
import threading
import time
from pathlib import Path

_original_getaddrinfo = socket.getaddrinfo
_original_gethostbyname = socket.gethostbyname
_original_gethostbyname_ex = socket.gethostbyname_ex
_original_gethostbyaddr = socket.gethostbyaddr
_original_getnameinfo = socket.getnameinfo
_original_connect = socket.socket.connect
_original_connect_ex = socket.socket.connect_ex
_original_create_connection = socket.create_connection
_original_sendto = socket.socket.sendto
_audit_lock = threading.Lock()


def _allowed_host(host: object) -> bool:
    if host is None:
        return True
    if isinstance(host, bytes):
        try:
            host = host.decode("ascii", "strict")
        except UnicodeDecodeError:
            return False
    text = str(host).strip().lower().rstrip(".")
    if text == "localhost" or text.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(text).is_loopback
    except ValueError:
        return False


def _allowed_address(address: object, *, family: int | None = None) -> bool:
    if family == socket.AF_UNIX:
        return isinstance(address, (str, bytes))
    if isinstance(address, tuple) and address:
        return _allowed_host(address[0])
    # Strings are Unix-socket paths only when the socket family says AF_UNIX.
    return False


def _audit(kind: str, target: object) -> None:
    path = os.environ.get("PORTABLE_REPRO_NETWORK_AUDIT", "").strip()
    if not path:
        return
    record = {
        "ts_unix": time.time(),
        "pid": os.getpid(),
        "thread_id": threading.get_ident(),
        "thread_name": threading.current_thread().name,
        "decision": "blocked",
        "operation": kind,
        "target": repr(target),
    }
    try:
        audit_path = Path(path)
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
        with _audit_lock, audit_path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
    except OSError as exc:
        # A failed audit sink must never cause a blocked request to be attempted.
        raise OSError(f"portable reproduction network audit failed: {exc}") from exc


def _blocked(kind: str, target: object) -> OSError:
    _audit(kind, target)
    return OSError(f"portable reproduction blocked live {kind}: {target!r}")


def _guarded_getaddrinfo(host, port, *args, **kwargs):
    if not _allowed_host(host):
        raise _blocked("DNS resolution", host)
    return _original_getaddrinfo(host, port, *args, **kwargs)


def _guarded_gethostbyname(host):
    if not _allowed_host(host):
        raise _blocked("DNS gethostbyname", host)
    return _original_gethostbyname(host)


def _guarded_gethostbyname_ex(host):
    if not _allowed_host(host):
        raise _blocked("DNS gethostbyname_ex", host)
    return _original_gethostbyname_ex(host)


def _guarded_gethostbyaddr(host):
    if not _allowed_host(host):
        raise _blocked("reverse DNS gethostbyaddr", host)
    return _original_gethostbyaddr(host)


def _guarded_getnameinfo(sockaddr, flags):
    if not _allowed_address(sockaddr):
        raise _blocked("reverse DNS getnameinfo", sockaddr)
    return _original_getnameinfo(sockaddr, flags)


def _guarded_connect(sock: socket.socket, address: object):
    if not _allowed_address(address, family=sock.family):
        raise _blocked("network destination", address)
    return _original_connect(sock, address)


def _guarded_connect_ex(sock: socket.socket, address: object):
    if not _allowed_address(address, family=sock.family):
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
    if address is not None and not _allowed_address(address, family=sock.family):
        raise _blocked("datagram destination", address)
    return _original_sendto(sock, data, *args, **kwargs)


socket.getaddrinfo = _guarded_getaddrinfo  # type: ignore[assignment]
socket.gethostbyname = _guarded_gethostbyname  # type: ignore[assignment]
socket.gethostbyname_ex = _guarded_gethostbyname_ex  # type: ignore[assignment]
socket.gethostbyaddr = _guarded_gethostbyaddr  # type: ignore[assignment]
socket.getnameinfo = _guarded_getnameinfo  # type: ignore[assignment]
socket.socket.connect = _guarded_connect  # type: ignore[assignment]
socket.socket.connect_ex = _guarded_connect_ex  # type: ignore[assignment]
socket.create_connection = _guarded_create_connection  # type: ignore[assignment]
socket.socket.sendto = _guarded_sendto  # type: ignore[assignment]
