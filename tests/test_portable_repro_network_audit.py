from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "diagnostics" / "portable_repro" / "network_guard" / "sitecustomize.py"


def _run_blocked(expression: str, audit: Path) -> list[dict[str, object]]:
    code = (
        "import importlib.util, socket; "
        f"p={str(GUARD)!r}; "
        "s=importlib.util.spec_from_file_location('portable_network_guard_audit', p); "
        "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
        f"\ntry:\n {expression}\nexcept OSError:\n pass\nelse:\n raise SystemExit(9)"
    )
    env = dict(os.environ)
    env["PORTABLE_REPRO_NETWORK_AUDIT"] = str(audit)
    subprocess.check_call([sys.executable, "-S", "-c", code], env=env)
    return [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]


def test_external_dns_is_blocked_and_audited_before_escape():
    with tempfile.TemporaryDirectory() as td:
        records = _run_blocked("socket.getaddrinfo('example.com', 443)", Path(td) / "audit.jsonl")
    assert len(records) == 1
    assert records[0]["decision"] == "blocked"
    assert records[0]["operation"] == "DNS resolution"
    assert "example.com" in str(records[0]["target"])


def test_external_literal_connect_is_blocked_and_audited():
    with tempfile.TemporaryDirectory() as td:
        records = _run_blocked("socket.create_connection(('8.8.8.8', 53), timeout=0.01)", Path(td) / "audit.jsonl")
    assert len(records) == 1
    assert records[0]["operation"] == "network destination"
    assert "8.8.8.8" in str(records[0]["target"])


def test_secondary_resolvers_are_fail_closed_and_audited():
    expressions = [
        "socket.gethostbyname('example.com')",
        "socket.gethostbyname_ex('example.com')",
        "socket.gethostbyaddr('8.8.8.8')",
        "socket.getnameinfo(('8.8.8.8', 53), 0)",
    ]
    with tempfile.TemporaryDirectory() as td:
        audit = Path(td) / "audit.jsonl"
        for expression in expressions:
            _run_blocked(expression, audit)
        records = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]
    assert len(records) == len(expressions)
    assert all(record["decision"] == "blocked" for record in records)


def test_loopback_and_localhost_remain_allowed():
    code = (
        "import importlib.util, socket; "
        f"p={str(GUARD)!r}; "
        "s=importlib.util.spec_from_file_location('portable_network_guard_loopback_audit', p); "
        "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
        "assert m._allowed_host('localhost'); "
        "assert m._allowed_host('svc.localhost.'); "
        "assert m._allowed_host('127.0.0.1'); "
        "assert m._allowed_host('::1')"
    )
    subprocess.check_call([sys.executable, "-S", "-c", code])
