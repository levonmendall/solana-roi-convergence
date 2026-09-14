"""Temporary CI diagnostic hook for PR #394.

Print each pytest node id as execution starts so an otherwise opaque timeout can
identify the exact blocking test. Remove this file before merge.
"""


def pytest_runtest_logstart(nodeid, location):
    print(f"DIAGNOSTIC_TEST_START {nodeid}", flush=True)
