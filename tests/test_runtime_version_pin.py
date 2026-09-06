from pathlib import Path


EXPECTED_PYTHON = "3.11.16"


def test_repository_and_ci_pin_exact_certified_python_runtime() -> None:
    assert Path(".python-version").read_text(encoding="utf-8").strip() == EXPECTED_PYTHON
    workflow = Path(".github/workflows/test.yml").read_text(encoding="utf-8")
    assert f"python-version: '{EXPECTED_PYTHON}'" in workflow
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
    assert 'requires-python = ">=3.11,<3.12"' in pyproject
