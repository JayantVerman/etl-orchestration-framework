"""Test the package foundation: version, metadata, and importability."""

import etl_orchestrator
from etl_orchestrator.cli import main as cli_main


def test_package_has_version() -> None:
    """The package must expose a valid semantic version string."""
    assert isinstance(etl_orchestrator.__version__, str)
    parts = etl_orchestrator.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


def test_package_imports_cleanly() -> None:
    """The package must be importable from the installed distribution."""
    assert etl_orchestrator is not None


def test_package_docstring_exists() -> None:
    """The package must document its purpose."""
    assert "orchestration" in (etl_orchestrator.__doc__ or "").lower()


def test_cli_entry_point_importable() -> None:
    """The console-script entry point target must be importable."""
    assert callable(cli_main)


def test_cli_version_command() -> None:
    """The ``version`` command must report the package version."""
    from click.testing import CliRunner

    result = CliRunner().invoke(cli_main, ["version"])
    assert result.exit_code == 0
    assert etl_orchestrator.__version__ in result.output
