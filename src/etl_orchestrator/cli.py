"""Command-line interface for the ETL Orchestration Framework.

Milestone 1 provides only the ``version`` command. Workflow commands
(``workflow list|validate|run|status|history``, ``run show|cancel``) are
added in Milestone 8.
"""

import click

from etl_orchestrator import __version__


@click.group()
@click.version_option(version=__version__, prog_name="orchestrator")
def main() -> None:
    """ETL Orchestration Framework command-line interface."""


@main.command()
def version() -> None:
    """Print the framework version."""
    click.echo(f"orchestrator {__version__}")


if __name__ == "__main__":  # pragma: no cover
    main()
