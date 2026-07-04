"""Module entrypoint for PgRelay."""

from pgrelay.cli.app import app


def main() -> None:
    """Run the PgRelay CLI."""
    app()


if __name__ == "__main__":
    main()
