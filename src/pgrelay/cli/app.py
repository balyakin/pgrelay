"""Typer CLI root."""

import typer

from pgrelay.cli.commands_api import run_api
from pgrelay.cli.commands_doctor import doctor
from pgrelay.cli.commands_jobs import drain, purge, replay
from pgrelay.cli.commands_migrate import migrate_app
from pgrelay.cli.commands_worker import run_worker

app = typer.Typer(help="PgRelay CLI")
app.command("api")(run_api)
app.command("worker")(run_worker)
app.command("doctor")(doctor)
app.command("replay")(replay)
app.command("drain")(drain)
app.command("purge")(purge)
app.add_typer(migrate_app, name="migrate")
