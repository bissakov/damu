import os
import platform
import sys
import tempfile
from pathlib import Path
from typing import Tuple

import click

project_folder = Path(__file__).resolve().parent.parent
sys.path.append(str(project_folder))

from src.utils.db_manager import DatabaseManager

project_folder = Path(__file__).resolve().parent.parent
database = project_folder / "resources" / "database.sqlite"
db = DatabaseManager(database)


@click.command()
@click.option("--contract_id", required=False)
@click.option("--random", is_flag=True, help="Select a random contract ID from the database")
@click.option("--is_shifted", is_flag=True)
def main(
    contract_id: str = None,
    random: bool = False,
    is_shifted: bool = False,
) -> None:
    if platform.system() != "Windows":
        click.echo(message=f"Non-Windows systems are not supported", err=True)
        return

    if not random and not contract_id:
        contract_id = click.prompt("Contract ID")

    with db.connect() as cursor:
        if random:
            if is_shifted:
                cursor.execute("SELECT id, shifted_macro FROM macros ORDER BY RANDOM() LIMIT 1")
            else:
                cursor.execute("SELECT id, macro FROM macros ORDER BY RANDOM() LIMIT 1")
        else:
            if is_shifted:
                cursor.execute(
                    """
                    SELECT id, shifted_macro FROM macros
                    WHERE id = ?
                """,
                    (contract_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, macro FROM macros
                    WHERE id = ?
                """,
                    (contract_id,),
                )

        result: Tuple[str, bytes] = cursor.fetchone()
        contract_id, macro_buffer = result

    if not result:
        click.echo(message=f"Macro of {contract_id!r} not found!", err=True)
        return

    with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".xlsx") as tmp:
        tmp.write(macro_buffer)
        tmp_path = Path(tmp.name)
        click.echo(f"Temporary file created: {tmp_path}")

        click.echo(f"Opening macro of {contract_id!r}...")
        try:
            os.startfile(tmp_path)
        except Exception as e:
            click.echo(f"Error opening file: {e}", err=True)


if __name__ == "__main__":
    main()
