import typer
from typing import Optional

from .ingest import run as ingest_run
from .model import build_star
from .analytics import run_pca

app = typer.Typer(help="HiloTools Data Pipeline")

@app.command()
def ingest(prefer_gdrive: bool = True, processed_dir: str = "data/processed", config_path: str = "config/config.yml"):
    res = ingest_run(output_dir=processed_dir, config_path=config_path, prefer_gdrive=prefer_gdrive)
    typer.echo(res)

@app.command("model")
def model_cmd(processed_dir: str = "data/processed", warehouse_path: str = "data/warehouse/warehouse.db"):
    res = build_star(processed_dir=processed_dir, warehouse_path=warehouse_path)
    typer.echo(res)

@app.command()
def analytics(warehouse_path: str = "data/warehouse/warehouse.db", out_dir: str = "report", n_components: int = 5):
    res = run_pca(warehouse_path=warehouse_path, out_dir=out_dir, n_components=n_components)
    typer.echo(res)

@app.command("run-all")
def run_all(prefer_gdrive: bool = True, processed_dir: str = "data/processed", config_path: str = "config/config.yml",
            warehouse_path: str = "data/warehouse/warehouse.db", out_dir: str = "report", n_components: int = 5):
    ingest_run(output_dir=processed_dir, config_path=config_path, prefer_gdrive=prefer_gdrive)
    build_star(processed_dir=processed_dir, warehouse_path=warehouse_path)
    res = run_pca(warehouse_path=warehouse_path, out_dir=out_dir, n_components=n_components)
    typer.echo(res)

if __name__ == "__main__":
    app()