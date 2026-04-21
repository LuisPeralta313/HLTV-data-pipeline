from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJ = "E:/ProyectoHLTV"

with DAG(
    dag_id="hltv_pipeline",
    schedule="0 9 1,15 * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["hltv", "cs2"],
) as dag:

    scrape_results = BashOperator(
        task_id="scrape_match_results",
        bash_command=(
            f"cd /d {PROJ} && "
            f".venv/Scripts/python -m ingestion.scrapers.results_scraper "
            f"--pages 5 --headless false"
        ),
    )

    scrape_players = BashOperator(
        task_id="scrape_player_stats",
        bash_command=(
            f"cd /d {PROJ} && "
            f".venv/Scripts/python -m ingestion.scrapers.player_stats_scraper "
            f"--headless false"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd /d {PROJ}/dbt_project && "
            f"../.venv/Scripts/dbt run --profiles-dir ."
        ),
    )

    scrape_results >> scrape_players >> dbt_run
