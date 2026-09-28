import logging
import pendulum
import requests
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

SPREADSHEET_URL = "https://www.riccardotommasini.com/assets/files/spreadsheet.xlsx"
DATA_FOLDER = "/opt/airflow/data"
DAGS_FOLDER = "/opt/airflow/dags"
XLSX_PATH = f"{DATA_FOLDER}/data.xlsx"
CSV_PATH = f"{DATA_FOLDER}/data_converted.csv"
FILTERED_CSV_PATH = f"{DATA_FOLDER}/data_c_filtered.csv"
INTAVOLATURE_CSV_PATH = f"{DATA_FOLDER}/intavolature.csv"
COMPOSER_CSV_PATH = f"{DATA_FOLDER}/composer.csv"
# Written under DAGS_FOLDER (already in template_searchpath) so the later insert
# tasks can reference them by filename via SQLExecuteQueryOperator's sql= param.
INTAVOLATURE_SQL_PATH = f"{DAGS_FOLDER}/intavolature_inserts.sql"
COMPOSER_SQL_PATH = f"{DAGS_FOLDER}/composer_inserts.sql"
TIME_FILTER_COLUMN = 28  # 0-indexed -> "Created" column (spreadsheet only has 30 cols; awk's $31 was out of range)
TIME_FILTER_THRESHOLD = 1588612377

# Column names of the source spreadsheet, in order (the header row is dropped by
# _time_filter since "Created"/"Modified" don't coerce to numeric > threshold).
COLUMN_NAMES = [
    "Title",
    "Subtitle",
    "Composer",
    "Orig. composer",
    "Source",
    "Document",
    "Volume",
    "Date",
    "Page",
    "Editor",
    "Encoder",
    "Arranger",
    "Intabulator",
    "Contributor",
    "Concordances",
    "Piece",
    "Section",
    "Type",
    "Key",
    "Difficulty",
    "Ensemble",
    "Part",
    "Remarks",
    "Recording",
    "Facsimile",
    "Fronimo",
    "PDF",
    "Midi",
    "Created",
    "Modified",
]
INTAVOLATURE_COLUMNS = ["Piece", "Type", "Key", "Difficulty", "Date", "Ensemble"]


def _get_spreadsheet():
    logger.info("Downloading spreadsheet from %s", SPREADSHEET_URL)
    resp = requests.get(SPREADSHEET_URL, timeout=60)
    resp.raise_for_status()
    with open(XLSX_PATH, "wb") as f:
        f.write(resp.content)
    logger.info("Saved spreadsheet to %s (%d bytes)", XLSX_PATH, len(resp.content))


def _emptiness_check():
    logger.info("Checking whether %s is empty", FILTERED_CSV_PATH)
    try:
        df = pd.read_csv(FILTERED_CSV_PATH, header=None)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    if len(df) == 0:
        logger.info("Filtered CSV has 0 rows, branching to 'end'")
        return "end"
    logger.info("Filtered CSV has %d rows, branching to 'split'", len(df))
    return "split"


def _split():
    logger.info(
        "Splitting %s into intavolature and composer datasets", FILTERED_CSV_PATH
    )
    df = pd.read_csv(FILTERED_CSV_PATH, header=None, names=COLUMN_NAMES)

    intavolature = df[INTAVOLATURE_COLUMNS]
    intavolature.to_csv(INTAVOLATURE_CSV_PATH, index=False)
    logger.info("Wrote %s with %d rows", INTAVOLATURE_CSV_PATH, len(intavolature))

    composer = df[["Composer"]].drop_duplicates()
    composer.to_csv(COMPOSER_CSV_PATH, index=False)
    logger.info("Wrote %s with %d rows", COMPOSER_CSV_PATH, len(composer))


def _sql_escape(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("'", "''")


def _build_table_sql(df: pd.DataFrame, table_name: str) -> str:
    columns = list(df.columns)
    column_defs = ",\n".join(f'  "{col}" VARCHAR' for col in columns)
    statements = [f"CREATE TABLE IF NOT EXISTS {table_name} (\n{column_defs}\n);\n"]
    for _, row in df.iterrows():
        values = ", ".join(f"'{_sql_escape(row[col])}'" for col in columns)
        statements.append(f"INSERT INTO {table_name} VALUES ({values});")
    return "\n".join(statements) + "\n"


def _create_intavolature_query():
    logger.info(
        "Building create/insert SQL for intavolature from %s", INTAVOLATURE_CSV_PATH
    )
    df = pd.read_csv(INTAVOLATURE_CSV_PATH)
    sql = _build_table_sql(df, "intavolature")
    with open(INTAVOLATURE_SQL_PATH, "w") as f:
        f.write(sql)
    logger.info("Wrote %s with %d insert statements", INTAVOLATURE_SQL_PATH, len(df))


def _create_composer_query():
    logger.info("Building create/insert SQL for composer from %s", COMPOSER_CSV_PATH)
    df = pd.read_csv(COMPOSER_CSV_PATH)
    sql = _build_table_sql(df, "composer")
    with open(COMPOSER_SQL_PATH, "w") as f:
        f.write(sql)
    logger.info("Wrote %s with %d insert statements", COMPOSER_SQL_PATH, len(df))


def _time_filter():
    logger.info(
        "Filtering %s where column %d > %d",
        CSV_PATH,
        TIME_FILTER_COLUMN,
        TIME_FILTER_THRESHOLD,
    )
    df = pd.read_csv(CSV_PATH, header=None)
    timestamps = pd.to_numeric(df[TIME_FILTER_COLUMN], errors="coerce")
    filtered = df[timestamps > TIME_FILTER_THRESHOLD]
    filtered.to_csv(FILTERED_CSV_PATH, header=False, index=False)
    logger.info("Wrote %s with %d/%d rows", FILTERED_CSV_PATH, len(filtered), len(df))


with DAG(
    dag_id="second_dag_stub",
    start_date=START_DATE,
    schedule="0 0 * * *",  # daily at 00:00 UTC
    catchup=False,
    max_active_tasks=1,  # replaces old DAG-level 'concurrency'
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=["/opt/airflow/dags/"],
    tags=["stub"],
) as dag:

    get_spreadsheet = PythonOperator(
        task_id="get_spreadsheet",
        python_callable=_get_spreadsheet,
    )
    transmute_to_csv = BashOperator(
        task_id="transmute_to_csv",
        bash_command=f"xlsx2csv {XLSX_PATH} > {CSV_PATH}",
    )
    time_filter = PythonOperator(
        task_id="time_filter",
        python_callable=_time_filter,
    )

    emptiness_check = BranchPythonOperator(
        task_id="emptiness_check",
        python_callable=_emptiness_check,
    )
    split = PythonOperator(
        task_id="split",
        python_callable=_split,
    )

    create_intavolature_query = PythonOperator(
        task_id="create_intavolature_query",
        python_callable=_create_intavolature_query,
    )
    create_composer_query = PythonOperator(
        task_id="create_composer_query",
        python_callable=_create_composer_query,
    )

    exec_intavolature_query = SQLExecuteQueryOperator(
        task_id="exec_intavolature_query",
        conn_id="project-db",
        sql="intavolature_inserts.sql",
        autocommit=True,
    )
    exec_composer_query = SQLExecuteQueryOperator(
        task_id="exec_composer_query",
        conn_id="project-db",
        sql="composer_inserts.sql",
        autocommit=True,
    )

    join_tasks = EmptyOperator(
        task_id="coalesce_transformations", trigger_rule="all_success"
    )
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Graph (structure preserved)
    get_spreadsheet >> transmute_to_csv >> time_filter >> emptiness_check
    emptiness_check >> [split, end]
    split >> [create_intavolature_query, create_composer_query]
    create_intavolature_query >> exec_intavolature_query
    create_composer_query >> exec_composer_query
    [exec_intavolature_query, exec_composer_query] >> join_tasks >> end
