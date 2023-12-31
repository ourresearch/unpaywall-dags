import gzip
import logging
import os

from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import heroku3
import pendulum

JSON_STAGING_TABLE = "daily_export_staging_test"
DAILY_EXPORT_HISTORY = "daily_export_dates_test"


@task()
def extract_changes(execution_date, prev_execution_date):
    start_date = prev_execution_date
    end_date = execution_date

    logging.info(f"Extracting daily snapshot changes from {start_date} to {end_date}")

    pg_hook = PostgresHook(postgres_conn_id="UNPAYWALL_POSTGRES")

    truncate_sql = f"TRUNCATE {JSON_STAGING_TABLE};"
    logging.info(f"Truncating {JSON_STAGING_TABLE}")
    pg_hook.run(truncate_sql)

    insert_sql = f"""
        INSERT INTO {JSON_STAGING_TABLE} (
            SELECT pub.id, pub.updated, pub.last_changed_date, pub.response_jsonb
            FROM pub 
            LEFT JOIN {DAILY_EXPORT_HISTORY} history USING (id)
            WHERE pub.last_changed_date BETWEEN %s AND %s
            AND pub.updated > '1043-01-01'::timestamp
            AND (history.last_exported_update IS NULL OR history.last_exported_update < pub.last_changed_date)
        );
        """

    logging.info(f"Inserting daily snapshot changes from {start_date} to {end_date}")
    pg_hook.run(insert_sql, parameters=(start_date, end_date))

    logging.info("Finished extracting daily snapshot changes")


@task
def export_gzip_and_upload_to_s3(execution_date):
    execution_date_dt = pendulum.parse(execution_date)
    pg_hook = PostgresHook(postgres_conn_id="UNPAYWALL_POSTGRES")
    s3_hook = S3Hook(aws_conn_id="UNPAYWALL_S3")

    logging.info(f"Exporting daily snapshot changes for {execution_date_dt}")

    filename = f"changed_dois_with_versions_{execution_date_dt.strftime('%Y-%m-%dT%H%M%S')}.jsonl.gz"
    temp_filepath = f"/tmp/{filename}"

    with gzip.open(temp_filepath, "wb") as gz:
        logging.info(f"Writing to {temp_filepath}")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql = f"COPY (SELECT response_jsonb FROM {JSON_STAGING_TABLE} WHERE response_jsonb IS NOT NULL) TO STDOUT"
        cursor.copy_expert(sql, gz)
        cursor.close()

    logging.info(f"Uploading {temp_filepath} to S3")

    s3_hook.load_file(
        filename=temp_filepath,
        key=filename,
        bucket_name="unpaywall-daily-data-feed-test",
        replace=True,
    )

    os.remove(temp_filepath)


@task()
def update_last_exported_dates():
    pg_hook = PostgresHook(postgres_conn_id="UNPAYWALL_POSTGRES")

    update_sql = f"""
        INSERT INTO {DAILY_EXPORT_HISTORY} (id, last_exported_update) (
            SELECT id, last_changed_date FROM {JSON_STAGING_TABLE}
        )
        ON CONFLICT (id) DO UPDATE SET last_exported_update = excluded.last_exported_update;

        TRUNCATE {JSON_STAGING_TABLE};
        """

    pg_hook.run(update_sql)


@task()
def update_changefile_dicts():
    heroku_api_key = Variable.get("HEROKU_API_KEY")
    heroku_conn = heroku3.from_key(heroku_api_key)
    app = heroku_conn.apps()["oadoi"]
    app.run_command("python cache_changefile_dicts.py", attach=False)


@dag(
    schedule_interval="@daily", start_date=pendulum.datetime(2023, 9, 4), catchup=False
)
def export_daily_changefile():
    extract_task = extract_changes(
        execution_date="{{ execution_date }}",
        prev_execution_date="{{ prev_execution_date }}",
    )
    export_task = export_gzip_and_upload_to_s3(execution_date="{{ execution_date }}")
    update_dates_task = update_last_exported_dates()
    # update_dicts_task = update_changefile_dicts()

    extract_task >> export_task >> update_dates_task


export_daily_changefile_dag = export_daily_changefile()
