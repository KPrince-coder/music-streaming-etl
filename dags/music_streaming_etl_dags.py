from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago, timedelta
import pandas as pd
import io
import logging

# Constants
S3_CONN_ID = "aws_s3_conn"
RDS_CONN_ID = "rds_conn"
REDSHIFT_CONN_ID = "redshift_conn"
S3_BUCKET = "music-streaming-lab"
S3_KEYS = ["streams1.csv", "streams2.csv", "streams3.csv"]
COPY_OPTIONS = [
    "CSV",
    "IGNOREHEADER 1",
    "TIMEFORMAT 'auto'",
    "TRUNCATECOLUMNS",
    "MAXERROR 0",  # Fail on any error
    "BLANKSASNULL",
    "EMPTYASNULL",
    "COMPUPDATE OFF",
]


# Define all functions first
def extract_rds_data():
    rds_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)
    users_df = rds_hook.get_pandas_df(
        "SELECT user_id, user_name, user_age, user_country, created_at FROM users;"
    )
    songs_df = rds_hook.get_pandas_df(
        "SELECT track_id, artists, album_name, track_name, popularity, duration_ms, track_genre FROM songs;"
    )
    users_df.to_csv("/tmp/users.csv", index=False)
    songs_df.to_csv("/tmp/songs.csv", index=False)


def extract_s3_data():
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    dataframes = []
    for key in S3_KEYS:
        file_obj = s3_hook.get_key(key, bucket_name=S3_BUCKET)
        file_content = file_obj.get()["Body"].read()
        dataframes.append(
            pd.read_csv(io.BytesIO(file_content), parse_dates=["listen_time"])
        )
    streaming_df = pd.concat(dataframes, ignore_index=True)
    streaming_df.to_csv("/tmp/streaming_data.csv", index=False)


def validate_data(**context):
    """Validate data quality after extraction"""
    try:
        rds_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)

        # Check users table
        users_validation = rds_hook.get_records("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_ids,
                COUNT(CASE WHEN user_name IS NULL THEN 1 END) as null_usernames,
                COUNT(CASE WHEN user_country IS NULL THEN 1 END) as null_countries
            FROM users;
        """)

        if users_validation[0][0] == 0:
            raise ValueError("Users table is empty")

        if users_validation[0][1] > 0:
            raise ValueError(
                f"Found {users_validation[0][1]} NULL user_ids in users table"
            )

        # Check songs table
        songs_validation = rds_hook.get_records("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN track_id IS NULL THEN 1 END) as null_track_ids,
                COUNT(CASE WHEN track_name IS NULL THEN 1 END) as null_track_names
            FROM songs;
        """)

        if songs_validation[0][0] == 0:
            raise ValueError("Songs table is empty")

        if songs_validation[0][1] > 0:
            raise ValueError(
                f"Found {songs_validation[0][1]} NULL track_ids in songs table"
            )

        # Validate S3 data
        streaming_df = pd.read_csv("/tmp/streaming_data.csv")
        if streaming_df.empty:
            raise ValueError("No streaming data found in extracted S3 files")

        null_counts = (
            streaming_df[["user_id", "track_id", "listen_time"]].isnull().sum()
        )
        if null_counts.any():
            raise ValueError(
                f"Found NULL values in streaming data: {null_counts.to_dict()}"
            )

        logging.info("Data validation passed successfully")
        return True

    except Exception as e:
        logging.error(f"Data validation failed: {str(e)}")
        raise


def compute_kpis():
    users_df = pd.read_csv("/tmp/users.csv")
    songs_df = pd.read_csv("/tmp/songs.csv")
    streaming_df = pd.read_csv("/tmp/streaming_data.csv", parse_dates=["listen_time"])

    merged_df = streaming_df.merge(songs_df, on="track_id", how="left")
    merged_df = merged_df.merge(users_df, on="user_id", how="left")

    genre_kpis = (
        merged_df.groupby("track_genre")
        .agg(
            listen_count=("track_id", "count"),
            avg_track_duration=("duration_ms", "mean"),
            most_popular_track=(
                "track_name",
                lambda x: x.mode()[0] if not x.mode().empty else None,
            ),
        )
        .reset_index()
    )

    merged_df["hour"] = merged_df["listen_time"].dt.hour
    hourly_kpis = (
        merged_df.groupby("hour")
        .agg(
            unique_listeners=("user_id", "nunique"),
            top_artists=("artists", lambda x: list(x.value_counts().index[:5])),
            track_diversity_index=("track_id", lambda x: x.nunique() / len(x)),
        )
        .reset_index()
    )

    genre_kpis.to_csv("/tmp/genre_kpis.csv", index=False)
    hourly_kpis.to_csv("/tmp/hourly_kpis.csv", index=False)


def validate_kpis(**context):
    """Validate computed KPIs before loading to Redshift"""
    try:
        genre_kpis = pd.read_csv("/tmp/genre_kpis.csv")
        hourly_kpis = pd.read_csv("/tmp/hourly_kpis.csv")

        # Validate genre KPIs
        if genre_kpis.empty:
            raise ValueError("Genre KPIs dataframe is empty")

        if genre_kpis["listen_count"].isnull().any():
            raise ValueError("Found NULL values in genre KPIs listen_count")

        # Validate hourly KPIs
        if hourly_kpis.empty:
            raise ValueError("Hourly KPIs dataframe is empty")

        if not hourly_kpis["hour"].between(0, 23).all():
            raise ValueError("Invalid hour values in hourly KPIs")

        if hourly_kpis["unique_listeners"].isnull().any():
            raise ValueError("Found NULL values in hourly KPIs unique_listeners")

        logging.info("KPI validation passed successfully")
        return True

    except Exception as e:
        logging.error(f"KPI validation failed: {str(e)}")
        raise


def test_redshift_connection(**context):
    """Test Redshift connection and cluster availability"""
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # Drop existing tables if they exist - execute separately
        cursor.execute("DROP TABLE IF EXISTS public.genre_kpis;")
        conn.commit()

        cursor.execute("DROP TABLE IF EXISTS public.hourly_kpis;")
        conn.commit()

        # Create genre_kpis table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.genre_kpis (
                track_genre VARCHAR(255),
                listen_count BIGINT,
                avg_track_duration DOUBLE PRECISION,
                most_popular_track VARCHAR(255)
            );
        """)
        conn.commit()

        # Create hourly_kpis table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.hourly_kpis (
                hour INTEGER,
                unique_listeners BIGINT,
                top_artists TEXT,
                track_diversity_index DOUBLE PRECISION
            );
        """)
        conn.commit()

        cursor.close()
        conn.close()
        logging.info("Successfully created/verified Redshift tables")
        return True

    except Exception as e:
        logging.error(f"Failed to connect to Redshift: {str(e)}")
        raise


def upload_to_s3():
    """Upload KPI files to S3"""
    try:
        # Validate CSV files before upload
        genre_kpis = pd.read_csv("/tmp/genre_kpis.csv")
        hourly_kpis = pd.read_csv("/tmp/hourly_kpis.csv")

        # Convert numeric columns to appropriate types
        genre_kpis["listen_count"] = genre_kpis["listen_count"].astype("Int64")
        genre_kpis["avg_track_duration"] = genre_kpis["avg_track_duration"].astype(
            "float64"
        )

        hourly_kpis["hour"] = hourly_kpis["hour"].astype("Int64")
        hourly_kpis["unique_listeners"] = hourly_kpis["unique_listeners"].astype(
            "Int64"
        )
        hourly_kpis["track_diversity_index"] = hourly_kpis[
            "track_diversity_index"
        ].astype("float64")

        # Save with correct types
        genre_kpis.to_csv("/tmp/genre_kpis.csv", index=False)
        hourly_kpis.to_csv("/tmp/hourly_kpis.csv", index=False)

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        # Upload files
        for filename, key in [
            ("/tmp/genre_kpis.csv", "genre_kpis.csv"),
            ("/tmp/hourly_kpis.csv", "hourly_kpis.csv"),
        ]:
            s3_hook.load_file(
                filename=filename, key=key, bucket_name=S3_BUCKET, replace=True
            )
            logging.info(f"Successfully uploaded {filename} to S3")

        return True

    except Exception as e:
        logging.error(f"Failed to upload files to S3: {str(e)}")
        raise


# Define DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 3,
}

dag = DAG(
    "etl_rds_s3_to_redshift_kpis",
    default_args=default_args,
    description="ETL pipeline to extract data, compute KPIs, and load into Redshift",
    schedule_interval="@hourly",
    catchup=False,
)

# Define tasks
start_task = EmptyOperator(task_id="start", dag=dag)

extract_rds_task = PythonOperator(
    task_id="extract_rds_data", python_callable=extract_rds_data, dag=dag
)

extract_s3_task = PythonOperator(
    task_id="extract_s3_data", python_callable=extract_s3_data, dag=dag
)

validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

compute_kpis_task = PythonOperator(
    task_id="compute_kpis", python_callable=compute_kpis, dag=dag
)

validate_kpis_task = PythonOperator(
    task_id="validate_kpis",
    python_callable=validate_kpis,
    provide_context=True,
    dag=dag,
)

load_genre_kpis_task = S3ToRedshiftOperator(
    task_id="load_genre_kpis",
    schema="public",
    table="genre_kpis",
    s3_bucket=S3_BUCKET,
    s3_key="genre_kpis.csv",
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_conn_id=S3_CONN_ID,
    copy_options=COPY_OPTIONS,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=30),
)

load_hourly_kpis_task = S3ToRedshiftOperator(
    task_id="load_hourly_kpis",
    schema="public",
    table="hourly_kpis",
    s3_bucket=S3_BUCKET,
    s3_key="hourly_kpis.csv",
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_conn_id=S3_CONN_ID,
    copy_options=COPY_OPTIONS,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=30),
)

end_task = EmptyOperator(task_id="end", dag=dag)

test_redshift_conn_task = PythonOperator(
    task_id="test_redshift_connection",
    python_callable=test_redshift_connection,
    provide_context=True,
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=2),
)

upload_to_s3_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

# Define task dependencies
(
    start_task
    >> [extract_rds_task, extract_s3_task]
    >> validate_task
    >> compute_kpis_task
    >> validate_kpis_task
    >> test_redshift_conn_task
    >> upload_to_s3_task
    >> [load_genre_kpis_task, load_hourly_kpis_task]
    >> end_task
)
