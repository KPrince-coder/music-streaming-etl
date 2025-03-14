"""
Redshift data loading module for music streaming ETL pipeline.
Handles staging and upserting data into Redshift tables.
"""

import pandas as pd
from typing import Dict, Any, List
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


class RedshiftLoaderError(Exception):
    """Custom exception for Redshift loading errors."""

    pass


# SQL statements for creating tables
CREATE_GENRE_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS genre_metrics (
    genre VARCHAR(100) NOT NULL,
    metric_date DATE NOT NULL,
    listen_count INTEGER,
    avg_duration_ms INTEGER,
    popularity_index FLOAT,
    top_tracks JSONB,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (genre, metric_date)
);
"""

CREATE_HOURLY_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS hourly_metrics (
    metric_hour TIMESTAMP NOT NULL,
    unique_listeners INTEGER,
    top_artists JSONB,
    track_diversity_index FLOAT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_hour)
);
"""

CREATE_GENRE_METRICS_STAGING = """
CREATE TABLE IF NOT EXISTS genre_metrics_staging (LIKE genre_metrics);
"""

CREATE_HOURLY_METRICS_STAGING = """
CREATE TABLE IF NOT EXISTS hourly_metrics_staging (LIKE hourly_metrics);
"""


class RedshiftLoader:
    """Handler for loading data into Redshift with staging and upsert support."""

    def __init__(self, redshift_conn_id: str = "redshift_connection"):
        """
        Initialize loader with connection.

        Args:
            redshift_conn_id: Airflow connection ID for Redshift
        """
        self.redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    def initialize_tables(self):
        """Create required Redshift tables if they don't exist."""
        try:
            with self.redshift_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Create main tables
                    cur.execute(CREATE_GENRE_METRICS_TABLE)
                    cur.execute(CREATE_HOURLY_METRICS_TABLE)

                    # Create staging tables
                    cur.execute(CREATE_GENRE_METRICS_STAGING)
                    cur.execute(CREATE_HOURLY_METRICS_STAGING)

                    conn.commit()

            logger.info("Successfully initialized Redshift tables")

        except Exception as e:
            error_msg = f"Failed to initialize Redshift tables: {str(e)}"
            logger.error(error_msg)
            raise RedshiftLoaderError(error_msg)

    def _load_dataframe_to_staging(
        self, df: pd.DataFrame, table_name: str, columns: List[str]
    ):
        """
        Load DataFrame to staging table.

        Args:
            df: DataFrame to load
            table_name: Name of staging table
            columns: List of column names in correct order
        """
        try:
            # Convert DataFrame to list of tuples for copying
            data_tuples = [tuple(row) for row in df[columns].values]

            with self.redshift_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Clear staging table
                    cur.execute(f"TRUNCATE TABLE {table_name}_staging")

                    # Prepare template and execute batch insert
                    template = f"INSERT INTO {table_name}_staging ({','.join(columns)}) VALUES %s"
                    from psycopg2.extras import execute_values

                    execute_values(cur, template, data_tuples)

                    conn.commit()

            logger.info(f"Loaded {len(df)} rows to {table_name}_staging")

        except Exception as e:
            error_msg = f"Failed to load data to staging table {table_name}: {str(e)}"
            logger.error(error_msg)
            raise RedshiftLoaderError(error_msg)

    def _upsert_from_staging(self, table_name: str, key_columns: List[str]):
        """
        Perform upsert from staging to main table.

        Args:
            table_name: Name of target table
            key_columns: List of primary key column names
        """
        try:
            key_conditions = " AND ".join(
                [f"target.{col} = staging.{col}" for col in key_columns]
            )

            non_key_columns = (
                [
                    "listen_count",
                    "avg_duration_ms",
                    "popularity_index",
                    "top_tracks",
                    "last_updated",
                ]
                if table_name == "genre_metrics"
                else [
                    "unique_listeners",
                    "top_artists",
                    "track_diversity_index",
                    "last_updated",
                ]
            )

            update_sets = ", ".join(
                [f"{col} = staging.{col}" for col in non_key_columns]
            )

            merge_sql = f"""
                DELETE FROM {table_name} 
                USING {table_name}_staging staging
                WHERE {key_conditions};
                
                INSERT INTO {table_name}
                SELECT * FROM {table_name}_staging;
            """

            with self.redshift_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(merge_sql)
                    conn.commit()

            logger.info(f"Successfully performed upsert on {table_name}")

        except Exception as e:
            error_msg = f"Failed to upsert data for {table_name}: {str(e)}"
            logger.error(error_msg)
            raise RedshiftLoaderError(error_msg)

    def load_genre_metrics(self, genre_metrics: Dict[str, Any], metric_date: datetime):
        """
        Load genre metrics into Redshift.

        Args:
            genre_metrics: Dictionary containing genre-level metrics
            metric_date: Date for which metrics were computed
        """
        try:
            # Convert metrics to DataFrame
            records = []
            for genre in genre_metrics["listen_counts"].keys():
                records.append(
                    {
                        "genre": genre,
                        "metric_date": metric_date.date(),
                        "listen_count": genre_metrics["listen_counts"].get(genre, 0),
                        "avg_duration_ms": int(
                            genre_metrics["avg_durations"].get(genre, 0)
                        ),
                        "popularity_index": genre_metrics["popularity_indices"].get(
                            genre, 0.0
                        ),
                        "top_tracks": genre_metrics["top_tracks"].get(genre, []),
                        "last_updated": datetime.now(),
                    }
                )

            df = pd.DataFrame.from_records(records)

            # Load to staging and upsert
            columns = [
                "genre",
                "metric_date",
                "listen_count",
                "avg_duration_ms",
                "popularity_index",
                "top_tracks",
                "last_updated",
            ]
            self._load_dataframe_to_staging(df, "genre_metrics", columns)
            self._upsert_from_staging("genre_metrics", ["genre", "metric_date"])

        except Exception as e:
            error_msg = f"Failed to load genre metrics: {str(e)}"
            logger.error(error_msg)
            raise RedshiftLoaderError(error_msg)

    def load_hourly_metrics(
        self, hourly_metrics: Dict[str, Any], metric_hour: datetime
    ):
        """
        Load hourly metrics into Redshift.

        Args:
            hourly_metrics: Dictionary containing hourly metrics
            metric_hour: Hour for which metrics were computed
        """
        try:
            # Create DataFrame with one row
            df = pd.DataFrame(
                [
                    {
                        "metric_hour": metric_hour,
                        "unique_listeners": hourly_metrics["unique_listeners"].get(
                            metric_hour.strftime("%Y-%m-%d %H:00:00"), 0
                        ),
                        "top_artists": hourly_metrics["top_artists"].get(
                            metric_hour.strftime("%Y-%m-%d %H:00:00"), []
                        ),
                        "track_diversity_index": hourly_metrics["track_diversity"].get(
                            metric_hour.strftime("%Y-%m-%d %H:00:00"), 0.0
                        ),
                        "last_updated": datetime.now(),
                    }
                ]
            )

            # Load to staging and upsert
            columns = [
                "metric_hour",
                "unique_listeners",
                "top_artists",
                "track_diversity_index",
                "last_updated",
            ]
            self._load_dataframe_to_staging(df, "hourly_metrics", columns)
            self._upsert_from_staging("hourly_metrics", ["metric_hour"])

        except Exception as e:
            error_msg = f"Failed to load hourly metrics: {str(e)}"
            logger.error(error_msg)
            raise RedshiftLoaderError(error_msg)
