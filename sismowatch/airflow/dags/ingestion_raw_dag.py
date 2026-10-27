"""
DAG de Ingesta Raw: USGS Earthquake Feed -> Capa Bronze (PostgreSQL)
Plataforma: SismoWatch
Sprint: 2 - Ingesta Bronze
"""
from datetime import datetime, timedelta
import logging
import os
import requests
import psycopg2
from psycopg2.extras import Json, execute_batch

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
REQUEST_TIMEOUT_SECONDS = 10


def get_db_connection():
    """Retorna una conexión activa a la base de datos PostgreSQL usando variables de entorno."""
    return psycopg2.connect(
        user=os.getenv("POSTGRES_USER", "sismo_user"),
        password=os.getenv("POSTGRES_PASSWORD", "sismo_password"),
        dbname=os.getenv("POSTGRES_DB", "sismowatch"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )


@dag(
    dag_id="usgs_ingestion_raw",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineer",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["bronze", "raw", "usgs", "sismowatch"],
    doc_md="""
    ### Pipeline de Ingesta Raw - SismoWatch
    Ingesta cada 5 minutos el feed GeoJSON de USGS a la capa Bronze (`raw.earthquake_events`).
    Garantiza idempotencia mediante upsert sobre la llave primaria `id`.
    """,
)
def usgs_ingestion_raw():

    @task
    def setup_table() -> None:
        """Asegura la existencia de la tabla raw.earthquake_events en PostgreSQL."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw.earthquake_events (
            id VARCHAR PRIMARY KEY,
            payload JSONB NOT NULL,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        logger.info("Verificando existencia de la tabla raw.earthquake_events...")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
            conn.commit()
        logger.info("Tabla raw.earthquake_events asegurada correctamente.")

    @task
    def fetch_and_store() -> int:
        """
        Consume el feed de USGS con timeout controlado de 10s y realiza un upsert idempotente
        en la tabla raw.earthquake_events para cada feature recibido.
        """
        logger.info("Consultando feed GeoJSON de USGS: %s", USGS_FEED_URL)
        try:
            response = requests.get(USGS_FEED_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
        except requests.exceptions.Timeout as err:
            logger.error("Timeout al conectar con la API de USGS tras %ss: %s", REQUEST_TIMEOUT_SECONDS, err)
            raise AirflowException(f"USGS API Timeout: la petición superó los {REQUEST_TIMEOUT_SECONDS} segundos.") from err
        except requests.exceptions.RequestException as err:
            logger.error("Error en la petición HTTP a USGS: %s", err)
            raise AirflowException(f"Error de conexión con la API de USGS: {err}") from err

        data = response.json()
        features = data.get("features", [])
        total_events = len(features)
        logger.info("Total de eventos recibidos en el payload: %d", total_events)

        if not features:
            logger.warning("No se recibieron eventos en la última hora.")
            return 0

        # Preparar registros para inserción en bloque
        records = [
            (event["id"], Json(event))
            for event in features
            if "id" in event and event["id"] is not None
        ]

        upsert_query = """
        INSERT INTO raw.earthquake_events (id, payload, ingested_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE SET
            payload = EXCLUDED.payload,
            ingested_at = EXCLUDED.ingested_at;
        """

        logger.info("Ejecutando upsert para %d registros en raw.earthquake_events...", len(records))
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, upsert_query, records, page_size=100)
            conn.commit()

        logger.info("Upsert finalizado exitosamente. %d registros procesados.", len(records))
        return len(records)

    # Trigger del DAG de Transformación y Calidad (Sprint 3)
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_quality",
        trigger_dag_id="usgs_transform_quality",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Definir dependencias entre tareas
    setup_table() >> fetch_and_store() >> trigger_transform


# Instanciar el DAG
usgs_ingestion_raw_dag = usgs_ingestion_raw()
