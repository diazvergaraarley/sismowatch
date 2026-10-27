"""
DAG de Transformación Medallón y Calidad de Datos (PySpark)
Plataforma: SismoWatch
Sprint: 3 - Staging, Curated y Data Quality
"""
from datetime import datetime, timedelta
import json
import logging
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


def get_db_connection():
    """Retorna una conexión activa a PostgreSQL usando variables de entorno."""
    return psycopg2.connect(
        user=os.getenv("POSTGRES_USER", "sismo_user"),
        password=os.getenv("POSTGRES_PASSWORD", "sismo_password"),
        dbname=os.getenv("POSTGRES_DB", "sismowatch"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )


def ensure_target_tables():
    """Asegura que las tablas de staging y curated existan en PostgreSQL antes de procesar."""
    ddl_queries = """
    -- Tabla de datos limpios en Staging (Capa Plata)
    CREATE TABLE IF NOT EXISTS staging.earthquakes_clean (
        id VARCHAR PRIMARY KEY,
        mag DOUBLE PRECISION,
        place VARCHAR,
        time TIMESTAMP,
        updated TIMESTAMP,
        sig INTEGER,
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        depth DOUBLE PRECISION,
        ingested_at TIMESTAMP,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Tabla de métricas de calidad de datos por ejecución
    CREATE TABLE IF NOT EXISTS staging.quality_metrics (
        id SERIAL PRIMARY KEY,
        dag_run_id VARCHAR,
        total_evaluated INT,
        total_passed INT,
        total_rejected INT,
        evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Tabla de consumo final en Curated (Capa Oro)
    CREATE TABLE IF NOT EXISTS curated.earthquakes (
        id VARCHAR PRIMARY KEY,
        mag DOUBLE PRECISION,
        place VARCHAR,
        time TIMESTAMP,
        updated TIMESTAMP,
        sig INTEGER,
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        depth DOUBLE PRECISION,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_queries)
        conn.commit()
    logger.info("Tablas de staging y curated verificadas con éxito.")


@dag(
    dag_id="usgs_transform_quality",
    schedule=None,  # Disparado automáticamente por usgs_ingestion_raw
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["silver", "gold", "staging", "curated", "pyspark", "quality"],
    doc_md="""
    ### Pipeline de Procesamiento Medallón y Calidad de Datos (PySpark)
    1. Extrae eventos de la capa Bronze (`raw.earthquake_events`).
    2. Aplica transformaciones y normalización espacial/temporal en PySpark.
    3. Evalúa reglas de calidad (completitud de ID, coordenadas y rangos válidos [-90,90] y [-180,180]).
    4. Carga registros limpios a `staging.earthquakes_clean` y `curated.earthquakes`.
    5. Descarta de memoria los registros que no superan las reglas de calidad.
    6. Registra métricas de observabilidad en `staging.quality_metrics`.
    """,
)
def usgs_transform_quality():

    @task
    def process_medallion_quality(**context) -> dict:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            DoubleType,
            LongType,
            IntegerType,
            ArrayType,
        )

        dag_run_id = context.get("run_id", f"manual_{datetime.utcnow().isoformat()}")
        logger.info("Iniciando procesamiento Medallón para run: %s", dag_run_id)

        # 1. Asegurar que las tablas de destino existen
        ensure_target_tables()

        # 2. Extraer registros nuevos o actualizados de raw.earthquake_events
        fetch_raw_query = """
        SELECT r.id, r.payload, r.ingested_at
        FROM raw.earthquake_events r
        LEFT JOIN staging.earthquakes_clean s ON r.id = s.id
        WHERE s.id IS NULL
           OR s.ingested_at < r.ingested_at;
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(fetch_raw_query)
                raw_rows = cur.fetchall()

        total_new_records = len(raw_rows)
        logger.info("Total registros brutos pendientes de procesamiento: %d", total_new_records)

        if total_new_records == 0:
            logger.info("No hay registros nuevos o actualizados para procesar.")
            return {"total_evaluated": 0, "total_passed": 0, "total_rejected": 0}

        # 3. Inicializar sesión de PySpark
        logger.info("Inicializando SparkSession local...")
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("SismoWatch_Quality")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )

        try:
            # Esquema robusto predefinido para evitar AnalysisException por campos ausentes en feeds crudos
            usgs_schema = StructType([
                StructField("id", StringType(), True),
                StructField("raw_id", StringType(), True),
                StructField("ingested_at", StringType(), True),
                StructField("raw_payload_str", StringType(), True),
                StructField("properties", StructType([
                    StructField("mag", DoubleType(), True),
                    StructField("place", StringType(), True),
                    StructField("time", LongType(), True),
                    StructField("updated", LongType(), True),
                    StructField("sig", IntegerType(), True),
                ]), True),
                StructField("geometry", StructType([
                    StructField("coordinates", ArrayType(DoubleType()), True),
                ]), True),
            ])

            # Preparar lista de JSON para el lector de PySpark
            json_payloads = [
                json.dumps({
                    "raw_id": row[0],
                    "ingested_at": str(row[2]),
                    "raw_payload_str": json.dumps(row[1]),
                    **row[1]
                })
                for row in raw_rows
            ]

            rdd = spark.sparkContext.parallelize(json_payloads)
            df_raw = spark.read.schema(usgs_schema).json(rdd)

            # 4. Extraer campos requeridos y castear tipos
            # properties: mag, place, time (epoch ms -> Timestamp), updated (epoch ms -> Timestamp), sig
            # geometry.coordinates: [longitude, latitude, depth]
            df_extracted = df_raw.select(
                F.coalesce(F.col("id"), F.col("raw_id")).alias("id"),
                F.col("properties.mag").cast(DoubleType()).alias("mag"),
                F.col("properties.place").cast(StringType()).alias("place"),
                F.to_timestamp(F.from_unixtime(F.col("properties.time") / 1000)).alias("time"),
                F.to_timestamp(F.from_unixtime(F.col("properties.updated") / 1000)).alias("updated"),
                F.col("properties.sig").cast(IntegerType()).alias("sig"),
                F.col("geometry.coordinates")[0].cast(DoubleType()).alias("longitude"),
                F.col("geometry.coordinates")[1].cast(DoubleType()).alias("latitude"),
                F.col("geometry.coordinates")[2].cast(DoubleType()).alias("depth"),
                F.to_timestamp(F.col("ingested_at")).alias("ingested_at"),
            )

            # 5. Aplicar Reglas de Calidad
            # Regla a: id no nulo ni vacío
            id_invalid = F.col("id").isNull() | (F.trim(F.col("id")) == "")
            # Regla b: time no nulo
            time_invalid = F.col("time").isNull()
            # Regla c: coordenadas no nulas
            coords_invalid = F.col("latitude").isNull() | F.col("longitude").isNull() | F.col("depth").isNull()
            # Regla d: latitud entre -90 y 90
            lat_invalid = (F.col("latitude") < -90.0) | (F.col("latitude") > 90.0)
            # Regla e: longitud entre -180 y 180
            lon_invalid = (F.col("longitude") < -180.0) | (F.col("longitude") > 180.0)

            # Determinar validez del registro
            is_invalid_expr = id_invalid | time_invalid | coords_invalid | lat_invalid | lon_invalid

            df_evaluated = df_extracted.withColumn("is_valid", ~is_invalid_expr)

            # Dividir en válidos e inválidos para métricas
            df_valid = df_evaluated.filter(F.col("is_valid") == True)
            df_invalid = df_evaluated.filter(F.col("is_valid") == False)

            total_evaluated = df_evaluated.count()
            total_passed = df_valid.count()
            total_rejected = df_invalid.count()

            logger.info(
                "Evaluación de Calidad: Evaluados=%d, Aprobados=%d, Rechazados (descartados)=%d",
                total_evaluated,
                total_passed,
                total_rejected,
            )

            # 6. Conversión a Pandas solo de registros válidos para persistencia eficiente
            valid_pd = df_valid.select(
                "id", "mag", "place", "time", "updated", "sig",
                "longitude", "latitude", "depth", "ingested_at"
            ).toPandas()

            # Los registros inválidos simplemente se descartan de la memoria
            del df_invalid
            del df_evaluated

        finally:
            spark.stop()
            logger.info("SparkSession finalizada y recursos liberados.")

        # 7. Carga de datos a PostgreSQL mediante execute_batch
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 7.1. Carga a staging.earthquakes_clean y curated.earthquakes
                if not valid_pd.empty:
                    clean_records = [
                        (
                            row.id,
                            None if pd.isna(row.mag) else float(row.mag),
                            None if pd.isna(row.place) else str(row.place),
                            None if pd.isna(row.time) else row.time,
                            None if pd.isna(row.updated) else row.updated,
                            None if pd.isna(row.sig) else int(row.sig),
                            None if pd.isna(row.longitude) else float(row.longitude),
                            None if pd.isna(row.latitude) else float(row.latitude),
                            None if pd.isna(row.depth) else float(row.depth),
                            None if pd.isna(row.ingested_at) else row.ingested_at,
                        )
                        for row in valid_pd.itertuples(index=False)
                    ]

                    # Staging Clean
                    upsert_staging_sql = """
                    INSERT INTO staging.earthquakes_clean (
                        id, mag, place, time, updated, sig, longitude, latitude, depth, ingested_at, processed_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (id) DO UPDATE SET
                        mag = EXCLUDED.mag,
                        place = EXCLUDED.place,
                        time = EXCLUDED.time,
                        updated = EXCLUDED.updated,
                        sig = EXCLUDED.sig,
                        longitude = EXCLUDED.longitude,
                        latitude = EXCLUDED.latitude,
                        depth = EXCLUDED.depth,
                        ingested_at = EXCLUDED.ingested_at,
                        processed_at = CURRENT_TIMESTAMP;
                    """
                    execute_batch(cur, upsert_staging_sql, clean_records, page_size=100)
                    logger.info("Cargados %d registros en staging.earthquakes_clean.", len(clean_records))

                    # Curated Earthquakes (Capa Oro de Consumo)
                    curated_records = [
                        (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8])
                        for r in clean_records
                    ]
                    upsert_curated_sql = """
                    INSERT INTO curated.earthquakes (
                        id, mag, place, time, updated, sig, longitude, latitude, depth, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (id) DO UPDATE SET
                        mag = EXCLUDED.mag,
                        place = EXCLUDED.place,
                        time = EXCLUDED.time,
                        updated = EXCLUDED.updated,
                        sig = EXCLUDED.sig,
                        longitude = EXCLUDED.longitude,
                        latitude = EXCLUDED.latitude,
                        depth = EXCLUDED.depth,
                        updated_at = CURRENT_TIMESTAMP;
                    """
                    execute_batch(cur, upsert_curated_sql, curated_records, page_size=100)
                    logger.info("Cargados %d registros en curated.earthquakes.", len(curated_records))

                # 7.2. Registro de métricas de calidad en staging.quality_metrics (sin payloads de rechazados)
                metrics_sql = """
                INSERT INTO staging.quality_metrics (
                    dag_run_id, total_evaluated, total_passed, total_rejected, evaluated_at
                )
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP);
                """
                cur.execute(metrics_sql, (dag_run_id, total_evaluated, total_passed, total_rejected))
                logger.info("Métricas de calidad registradas en staging.quality_metrics.")

            conn.commit()

        return {
            "total_evaluated": total_evaluated,
            "total_passed": total_passed,
            "total_rejected": total_rejected,
        }

    # Instanciar tarea
    process_medallion_quality()


# Instanciar el DAG
usgs_transform_quality_dag = usgs_transform_quality()
