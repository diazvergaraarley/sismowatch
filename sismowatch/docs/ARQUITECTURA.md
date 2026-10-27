# Arquitectura SismoWatch

## Visión General
SismoWatch es una plataforma de monitoreo sísmico en tiempo real construida con una Arquitectura Medallón en PostgreSQL y orquestada con Apache Airflow.

## Capas de Datos (Medallón)
1. **Raw (Bronce):** Ingesta cruda de eventos sísmicos desde la API de USGS.
2. **Staging (Plata):** Validación de calidad, limpieza, deduplicación y normalización de coordenadas y magnitudes.
3. **Curated (Oro):** Modelado dimensional, agregaciones analíticas y métricas para consumo de dashboards y reportes.

## Servicios
- **PostgreSQL 15:** Base de datos relacional para metadata de Airflow y almacenamiento Medallón.
- **Apache Airflow 2.8.1:** Orquestador de pipelines (Webserver y Scheduler con LocalExecutor).
- **API (FastAPI):** Microservicio para consumo de datos, streaming y endpoints administrativos.
- **Web (Streamlit):** Interfaz gráfica para operadores y analistas de gestión de riesgo.

