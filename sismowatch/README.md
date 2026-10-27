# 🌍 SismoWatch - Plataforma de Monitoreo Sísmico y Analítica con IA

[![Apache Airflow](<https://img.shields.io/badge/Airflow-2.8.1-017CEE?style=flat-square&logo=Apache%20Airflow&logoColor=white>)](https://airflow.apache.org/)
[![Apache Spark](<https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=flat-square&logo=Apache%20Spark&logoColor=white>)](https://spark.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109.0-009688?style=flat-square&logo=FastAPI&logoColor=white)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30.0-FF4B4B?style=flat-square&logo=Streamlit&logoColor=white)](https://streamlit.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15--alpine-4169E1?style=flat-square&logo=PostgreSQL&logoColor=white)](https://www.postgresql.org/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=Docker&logoColor=white)](https://www.docker.com/)
[![OpenAI](https://img.shields.io/badge/OpenAI-gpt--4o--mini-412991?style=flat-square&logo=OpenAI&logoColor=white)](https://openai.com/)

---

## 📋 Tabla de Contenidos

1. [Descripción y Propósito del Proyecto](#-descripción-y-propósito-del-proyecto)
2. [Arquitectura General](#-arquitectura-general)
3. [Requisitos Previos del Sistema](#-requisitos-previos-del-sistema)
4. [Guía de Despliegue Paso a Paso (Atómica)](#-guía-de-despliegue-paso-a-paso-atómica)
5. [Accesos y Puertos de la Plataforma](#-accesos-y-puertos-de-la-plataforma)
6. [Credenciales y Control de Acceso (RBAC)](#-credenciales-y-control-de-acceso-rbac)
7. [Pipelines de Datos en Apache Airflow](#-pipelines-de-datos-en-apache-airflow)
8. [Implementación de Calidad de Datos (PySpark)](#-implementación-de-calidad-de-datos-pyspark)
9. [Seguridad y Aislamiento de Capas](#-seguridad-y-aislamiento-de-capas)
10. [Asistente de IA y Salvaguardas Anti-Alucinación](#-asistente-de-ia-y-salvaguardas-anti-alucinación)
11. [Estrategia de Tiempo Real](#-estrategia-de-tiempo-real)
12. [Ejecución de Pruebas Automatizadas](#-ejecución-de-pruebas-automatizadas)
13. [Estructura del Repositorio](#-estructura-del-repositorio)
14. [Limitaciones Conocidas y Roadmap a Producción](#-limitaciones-conocidas-y-roadmap-a-producción)

---

## 📖 Descripción y Propósito del Proyecto

**SismoWatch** es una plataforma analítica y de ingeniería de datos diseñada para la vertical de monitoreo y gestión de riesgo de **SuperApp**.

### El Problema

El Servicio Geológico de los Estados Unidos (USGS) provee un feed público de eventos sísmicos en formato GeoJSON (`all_hour.geojson`) que se actualiza continuamente. Sin embargo:

- No almacena historial consolidado ni trazable.
- No cuenta con validación de calidad ni tipado estricto.
- Es susceptible a reescrituras y ajustes continuos en magnitud y estado sin control de versiones.
- No dispone de una interfaz analítica amigable ni mecanismos de consulta en lenguaje natural con respaldo de datos auditables.

### La Solución

SismoWatch implementa una **Arquitectura Medallón (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** completa, orquestada mediante **Apache Airflow**, procesada y validada en memoria con **PySpark**, almacenada en **PostgreSQL**, expuesta mediante una API segura con **FastAPI** (JWT + RBAC) y consumida mediante una interfaz interactiva en **Streamlit** respaldada por un agente inteligente Text-to-SQL (**OpenAI gpt-4o-mini**) protegido contra inyecciones y alucinaciones.

---

## 🏛 Arquitectura General

```
                                  [ USGS Earthquake API ]
                                             │
                                             ▼
                             ┌───────────────────────────────┐
                             │  Apache Airflow (Scheduler)   │
                             │  DAG: usgs_ingestion_raw      │
                             └───────────────┬───────────────┘
                                             │
                                             ▼
                             ┌───────────────────────────────┐
                             │    PostgreSQL (Capa BRONZE)   │
                             │    raw.earthquake_events      │
                             └───────────────┬───────────────┘
                                             │
                                   TriggerDagRunOperator
                                             │
                                             ▼
                             ┌───────────────────────────────┐
                             │    Airflow + PySpark Local    │
                             │  DAG: usgs_transform_quality  │
                             └───────┬───────────────┬───────┘
                     (Datos Válidos) │               │ (Métricas)
                                     ▼               ▼
                      ┌─────────────────────┐ ┌───────────────────────────┐
                      │ PostgreSQL (SILVER) │ │    PostgreSQL (SILVER)    │
                      │ earthquakes_clean   │ │ staging.quality_metrics   │
                      └──────────────┬──────┘ └───────────────────────────┘
                                     │
                                     ▼
                      ┌─────────────────────────────────┐
                      │    PostgreSQL (Capa GOLD)       │
                      │    curated.earthquakes          │
                      └──────────────┬──────────────────┘
                                     │
                        (Solo esquema curated)
                                     │
                                     ▼
                      ┌─────────────────────────────────┐
                      │        FastAPI Microservice     │
                      │  - JWT Bearer Authentication    │
                      │  - RBAC (Admin / Viewer)        │
                      │  - Text-to-SQL Guardrails       │
                      │  - OpenAI gpt-4o-mini           │
                      └──────────────┬──────────────────┘
                                     │
                                     ▼
                      ┌─────────────────────────────────┐
                      │       Streamlit Dashboard       │
                      │  - Métricas y KPIs en vivo      │
                      │  - Mapa geoespacial interactivo │
                      │  - Línea temporal Plotly        │
                      │  - Chat IA con SQL transparente │
                      └─────────────────────────────────┘
```

> [!NOTE]
> Para un análisis profundo de los trade-offs, diagramas de flujo de datos y decisiones técnicas detalladas, consulta el documento [docs/ARQUITECTURA.md](file:///home/cohorte5/Escritorio/Arley/prueba_de_desarrollo_ruta_avanzada/sismowatch/docs/ARQUITECTURA.md).

---

## 💻 Requisitos Previos del Sistema

Antes de iniciar, asegúrate de contar con las siguientes herramientas instaladas en tu equipo anfitrión:

- **Docker Engine:** Versión 24.0.0 o superior.
- **Docker Compose:** Versión v2.20.0 o superior (`docker compose version`).
- **Git:** Para clonar el repositorio.
- **Conexión a Internet:** Para descargar imágenes de Docker, librerías y conectarse tanto a la API pública de USGS como a la API de OpenAI.
- **Clave API de OpenAI:** Una clave activa (`sk-...`) con acceso al modelo `gpt-4o-mini`.
- **Puertos Libres en el Host:**
  - `3000` (Interfaz Web Streamlit)
  - `8000` (Microservicio FastAPI)
  - `8080` (Consola Web de Airflow)
  - `5434` (Base de Datos PostgreSQL en el Host)

---

## 🚀 Guía de Despliegue Paso a Paso (Atómica)

Sigue cada uno de estos pasos en tu terminal para levantar la plataforma desde cero:

### Paso 1: Clonar el Repositorio y Ubicarse en el Proyecto

```bash
git clone https://github.com/diazvergaraarley/sismowatch
cd sismowatch
```

### Paso 2: Crear el Archivo de Configuración de Entorno

Copia la plantilla de variables de entorno `.env.example` para generar tu archivo `.env`:

```bash
cp .env.example .env
```

### Paso 3: Configurar tu API Key de OpenAI

Abre el archivo `.env`  y asigna tu token de OpenAI a la variable `OPENAI_API_KEY`:

```env
OPENAI_API_KEY=sk-proj-tuClaveRealDeOpenAiAqui...
```

> [!IMPORTANT]
> El archivo `.env` ya viene precargado con valores por defecto óptimos para el resto de los servicios (PostgreSQL en puerto host `5434`, usuarios de Airflow y credenciales de la aplicación). No modifiques las demás variables a menos que tengas requisitos específicos de infraestructura.
>
> *Si eres un **evaluador** leyendo esto, seguro tienes el archivo zip, por lo que puedes utilizar **mi token de open ai** alojado en el archivo /prueba_de_desarrollo_ruta_avanzada/open_ai_studio.txt

### Paso 4: Construir y Levantar los Contenedores

Ejecuta el siguiente comando para compilar las imágenes personalizadas (Airflow con Java/PySpark, API con FastAPI y Web con Streamlit) y levantar todos los servicios en segundo plano:

```bash
docker compose up -d --build
```

### Paso 5: Verificar el Estado de los Servicios

Comprueba que los 5 contenedores principales se encuentren en estado saludable (`healthy`) o en ejecución (`Up`):

```bash
docker compose ps
```

Deberás observar los siguientes contenedores activos:

- `sismowatch_postgres` (PostgreSQL 15, puerto 5434)
- `sismowatch_airflow_init` (Completado con éxito tras migrar la BD y crear el usuario)
- `sismowatch_airflow_scheduler` (Orquestador continuo)
- `sismowatch_airflow_webserver` (Interfaz de Airflow, puerto 8080)
- `sismowatch_api` (FastAPI, puerto 8000)
- `sismowatch_web` (Streamlit, puerto 3000)

---

## 🌐 Accesos y Puertos de la Plataforma

Una vez iniciados los contenedores, los servicios quedan disponibles en las siguientes URLs:

| Servicio                 | URL Local                                                 | Puerto Host | Descripción                                                           |
| :----------------------- | :-------------------------------------------------------- | :---------- | :--------------------------------------------------------------------- |
| **Streamlit Web**  | [http://localhost:3000](http://localhost:3000)             | `3000`    | Interfaz gráfica de usuario: Dashboard en tiempo real y Asistente IA. |
| **FastAPI Docs**   | [http://localhost:8000/docs](http://localhost:8000/docs)   | `8000`    | Documentación interactiva Swagger UI con todos los endpoints.         |
| **FastAPI ReDoc**  | [http://localhost:8000/redoc](http://localhost:8000/redoc) | `8000`    | Documentación técnica alternativa OpenAPI.                           |
| **Apache Airflow** | [http://localhost:8080](http://localhost:8080)             | `8080`    | Panel de control de DAGs, logs de ejecución y monitoreo de tareas.    |
| **PostgreSQL**     | `localhost:5434`                                        | `5434`    | Conexión directa a la BD (cliente DBeaver, `psql`, etc.).           |

---

## 🔐 Credenciales y Control de Acceso (RBAC)

El sistema implementa autenticación basada en **JWT (JSON Web Tokens)** con roles segregados:

| Rol / Módulo          | Usuario        | Contraseña        | Permisos y Alcance                                                                                               |
| :--------------------- | :------------- | :----------------- | :--------------------------------------------------------------------------------------------------------------- |
| **Admin (App)**  | `admin`      | `supersecreto`   | Acceso completo al Dashboard, visualización de métricas de calidad y consultas ilimitadas con el Asistente IA. |
| **Viewer (App)** | `viewer`     | `sololeer`       | Acceso de solo lectura al Dashboard y al Asistente IA. Restringido de operaciones administrativas.               |
| **Airflow UI**   | `admin`      | `admin`          | Administración y ejecución manual de DAGs, visualización de logs y variables.                                 |
| **PostgreSQL**   | `sismo_user` | `sismo_password` | Usuario de base de datos propietario de los esquemas `raw`, `staging` y `curated` (BD: `sismowatch`).    |

> [!NOTE]
> Sin iniciar sesión en la barra lateral de Streamlit, la plataforma restringe por completo la visualización de los datos y muestra una advertencia de autenticación requerida.

---

## 🔄 Pipelines de Datos en Apache Airflow

El pipeline está desacoplado en dos DAGs secuenciales e independientes para garantizar aislamiento de fallos:

```
[ usgs_ingestion_raw ] ────(TriggerDagRunOperator)────▶ [ usgs_transform_quality ]
```

### 1. DAG `usgs_ingestion_raw`

- **Frecuencia:** Cada 5 minutos (`schedule_interval="*/5 * * * *"`), `catchup=False`.
- **Tarea `setup_table`:** Asegura la existencia de la tabla `raw.earthquake_events` con llaves primarias y tipos estructurados.
- **Tarea `fetch_and_store`:**
  - Extrae el GeoJSON desde `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` con un timeout controlado de 10 segundos.
  - Itera sobre la colección de `features`.
  - Realiza un **Upsert Idempotente** en PostgreSQL:
    ```sql
    INSERT INTO raw.earthquake_events (id, payload, ingested_at)
    VALUES (%s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (id) DO UPDATE SET 
        payload = EXCLUDED.payload, 
        ingested_at = CURRENT_TIMESTAMP;
    ```
- **Tarea `trigger_transform`:** Utiliza `TriggerDagRunOperator` para desencadenar automáticamente la fase de calidad y transformación inmediatamente después de terminar la ingesta.

### 2. DAG `usgs_transform_quality`

- **Disparo:** Automatizado vía trigger tras completar la ingesta cruda.
- **Tarea `spark_transform_and_quality`:**
  - Inicializa una sesión optimizada de **PySpark local** (`local[*]`).
  - Extrae los payloads JSONB de `raw.earthquake_events`.
  - Aplica un esquema riguroso con `StructType` para evitar errores de inferencia.
  - Ejecuta las reglas de calidad de datos en memoria.
  - Registra las métricas de ejecución en `staging.quality_metrics`.
  - Persiste los datos normalizados en `staging.earthquakes_clean` y `curated.earthquakes` mediante upserts libres de duplicados.

---

## 🛡 Implementación de Calidad de Datos (PySpark)

Para el motor de calidad se seleccionó **PySpark en modo local**, integrado en la imagen personalizada de Airflow con soporte para Java Runtime (`default-jre-headless`).

### Reglas de Calidad Aplicadas

1. **Completitud e Integridad Estructural:** El identificador `id` y la marca temporal `time` no deben ser nulos (`col("id").isNotNull() & col("time").isNotNull()`).
2. **Coordenadas de Latitud Válidas:** La latitud geográfica debe ubicarse en el intervalo $[-90.0, 90.0]$.
3. **Coordenadas de Longitud Válidas:** La longitud geográfica debe ubicarse en el intervalo $[-180.0, 180.0]$.

### Política de Manejo de Anomalías

- **Descarte en Memoria:** Los registros que no satisfacen la totalidad de las reglas de calidad son filtrados en los DataFrames de Spark antes de la persistencia. No se almacenan datos corruptos en la capa de consumo.
- **Observabilidad y Auditoría:** Cada corrida del pipeline inserta un registro en la tabla `staging.quality_metrics` con:
  - `dag_run_id`: Identificador de la ejecución de Airflow.
  - `evaluated_records`: Número total de registros analizados.
  - `approved_records`: Número de registros que pasaron todas las reglas.
  - `rejected_records`: Cantidad de registros descartados por anomalías.
  - `executed_at`: Timestamp de la auditoría.

---

## 🔒 Seguridad y Aislamiento de Capas

El diseño de la plataforma sigue el principio de **Defensa en Profundidad**:

1. **Gestión de Secretos:** Ninguna credencial ni token reside en el código fuente. Toda configuración se inyecta por variables de entorno (.env), excluido de Git vía `.gitignore`.
2. **Autenticación Stateless JWT:** El microservicio FastAPI firma y verifica tokens con algoritmo `HS256` y expiración controlada. Todos los endpoints analíticos (`/api/kpis`, `/api/events`, `/api/chat`) exigen el encabezado HTTP `Authorization: Bearer <token>`.
3. **Aislamiento de Base de Datos:** El pool de conexiones de SQLAlchemy de la API de consumo se configura con `options="-c search_path=curated"`. La aplicación carece de visibilidad sobre los esquemas `raw` y `staging`, impidiendo fugas de datos crudos o métricas internas.

---

## 🤖 Asistente de IA y Salvaguardas Anti-Alucinación

El endpoint `POST /api/chat` implementa un flujo inteligente en dos fases con **OpenAI (`gpt-4o-mini`)**:

```
[ Pregunta Usuario ] ──▶ [ Prompt de Sistema + Esquema Curated ] ──▶ [ SQL Generado por LLM ]
                                                                             │
                                                                             ▼
                                                                [ Filtro Regex de Seguridad ]
                                                                             │
                                                       ┌─────────────────────┴─────────────────────┐
                                                       │ (SQL Válido)                              │ (Comando No Permitido)
                                                       ▼                                           ▼
                                             [ Ejecución SELECT ]                         [ HTTP 403 Forbidden ]
                                             [ en PostgreSQL    ]
                                                       │
                                                       ▼
                                             [ Resultados Reales ]
                                                       │
                                                       ▼
                                             [ LLM Grounding Prompt ]
                                                       │
                                                       ▼
                                             [ Respuesta Final Confiable ]
```

### Medidas Anti-Alucinación y de Seguridad:

1. **Validación de Consultas:** La consulta generada por el LLM se valida mediante expresiones regulares:
   - Debe iniciar estrictamente con la cláusula `SELECT`.
   - Se prohíben comandos de manipulación o definición de datos: `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `EXEC`.
   - La consulta queda acotada únicamente a la tabla `curated.earthquakes`.
2. **Grounding Numérico Estricto:** El modelo de lenguaje final no calcula métricas por sí mismo; recibe las filas numéricas exactas resultantes de la ejecución en PostgreSQL con la instrucción de basarse única y exclusivamente en ellas.
3. **Transparencia Técnica:** La interfaz de Streamlit expone un componente colapsable (`st.expander("🔍 Ver detalles técnicos de la consulta (SQL)")`) donde el analista puede auditar el SQL generado y los registros recuperados.

---

## ⏱ Estrategia de Tiempo Real

### Justificación Técnica: Polling Coordinado vs WebSockets

Para la naturaleza de los datos sísmicos del USGS:

- El feed oficial `all_hour.geojson` actualiza sus archivos aproximadamente cada 60 a 300 segundos.
- Un protocolo de WebSockets o Server-Sent Events (SSE) hacia la fuente externa no está disponible de forma nativa por el USGS sin intermediarios.
- **Decisión:** Se implementó una arquitectura de **Polling Coordinado a 5 minutos** orquestado por Airflow. Esto optimiza el consumo de red, respeta los límites de la API del USGS y garantiza consistencia mediante transacciones atómicas en PostgreSQL.
- En la interfaz de Streamlit, el usuario dispone de un botón de actualización manual y de mecanismos de caché reactivos (`st.cache_data` con TTL) para visualizar los nuevos eventos instantáneamente sin sobrecargar la API.

---

## 🧪 Ejecución de Pruebas Automatizadas

El proyecto incluye una suite de pruebas con **pytest** y **HTTPX** (`tests/test_api.py`) que valida:

- Rechazo HTTP 401 en `/api/kpis` sin token.
- Rechazo HTTP 401 en `/api/events` sin token.
- Rechazo HTTP 401 en `/api/chat` sin token.
- Rechazo HTTP 401 ante credenciales inválidas en `/auth/login`.
- Emisión de token JWT válido y consulta autorizada exitosa (HTTP 200).

Para ejecutar los tests en cualquier máquina con los contenedores arriba:

```bash
docker compose exec api pytest -v /app/tests/test_api.py
```

### Salida Esperada:

```text
tests/test_api.py::test_get_kpis_unauthorized PASSED                     [ 20%]
tests/test_api.py::test_get_events_unauthorized PASSED                   [ 40%]
tests/test_api.py::test_chat_unauthorized PASSED                         [ 60%]
tests/test_api.py::test_login_invalid_credentials PASSED                 [ 80%]
tests/test_api.py::test_login_and_authorized_access PASSED               [100%]

======================== 5 passed in 0.68s =========================
```

---

## 📁 Estructura del Repositorio

```plaintext
sismowatch/
├── docker-compose.yml           # Orquestación de contenedores (Postgres, Airflow, API, Web)
├── .env.example                 # Plantilla de variables de entorno y secretos
├── .env                         # Variables de entorno activas (ignorado en git)
├── README.md                    # Documentación principal del proyecto
├── setup_structure.sh           # Script auxiliar de inicialización
├── docs/
│   └── ARQUITECTURA.md          # Documento de arquitectura, decisiones y trade-offs
├── db/
│   └── init/
│       └── 01_init.sql          # Creación de esquemas (raw, staging, curated) y extensiones
├── airflow/
│   ├── Dockerfile               # Imagen Airflow extendida con Java Runtime para PySpark
│   ├── requirements.txt         # Dependencias Python para Airflow
│   ├── dags/
│   │   ├── ingestion_raw_dag.py     # DAG de ingesta continua desde la API USGS
│   │   └── transform_quality_dag.py # DAG de transformación PySpark y reglas de calidad
│   └── include/                 # Recursos auxiliares y configuraciones
├── api/
│   ├── Dockerfile               # Imagen Docker para FastAPI
│   ├── requirements.txt         # Dependencias del backend (FastAPI, SQLAlchemy, PyJWT, OpenAI)
│   └── main.py                  # Endpoints REST, autenticación JWT y Agente Text-to-SQL
├── web/
│   ├── Dockerfile               # Imagen Docker para Streamlit
│   ├── requirements.txt         # Dependencias del frontend (Streamlit, Plotly, Pandas)
│   └── app.py                   # Dashboard visual, mapa, gráficos y chat analítico
└── tests/
    └── test_api.py              # Suite de pruebas automatizadas con pytest
```

---

## 🚧 Limitaciones Conocidas y Roadmap a Producción

Si bien la arquitectura actual cumple rigurosamente con los requisitos funcionales y de seguridad para un entorno analítico, para una escala empresarial masiva se contemplan las siguientes evoluciones:

1. **Clúster Distribuido de Spark:** Desacoplar PySpark de los contenedores de Airflow hacia un clúster dedicado gestionado (por ejemplo, AWS EMR, Google Cloud Dataproc o Databricks) para procesar volúmenes masivos de datos históricos.
2. **Ingesta Orientada a Eventos (Streaming):** Incorporar **Apache Kafka** o **AWS Kinesis** junto a **Spark Structured Streaming** para lograr una latencia inferior a los 5 segundos por sismo detectado.
3. **Base de Datos Gestionada:** Migrar PostgreSQL local hacia una instancia en alta disponibilidad (Amazon RDS / Cloud SQL) con réplicas de solo lectura para la capa `curated`.
4. **Airflow KubernetesExecutor:** Reemplazar el `LocalExecutor` por `KubernetesExecutor` o migrar a soluciones como AWS MWAA / Google Cloud Composer para auto-escalado horizontal de tareas.
5. **Auditoría Avanzada de LLM:** Incorporar un framework de observabilidad como **LangSmith** o **Phoenix (Arize)** para monitorear costos, latencia de tokens y evaluación continua del modelo.
