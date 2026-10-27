# SismoWatch - Plataforma de Monitoreo Sísmico

Plataforma de ingesta, orquestación, procesamiento y visualización de eventos sísmicos globales.

## Despliegue Rápido

1. Configurar variables de entorno:
   ```bash
   cp .env.example .env
   ```

2. Levantar los servicios:
   ```bash
   docker compose up -d --build
   ```

## Servicios y Puertos
- **Airflow Web UI:** [http://localhost:8080](http://localhost:8080) (Credenciales: `admin` / `admin`)
- **API (FastAPI Docs):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Web UI (Streamlit):** [http://localhost:3000](http://localhost:3000)
- **PostgreSQL:** `localhost:5432`

