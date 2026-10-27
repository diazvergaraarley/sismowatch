#!/usr/bin/env bash
set -e

# Crear árbol de directorios
mkdir -p {docs,airflow/{dags,include},db/init,api,web}

# Crear archivos de configuración y documentación raíz
touch docker-compose.yml .env.example README.md docs/ARQUITECTURA.md

# Crear archivos de Airflow
touch airflow/requirements.txt

# Crear script de inicialización de Base de Datos
touch db/init/01_init.sql

# Crear archivos para el microservicio API
touch api/{main.py,requirements.txt,Dockerfile}

# Crear archivos para el microservicio Web
touch web/{app.py,requirements.txt,Dockerfile}

echo "Estructura de sismowatch creada exitosamente."

