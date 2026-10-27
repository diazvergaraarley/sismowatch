FROM apache/airflow:2.8.1

USER root

# Instalar Java Runtime Environment (JRE) requerido por PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurar variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copiar requirements.txt e instalar dependencias Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
