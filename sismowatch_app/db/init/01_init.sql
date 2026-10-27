-- ====================================================================
-- Inicialización de Esquemas - Arquitectura Medallón (SismoWatch)
-- ====================================================================

-- Capa Bronce (Raw): Ingesta directa de datos crudos sin procesar
CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS 'Capa Bronce: Almacena payloads y eventos sísmicos crudos sin transformaciones.';

-- Capa Plata (Staging): Datos limpios, normalizados, validados y desduplicados
CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS 'Capa Plata: Datos normalizados, con tipos casteados y coordenadas validadas.';

-- Capa Oro (Curated): Modelos dimensionales, agregaciones y métricas para analítica y visualización
CREATE SCHEMA IF NOT EXISTS curated;
COMMENT ON SCHEMA curated IS 'Capa Oro: Tablas agregadas y optimizadas para consumo de API y Dashboards.';

