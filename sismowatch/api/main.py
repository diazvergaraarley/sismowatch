"""
Microservicio API y Agente LLM - SismoWatch
Plataforma de Monitoreo Sísmico
Sprint: 6 - Autenticación JWT, Control de Acceso y Testing
"""
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import re
from typing import Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt
from openai import OpenAI
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sismowatch-api")

# Inicialización de FastAPI
app = FastAPI(
    title="SismoWatch API Segura",
    description="API analítica protegida con JWT y agente conversacional de SismoWatch",
    version="1.1.0",
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# Variables de Seguridad y Autenticación JWT
# ==========================================
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "supersecreto")
VIEWER_USER = os.getenv("VIEWER_USER", "viewer")
VIEWER_PASSWORD = os.getenv("VIEWER_PASSWORD", "sololeer")
JWT_SECRET = os.getenv("JWT_SECRET", "sismowatch_firma_secreta_2026")
JWT_ALGORITHM = "HS256"

# Esquema de seguridad Bearer
security_bearer = HTTPBearer(auto_error=False)

# ==========================================
# Conexión a Base de Datos (PostgreSQL)
# ==========================================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://sismo_user:sismo_password@postgres:5432/sismowatch",
)
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"options": "-csearch_path=curated"},
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ==========================================
# Modelos Pydantic
# ==========================================
class LoginRequest(BaseModel):
    username: str = Field(..., description="Nombre de usuario del sistema")
    password: str = Field(..., description="Contraseña de acceso")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=2, description="Pregunta del usuario en lenguaje natural")


class ChatResponse(BaseModel):
    question: str
    sql_query: str
    data: List[Dict[str, Any]]
    answer: str


# ==========================================
# Dependencia de Autenticación y Validación JWT
# ==========================================
def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security_bearer),
) -> Dict[str, Any]:
    """
    Extrae y valida el token JWT del header 'Authorization: Bearer <token>'.
    Lanza HTTP 401 si no se envía el token, si ha expirado o si la firma es inválida.
    """
    if not credentials or not credentials.credentials:
        logger.warning("Intento de acceso no autorizado sin token.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Acceso denegado: Token de autenticación no proporcionado.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError as exc:
        logger.warning("Token expirado presentado: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Acceso denegado: El token de sesión ha expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except jwt.PyJWTError as exc:
        logger.warning("Token inválido presentado: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Acceso denegado: Token JWT inválido ({str(exc)}).",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


# ==========================================
# Endpoint de Autenticación (Login)
# ==========================================
@app.post("/auth/login", response_model=TokenResponse, tags=["Autenticación"])
def login(credentials: LoginRequest):
    """
    Valida credenciales contra las variables de entorno:
    - ADMIN_USER / ADMIN_PASSWORD -> Rol 'admin'
    - VIEWER_USER / VIEWER_PASSWORD -> Rol 'viewer'
    Genera un token JWT firmado válido por 24 horas.
    """
    role = None
    if credentials.username == ADMIN_USER and credentials.password == ADMIN_PASSWORD:
        role = "admin"
    elif credentials.username == VIEWER_USER and credentials.password == VIEWER_PASSWORD:
        role = "viewer"
    else:
        logger.warning("Fallo de autenticación para usuario: %s", credentials.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas: Usuario o contraseña no válidos.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Generar Token JWT con rol y expiración de 24 horas
    now = datetime.now(timezone.utc)
    payload = {
        "sub": credentials.username,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=24)).timestamp()),
    }

    access_token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    logger.info("Login exitoso para usuario: %s con rol: %s", credentials.username, role)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        role=role,
        username=credentials.username,
    )


# ==========================================
# Endpoint de Salud (Público para Healthchecks)
# ==========================================
@app.get("/health", tags=["Salud"])
def health_check():
    """Endpoint público de comprobación de salud del contenedor."""
    return {"status": "ok", "service": "SismoWatch API Segura"}


# ==========================================
# Endpoints de Dashboard (Protegidos con JWT)
# ==========================================
@app.get("/api/kpis", tags=["Dashboard"])
def get_kpis(current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Devuelve los indicadores clave (KPIs) de las últimas 24 horas.
    Requiere autenticación válida con JWT.
    """
    query = text("""
        SELECT 
            COUNT(*) AS total_events,
            COALESCE(MAX(mag), 0.0) AS max_magnitude,
            COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0) AS avg_magnitude
        FROM curated.earthquakes
        WHERE time >= NOW() - INTERVAL '24 hours';
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query).mappings().first()
            return {
                "total_events": int(result["total_events"]) if result else 0,
                "max_magnitude": float(result["max_magnitude"]) if result else 0.0,
                "avg_magnitude": float(result["avg_magnitude"]) if result else 0.0,
                "window": "last_24_hours",
                "requested_by": current_user.get("sub"),
            }
    except Exception as exc:
        logger.error("Error al consultar KPIs: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al calcular KPIs: {str(exc)}",
        ) from exc


@app.get("/api/events", tags=["Dashboard"])
def get_events(limit: int = 50, current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Devuelve los últimos eventos sísmicos registrados ordenados por fecha descendente.
    Requiere autenticación válida con JWT.
    """
    query = text("""
        SELECT 
            id, mag, place, time, updated, sig, longitude, latitude, depth, updated_at
        FROM curated.earthquakes
        ORDER BY time DESC
        LIMIT :limit;
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"limit": limit}).mappings().all()
            events = []
            for r in rows:
                events.append({
                    "id": r["id"],
                    "mag": r["mag"],
                    "place": r["place"],
                    "time": r["time"].isoformat() if r["time"] else None,
                    "updated": r["updated"].isoformat() if r["updated"] else None,
                    "sig": r["sig"],
                    "longitude": r["longitude"],
                    "latitude": r["latitude"],
                    "depth": r["depth"],
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                })
            return {
                "count": len(events),
                "events": events,
                "requested_by": current_user.get("sub"),
            }
    except Exception as exc:
        logger.error("Error al consultar eventos: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al obtener eventos sísmicos: {str(exc)}",
        ) from exc


# ==========================================
# Agente LLM para Consultas (Protegido con JWT)
# ==========================================
def validate_sql_security(raw_sql: str) -> str:
    """Valida y sanitiza la consulta SQL generada por el LLM."""
    cleaned = raw_sql.strip()

    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned).strip()

    if not cleaned.upper().startswith("SELECT"):
        logger.warning("Intento de consulta no-SELECT rechazado: %s", cleaned)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operación denegada: La consulta debe comenzar obligatoriamente con SELECT.",
        )

    cleaned_lower = cleaned.lower()
    if "curated.earthquakes" not in cleaned_lower:
        logger.warning("Consulta no apunta a curated.earthquakes: %s", cleaned)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operación denegada: La consulta debe hacer referencia explícita a la tabla 'curated.earthquakes'.",
        )

    forbidden_keywords = [
        r"\bDROP\b", r"\bUPDATE\b", r"\bINSERT\b", r"\bDELETE\b",
        r"\bALTER\b", r"\bTRUNCATE\b", r"\bCREATE\b", r"\bGRANT\b",
        r"\bREVOKE\b", r"\bEXEC\b", r"\bEXECUTE\b"
    ]
    for pattern in forbidden_keywords:
        if re.search(pattern, cleaned, re.IGNORECASE):
            logger.warning("Palabra clave prohibida detectada (%s): %s", pattern, cleaned)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operación denegada: Comandos de modificación de datos o estructura no están permitidos.",
            )

    forbidden_schemas = [r"\braw\.", r"\bstaging\.", r"\bpublic\.", r"\bpg_catalog\.", r"\binformation_schema\."]
    for pattern in forbidden_schemas:
        if re.search(pattern, cleaned_lower):
            logger.warning("Intento de acceso a esquema restringido (%s): %s", pattern, cleaned)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operación denegada: No tiene autorización para consultar esquemas distintos a 'curated'.",
            )

    if ";" in cleaned.rstrip(";"):
        logger.warning("Múltiples declaraciones detectadas: %s", cleaned)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operación denegada: No se permiten sentencias SQL compuestas o múltiples.",
        )

    return cleaned


@app.post("/api/chat", response_model=ChatResponse, tags=["Agente IA"])
def chat_with_data(payload: ChatRequest, current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Agente LLM para consultas en lenguaje natural protegido con JWT:
    - Paso 1: Generación de SQL seguro a partir de la pregunta del usuario.
    - Paso 2: Validación estricta y ejecución en PostgreSQL.
    - Paso 3: Redacción final de respuesta amable basada exclusivamente en los datos reales.
    """
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key or openai_api_key == "your_openai_api_key_here":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="La clave de API de OpenAI (OPENAI_API_KEY) no está configurada en las variables de entorno del microservicio.",
        )

    client = OpenAI(api_key=openai_api_key)
    user_question = payload.question.strip()

    # Paso 1: Generación SQL
    sql_system_prompt = """Eres el asistente de datos oficial de SismoWatch, una plataforma de monitoreo sísmico global.
Tu única misión es convertir la pregunta del usuario en una consulta SQL válida para PostgreSQL.

Esquema exacto de la tabla de consumo analítico:
curated.earthquakes (
    id VARCHAR PRIMARY KEY,          -- Identificador único del evento
    mag DOUBLE PRECISION,            -- Magnitud del sismo
    place VARCHAR,                   -- Ubicación geográfica descriptiva
    time TIMESTAMP,                  -- Fecha y hora del evento sísmico
    updated TIMESTAMP,               -- Fecha de última actualización del reporte
    sig INTEGER,                     -- Nivel de significancia del evento
    longitude DOUBLE PRECISION,      -- Longitud (-180 a 180)
    latitude DOUBLE PRECISION,       -- Latitud (-90 a 90)
    depth DOUBLE PRECISION           -- Profundidad del hipocentro en km
)

Instrucciones imperativas:
1. Devuelve ÚNICAMENTE la consulta SQL en texto plano, sin markdown, sin comillas invertidas (```), sin explicaciones.
2. La consulta debe comenzar obligatoriamente con SELECT y referenciar siempre la tabla curated.earthquakes.
3. Para consultas temporales usa funciones estándar de PostgreSQL como NOW(), INTERVAL, etc.
4. Para consultas sobre sismos más fuertes, usa ORDER BY mag DESC LIMIT N.
5. Para consultas sobre sismos recientes, usa ORDER BY time DESC LIMIT N."""

    try:
        sql_completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": sql_system_prompt},
                {"role": "user", "content": user_question},
            ],
            temperature=0.0,
        )
        generated_sql = sql_completion.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("Fallo al invocar OpenAI para generación SQL: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Fallo de comunicación con OpenAI al generar la consulta: {str(exc)}",
        ) from exc

    logger.info("SQL Generado por LLM (Usuario: %s): %s", current_user.get("sub"), generated_sql)

    # Paso 2: Validación y Ejecución
    safe_sql = validate_sql_security(generated_sql)

    try:
        with engine.connect() as conn:
            db_result = conn.execute(text(safe_sql))
            columns = list(db_result.keys())
            rows = db_result.fetchall()

            serialized_rows = []
            for row in rows:
                record = {}
                for col_name, val in zip(columns, row):
                    if hasattr(val, "isoformat"):
                        record[col_name] = val.isoformat()
                    else:
                        record[col_name] = val
                serialized_rows.append(record)
    except Exception as exc:
        logger.error("Error al ejecutar SQL validado en BD: %s | Query: %s", exc, safe_sql)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error en la ejecución de la consulta generada: {str(exc)}",
        ) from exc

    # Paso 3: Redacción Segura
    formatted_data_str = json.dumps(serialized_rows, ensure_ascii=False, indent=2)

    redaction_prompt = (
        f"Aquí están los datos calculados por el sistema:\n{formatted_data_str}\n\n"
        f"Redacta una respuesta amable para el usuario respondiendo a su pregunta original ('{user_question}'), "
        f"usando estrictamente estos números. No inventes datos ni hagas cálculos matemáticos por tu cuenta."
    )

    try:
        redaction_completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un asistente amable y riguroso de SismoWatch que responde basándose únicamente en los datos provistos."},
                {"role": "user", "content": redaction_prompt},
            ],
            temperature=0.3,
        )
        final_answer = redaction_completion.choices[0].message.content.strip()
    except Exception as exc:
        logger.error("Fallo al invocar OpenAI para redacción: %s", exc)
        final_answer = f"Se obtuvieron {len(serialized_rows)} resultado(s) para tu consulta. Datos: {formatted_data_str}"

    return ChatResponse(
        question=user_question,
        sql_query=safe_sql,
        data=serialized_rows,
        answer=final_answer,
    )
