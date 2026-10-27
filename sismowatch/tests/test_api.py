"""
Pruebas Automatizadas de Seguridad y Control de Acceso - SismoWatch
Sprint: 6 - Testing de Autenticación JWT y Endpoints Protegidos
"""
import os
import sys
import pytest
from fastapi.testclient import TestClient

# Añadir la carpeta api al sys.path para importar la aplicación
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "api")))
sys.path.insert(0, "/app")  # Cuando corre dentro del contenedor Docker

from main import app, ADMIN_USER, ADMIN_PASSWORD, VIEWER_USER, VIEWER_PASSWORD

client = TestClient(app)


def test_get_kpis_unauthorized():
    """
    Requisito estricto del Sprint 6:
    Verifica que una petición GET a /api/kpis sin token de autorización
    sea rechazada obligatoriamente con código HTTP 401 (Unauthorized) o 403 (Forbidden).
    """
    response = client.get("/api/kpis")
    assert response.status_code in [401, 403], (
        f"Se esperaba HTTP 401 o 403 al acceder sin token, pero se recibió {response.status_code}"
    )


def test_get_events_unauthorized():
    """Verifica que el endpoint /api/events no sea accesible sin token JWT."""
    response = client.get("/api/events")
    assert response.status_code in [401, 403]


def test_chat_unauthorized():
    """Verifica que el endpoint /api/chat no permita consultas sin token JWT."""
    response = client.post("/api/chat", json={"question": "¿Cuántos sismos hubo?"})
    assert response.status_code in [401, 403]


def test_login_invalid_credentials():
    """Verifica que credenciales erróneas devuelvan HTTP 401."""
    response = client.post(
        "/auth/login",
        json={"username": "usuario_falso", "password": "password_incorrecto"},
    )
    assert response.status_code == 401


def test_login_and_authorized_access():
    """Verifica login exitoso y acceso autorizado con token JWT a /api/kpis."""
    # 1. Login con credenciales de admin
    login_response = client.post(
        "/auth/login",
        json={"username": ADMIN_USER, "password": ADMIN_PASSWORD},
    )
    assert login_response.status_code == 200
    token_data = login_response.json()
    assert "access_token" in token_data
    token = token_data["access_token"]
    assert token_data["role"] == "admin"

    # 2. Acceso con Bearer Token
    headers = {"Authorization": f"Bearer {token}"}
    kpis_response = client.get("/api/kpis", headers=headers)
    assert kpis_response.status_code == 200
    kpis_data = kpis_response.json()
    assert "total_events" in kpis_data
    assert "max_magnitude" in kpis_data
    assert "avg_magnitude" in kpis_data
