"""Tests de seguridad: ningun secreto escrito en el codigo versionado."""

import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]

PATRONES_SECRETOS = [
    re.compile(r"(?i)(password|passwd|secret|api_key|token)\s*[:=]\s*['\"][^'\"{}$\s]{6,}['\"]"),
    re.compile(r"AKIA[0-9A-Z]{16}"),  # access key de AWS
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]


def _archivos_versionables():
    for patron in ("dags/**/*.py", "dags/**/*.yml", "dags/**/*.sql", "docker-compose.yaml", "infra/**/*"):
        for ruta in RAIZ.glob(patron):
            if ruta.is_file() and "target" not in ruta.parts:
                yield ruta


def test_no_hay_credenciales_escritas_en_el_codigo():
    hallazgos = [
        f"{ruta.relative_to(RAIZ)}: {m.group(0)[:40]}"
        for ruta in _archivos_versionables()
        for patron in PATRONES_SECRETOS
        for m in patron.finditer(ruta.read_text(encoding="utf-8", errors="ignore"))
    ]
    assert hallazgos == []


def test_profiles_yml_lee_credenciales_de_variables_de_entorno():
    perfil = (RAIZ / "dags/dbt/salud_aire/profiles.yml").read_text(encoding="utf-8")
    for campo in ("account", "user", "password"):
        assert re.search(rf"{campo}:\s*\"\{{\{{ env_var\('SNOWFLAKE_", perfil), f"{campo} no usa env_var()"
