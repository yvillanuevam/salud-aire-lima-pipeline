"""Callbacks de alertamiento."""

from __future__ import annotations

import logging

log = logging.getLogger("salud_aire.alertas")


def notificar_fallo(context: dict) -> None:
    """on_failure_callback: deja un mensaje estructurado y facil de buscar en
    los logs. En un ambiente real aqui se enviaria a Slack/Teams/correo usando
    una Connection de Airflow (nunca un webhook escrito en el codigo)."""
    ti = context.get("ti") or context.get("task_instance")
    log.error(
        "[ALERTA salud_aire] dag=%s tarea=%s corrida=%s intento=%s error=%r",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        getattr(ti, "run_id", "?"),
        getattr(ti, "try_number", "?"),
        context.get("exception"),
    )
