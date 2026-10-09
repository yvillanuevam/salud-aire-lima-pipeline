"""Regla de negocio: que dia procesa cada corrida."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from salud_aire.config import ZONA_HORARIA


def resolver_fecha_proceso(
    fecha_param: str | None,
    fin_intervalo: datetime | None,
    ahora: datetime | None = None,
) -> date:
    """Devuelve el dia de negocio que procesa la corrida.

    Regla: cada corrida procesa el dia ANTERIOR (en hora de Lima), porque las
    clinicas envian el archivo del dia D en la madrugada del dia D+1 y OpenAQ
    publica el promedio diario cuando el dia termina.

    Prioridad:
      1. ``fecha_param`` (parametro manual "fecha_proceso", YYYY-MM-DD) para
         reprocesos o backfills puntuales.
      2. ``fin_intervalo`` (data_interval_end de la corrida programada).
      3. ``ahora`` (corridas manuales sin intervalo de datos).
    """
    if fecha_param:
        try:
            return date.fromisoformat(str(fecha_param).strip())
        except ValueError as exc:
            raise ValueError(
                f"fecha_proceso='{fecha_param}' no tiene formato YYYY-MM-DD"
            ) from exc

    zona = ZoneInfo(ZONA_HORARIA)
    referencia = fin_intervalo or ahora or datetime.now(tz=zona)
    if referencia.tzinfo is None:
        raise ValueError("La fecha de referencia debe tener zona horaria")
    return referencia.astimezone(zona).date() - timedelta(days=1)
