"""Capa de consumo: reporte diario a partir del mart de dbt."""

from __future__ import annotations

COLUMNAS_REPORTE = [
    "distrito",
    "zona",
    "total_atenciones",
    "atenciones_menores_5",
    "atenciones_adultos_mayores",
    "pm25_promedio_24h",
    "categoria_aire",
    "variacion_vs_7d_pct",
    "alerta_salud_publica",
]

ETIQUETAS_CATEGORIA = {
    "DENTRO_GUIA_OMS": "Dentro de la guia OMS",
    "SOBRE_GUIA_OMS": "Sobre la guia OMS",
    "SOBRE_ECA_PERU": "Sobre el ECA Peru",
    "SIN_COBERTURA": "Sin estacion cercana",
}


def _fmt(valor, decimales: int = 1) -> str:
    if valor is None:
        return "-"
    if isinstance(valor, bool):
        return "SI" if valor else "no"
    if isinstance(valor, float):
        return f"{valor:.{decimales}f}"
    return str(valor)


def construir_reporte(fecha_iso: str, filas: list[dict]) -> str:
    """Genera el reporte en Markdown. ``filas`` viene del mart (claves en minuscula)."""
    if not filas:
        raise ValueError(
            f"El mart no tiene filas para {fecha_iso}: la transformacion no produjo datos."
        )

    ordenadas = sorted(
        filas,
        key=lambda f: (
            not f.get("alerta_salud_publica"),
            -(f.get("pm25_promedio_24h") or -1),
            -(f.get("total_atenciones") or 0),
        ),
    )
    alertas = [f for f in filas if f.get("alerta_salud_publica")]
    con_pm25 = [f for f in filas if f.get("pm25_promedio_24h") is not None]
    total_atenciones = sum(int(f.get("total_atenciones") or 0) for f in filas)

    lineas = [
        f"# Reporte Salud-Aire Lima — {fecha_iso}",
        "",
        f"- Distritos analizados: **{len(filas)}**",
        f"- Atenciones respiratorias reportadas: **{total_atenciones}**",
        f"- Distritos con medicion de PM2.5: **{len(con_pm25)}**",
    ]
    if con_pm25:
        peor = max(con_pm25, key=lambda f: f["pm25_promedio_24h"])
        lineas.append(
            f"- Mayor PM2.5 del dia: **{peor['pm25_promedio_24h']:.1f} µg/m³** en {peor['distrito']}"
        )
    lineas.append(f"- Distritos en ALERTA de salud publica: **{len(alertas)}**")
    if alertas:
        lineas.append("  - " + ", ".join(sorted(f["distrito"] for f in alertas)))

    lineas += [
        "",
        "| Distrito | Zona | Atenciones | <5 años | 60+ | PM2.5 (µg/m³) | Calidad del aire | Var. vs 7 dias | Alerta |",
        "|---|---|---:|---:|---:|---:|---|---:|---|",
    ]
    for f in ordenadas:
        variacion = f.get("variacion_vs_7d_pct")
        lineas.append(
            "| {distrito} | {zona} | {tot} | {men} | {may} | {pm} | {cat} | {var} | {ale} |".format(
                distrito=f.get("distrito"),
                zona=f.get("zona") or "-",
                tot=_fmt(f.get("total_atenciones")),
                men=_fmt(f.get("atenciones_menores_5")),
                may=_fmt(f.get("atenciones_adultos_mayores")),
                pm=_fmt(f.get("pm25_promedio_24h")),
                cat=ETIQUETAS_CATEGORIA.get(f.get("categoria_aire"), f.get("categoria_aire")),
                var="-" if variacion is None else f"{float(variacion):+.1f}%",
                ale=_fmt(bool(f.get("alerta_salud_publica"))),
            )
        )
    lineas += [
        "",
        "Criterios: guia OMS 2021 PM2.5 24 h = 15 µg/m³; ECA Peru PM2.5 24 h = 50 µg/m³. "
        "Alerta = aire sobre la guia OMS y atenciones al menos 20% sobre el promedio de "
        "los 7 dias previos.",
    ]
    return "\n".join(lineas) + "\n"
