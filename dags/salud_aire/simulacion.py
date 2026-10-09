"""Generadores de datos SIMULADOS (permitido por el enunciado, seccion 12).

1) ``generar_csv_atenciones``: simula al proveedor externo (la red de
   clinicas). En produccion este codigo NO existe: las clinicas suben su
   archivo al SFTP por su cuenta. Aqui lo usa el DAG ``simulador_envio_clinicas``
   para que el pipeline tenga un archivo real que esperar y procesar.
   El archivo sale con los defectos tipicos de un export de Excel.

2) ``medicion_simulada``: plan B de la fuente de calidad del aire, solo si la
   Variable ``fuente_calidad_aire`` = "simulada" (por ejemplo, si OpenAQ esta
   caido el dia de la demo). Nunca se activa en silencio.

Todo es deterministico por fecha (misma fecha -> mismos datos), para que los
reprocesos y los tests sean reproducibles.
"""

from __future__ import annotations

import csv
import hashlib
import io
import random
from datetime import date

# Estaciones de referencia (coordenadas aproximadas de la red de monitoreo de
# Lima). Se marcan como simuladas en el id para que nunca se confundan con datos reales.
ESTACIONES_SIMULADAS = [
    {
        "estacion_id": "SIM-ATE",
        "estacion_nombre": "Estacion simulada Ate",
        "latitud": -12.0262, "longitud": -76.9190, "base_pm25": 34.0,
    },
    {
        "estacion_id": "SIM-CAMPO-MARTE",
        "estacion_nombre": "Estacion simulada Campo de Marte",
        "latitud": -12.0705, "longitud": -77.0433, "base_pm25": 20.0,
    },
    {
        "estacion_id": "SIM-CARABAYLLO",
        "estacion_nombre": "Estacion simulada Carabayllo",
        "latitud": -11.9022, "longitud": -77.0336, "base_pm25": 27.0,
    },
    {
        "estacion_id": "SIM-PUENTE-PIEDRA",
        "estacion_nombre": "Estacion simulada Puente Piedra",
        "latitud": -11.8633, "longitud": -77.0742, "base_pm25": 25.0,
    },
    {
        "estacion_id": "SIM-SAN-BORJA",
        "estacion_nombre": "Estacion simulada San Borja",
        "latitud": -12.1086, "longitud": -77.0078, "base_pm25": 17.0,
    },
    {
        "estacion_id": "SIM-SJL",
        "estacion_nombre": "Estacion simulada San Juan de Lurigancho",
        "latitud": -12.0177, "longitud": -76.9997, "base_pm25": 31.0,
    },
    {
        "estacion_id": "SIM-SMP",
        "estacion_nombre": "Estacion simulada San Martin de Porres",
        "latitud": -12.0089, "longitud": -77.0844, "base_pm25": 26.0,
    },
    {
        "estacion_id": "SIM-SANTA-ANITA",
        "estacion_nombre": "Estacion simulada Santa Anita",
        "latitud": -12.0430, "longitud": -76.9713, "base_pm25": 33.0,
    },
    {
        "estacion_id": "SIM-VMT",
        "estacion_nombre": "Estacion simulada Villa Maria del Triunfo",
        "latitud": -12.1665, "longitud": -76.9201, "base_pm25": 29.0,
    },
]

# Distritos de residencia como los escriben las clinicas (con variantes reales
# de escritura: abreviaturas, mayusculas, espacios de mas, sin tildes...).
DISTRITOS_REPORTADOS = {
    "150101": ["Cercado de Lima", "LIMA", "Lima "],
    "150103": ["Ate", "ATE VITARTE", "Ate-Vitarte"],
    "150106": ["Carabayllo", "CARABAYLLO"],
    "150110": ["Comas", "COMAS "],
    "150122": ["Miraflores", "MIRAFLORES"],
    "150125": ["Puente Piedra", "PTE. PIEDRA"],
    "150130": ["San Borja", "SAN BORJA"],
    "150132": ["San Juan de Lurigancho", "S.J.L.", "SJL", "SAN JUAN DE LURIGANCHO"],
    "150135": ["San Martín de Porres", "SAN MARTIN DE PORRES", "S.M.P."],
    "150137": ["Santa Anita", "STA. ANITA"],
    "150142": ["Villa El Salvador", "V.E.S."],
    "150143": ["Villa María del Triunfo", "VILLA MARIA DEL TRIUNFO", "V.M.T."],
}

# Peso relativo de atenciones por distrito (mas poblacion -> mas atenciones).
PESO_DISTRITO = {
    "150101": 1.0, "150103": 1.4, "150106": 0.8, "150110": 1.2, "150122": 0.4,
    "150125": 0.9, "150130": 0.5, "150132": 2.0, "150135": 1.5, "150137": 0.7,
    "150142": 1.1, "150143": 1.0,
}

CLINICAS = ["CLI-001", "CLI-002", "CLI-003", "CLI-004", "CLI-005", "CLI-006"]
GRUPOS_EDAD = ["0-4", "5-17", "18-59", "60+"]
PESO_EDAD = {"0-4": 1.6, "5-17": 0.9, "18-59": 0.7, "60+": 1.3}
DIAGNOSTICOS = {"J06": 2.0, "J20": 1.0, "J18": 0.6, "J45": 0.8, "J44": 0.4}

ENCABEZADO_EXCEL = ["Fecha Atención", "Cod. Clínica", "Distrito", "Grupo Edad", "Diagnóstico CIE10", "N° Atenciones"]


def _rng(fecha: date, semilla: str) -> random.Random:
    digest = hashlib.sha256(f"{fecha.isoformat()}|{semilla}".encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


def factor_contaminacion_dia(fecha: date) -> float:
    """Factor comun del dia (0.6 a 1.7): dias de inversion termica / poco viento
    elevan a la vez el PM2.5 y las atenciones respiratorias."""
    return round(_rng(fecha, "factor-dia").uniform(0.6, 1.7), 3)


def sensores_simulados() -> list[dict]:
    return [
        {k: e[k] for k in ("estacion_id", "estacion_nombre", "latitud", "longitud")}
        | {"sensor_id": f"{e['estacion_id']}-PM25"}
        for e in ESTACIONES_SIMULADAS
    ]


def medicion_simulada(sensor: dict, fecha: date) -> dict | None:
    estacion = next(e for e in ESTACIONES_SIMULADAS if e["estacion_id"] == sensor["estacion_id"])
    rng = _rng(fecha, sensor["sensor_id"])
    if rng.random() < 0.05:  # 5% de dias sin dato: las estaciones reales tambien se caen
        return None
    valor = estacion["base_pm25"] * factor_contaminacion_dia(fecha) * rng.uniform(0.85, 1.15)
    return {
        "fecha": fecha.isoformat(),
        "estacion_id": sensor["estacion_id"],
        "estacion_nombre": sensor["estacion_nombre"],
        "latitud": sensor["latitud"],
        "longitud": sensor["longitud"],
        "sensor_id": sensor["sensor_id"],
        "parametro": "pm25",
        "unidad": "µg/m³",
        "valor_promedio": round(valor, 3),
        "cobertura_pct": round(rng.uniform(70, 100), 1),
        "n_observaciones": rng.randint(17, 24),
        "fuente": "simulada",
    }


def generar_filas_atenciones(fecha: date) -> list[list[str]]:
    """Filas (sin encabezado) del archivo que envian las clinicas."""
    rng = _rng(fecha, "atenciones")
    factor = factor_contaminacion_dia(fecha)
    fecha_excel = fecha.strftime("%d/%m/%Y")  # formato regional de Excel
    filas: list[list[str]] = []

    for clinica in CLINICAS:
        for ubigeo, variantes in DISTRITOS_REPORTADOS.items():
            if rng.random() < 0.35:  # cada clinica atiende pacientes de algunos distritos
                continue
            for grupo in GRUPOS_EDAD:
                for cie10, peso_dx in DIAGNOSTICOS.items():
                    esperado = 1.2 * PESO_DISTRITO[ubigeo] * PESO_EDAD[grupo] * peso_dx * factor
                    cantidad = max(0, round(rng.gauss(esperado, esperado * 0.35)))
                    if cantidad == 0:
                        continue
                    filas.append([fecha_excel, clinica, rng.choice(variantes), grupo, cie10, str(cantidad)])

    # --- Defectos tipicos que el pipeline debe manejar ---
    if filas:
        for fila in rng.sample(filas, k=max(1, len(filas) // 50)):
            filas.append(list(fila))  # reenvio de filas: duplicados exactos
        filas[rng.randrange(len(filas))][5] = "N/D"  # celda sin dato
        filas[rng.randrange(len(filas))][5] = "-3"  # valor negativo
        filas[rng.randrange(len(filas))][4] = "Z00"  # codigo no respiratorio
        filas[rng.randrange(len(filas))][3] = "60 +"  # espacio extra (se corrige)
    filas.append([fecha_excel, "CLI-004", "CALLAO", "18-59", "J06", "4"])  # fuera de Lima Metropolitana
    filas.append(["", "", "", "", "", ""])  # fila vacia al final del Excel
    return filas


def generar_csv_atenciones(fecha: date) -> bytes:
    """CSV como lo exporta Excel en Windows: ';' y codificacion cp1252."""
    salida = io.StringIO()
    escritor = csv.writer(salida, delimiter=";", lineterminator="\r\n")
    escritor.writerow(ENCABEZADO_EXCEL)
    escritor.writerows(generar_filas_atenciones(fecha))
    return salida.getvalue().encode("cp1252")
