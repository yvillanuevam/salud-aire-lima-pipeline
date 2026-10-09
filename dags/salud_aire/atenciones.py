"""Validacion del archivo diario de atenciones respiratorias (fuente SFTP).

El archivo lo genera cada clinica exportando desde Excel, asi que llega
"imperfecto": separador ';', codificacion Windows-1252, encabezados con tildes
y simbolos (``N° Atenciones``), fechas en formato dd/mm/aaaa, celdas vacias o
con "N/D", etc.

Division de responsabilidades:
  * Aqui (Python, antes de cargar): problemas ESTRUCTURALES y de tipo de dato.
    Una fila que no se puede interpretar va a cuarentena con su motivo.
  * En dbt (despues de cargar): reglas de NEGOCIO (normalizar distritos,
    deduplicar reenvios, cruzar con calidad del aire).
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime

COLUMNAS_CANONICAS = [
    "fecha_atencion",
    "codigo_clinica",
    "distrito",
    "grupo_edad",
    "diagnostico_cie10",
    "num_atenciones",
]

# Variantes de encabezado observadas en los archivos de las clinicas
# (ya normalizadas: minusculas, sin tildes, separadas por "_").
ALIAS_COLUMNAS = {
    "fecha_atencion": "fecha_atencion",
    "fecha": "fecha_atencion",
    "codigo_clinica": "codigo_clinica",
    "cod_clinica": "codigo_clinica",
    "clinica": "codigo_clinica",
    "distrito": "distrito",
    "distrito_paciente": "distrito",
    "grupo_edad": "grupo_edad",
    "grupo_etario": "grupo_edad",
    "diagnostico_cie10": "diagnostico_cie10",
    "cie10": "diagnostico_cie10",
    "diagnostico": "diagnostico_cie10",
    "num_atenciones": "num_atenciones",
    "n_atenciones": "num_atenciones",
    "nro_atenciones": "num_atenciones",
    "atenciones": "num_atenciones",
}

GRUPOS_EDAD_VALIDOS = {"0-4", "5-17", "18-59", "60+"}
PATRON_CIE10_RESPIRATORIO = re.compile(r"^J\d{2}(\.\d{1,2})?$")
FORMATOS_FECHA = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y")


class ErrorEstructuraArchivo(ValueError):
    """El archivo no se puede procesar en absoluto (vacio, sin columnas clave)."""


class ErrorCalidadDatos(ValueError):
    """El archivo se proceso, pero la proporcion de filas rechazadas es inaceptable."""


@dataclass
class ResultadoValidacion:
    validas: list[dict] = field(default_factory=list)
    rechazadas: list[dict] = field(default_factory=list)
    codificacion: str = ""
    separador: str = ""

    @property
    def total(self) -> int:
        return len(self.validas) + len(self.rechazadas)

    @property
    def tasa_rechazo(self) -> float:
        return len(self.rechazadas) / self.total if self.total else 0.0


def normalizar_encabezado(texto: str) -> str:
    """'N° Atenciones ' -> 'n_atenciones';  'Fecha Atención' -> 'fecha_atencion'."""
    sin_tildes = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    limpio = re.sub(r"[^a-z0-9]+", "_", sin_tildes.lower())
    return limpio.strip("_")


def decodificar(contenido: bytes) -> tuple[str, str]:
    """Intenta UTF-8 (con o sin BOM) y si falla cae a Windows-1252 (Excel)."""
    try:
        return contenido.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return contenido.decode("cp1252"), "cp1252"


def detectar_separador(primera_linea: str) -> str:
    candidatos = [";", ",", "|", "\t"]
    conteos = {sep: primera_linea.count(sep) for sep in candidatos}
    separador, conteo = max(conteos.items(), key=lambda par: par[1])
    if conteo == 0:
        raise ErrorEstructuraArchivo("No se pudo detectar el separador del CSV")
    return separador


def parsear_fecha(texto: str) -> date:
    texto = (texto or "").strip()
    for formato in FORMATOS_FECHA:
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    raise ValueError(f"fecha invalida '{texto}'")


def parsear_entero_no_negativo(texto: str) -> int:
    texto = (texto or "").strip()
    if not re.fullmatch(r"\d+(\.0+)?", texto):
        raise ValueError(f"num_atenciones invalido '{texto}'")
    return int(float(texto))


def _validar_fila(fila: dict, fecha_proceso: date) -> tuple[dict | None, list[str]]:
    errores: list[str] = []
    limpia: dict = {}

    try:
        fecha = parsear_fecha(fila.get("fecha_atencion", ""))
        if fecha != fecha_proceso:
            errores.append(f"fecha {fecha.isoformat()} distinta al dia procesado")
        limpia["fecha_atencion"] = fecha.isoformat()
    except ValueError as exc:
        errores.append(str(exc))

    for columna in ("codigo_clinica", "distrito"):
        valor = (fila.get(columna) or "").strip()
        if not valor:
            errores.append(f"{columna} vacio")
        limpia[columna] = valor

    grupo = re.sub(r"\s+", "", fila.get("grupo_edad") or "")
    if grupo not in GRUPOS_EDAD_VALIDOS:
        errores.append(f"grupo_edad invalido '{fila.get('grupo_edad')}'")
    limpia["grupo_edad"] = grupo

    cie10 = (fila.get("diagnostico_cie10") or "").strip().upper()
    if not PATRON_CIE10_RESPIRATORIO.match(cie10):
        errores.append(f"diagnostico_cie10 no respiratorio o invalido '{cie10}'")
    limpia["diagnostico_cie10"] = cie10

    try:
        limpia["num_atenciones"] = parsear_entero_no_negativo(fila.get("num_atenciones", ""))
    except ValueError as exc:
        errores.append(str(exc))

    return (None if errores else limpia), errores


def validar_atenciones(contenido: bytes, fecha_proceso: date) -> ResultadoValidacion:
    """Valida el CSV crudo y separa filas validas de filas en cuarentena.

    Lanza ErrorEstructuraArchivo si el archivo es inutilizable (vacio o sin
    columnas obligatorias): en ese caso no tiene sentido cargar nada.
    """
    if not contenido or not contenido.strip():
        raise ErrorEstructuraArchivo("El archivo de atenciones esta vacio")

    texto, codificacion = decodificar(contenido)
    primera_linea = texto.splitlines()[0]
    separador = detectar_separador(primera_linea)

    lector = csv.reader(io.StringIO(texto), delimiter=separador)
    encabezado_original = next(lector)
    encabezado = [ALIAS_COLUMNAS.get(normalizar_encabezado(c), normalizar_encabezado(c))
                  for c in encabezado_original]

    faltantes = [c for c in COLUMNAS_CANONICAS if c not in encabezado]
    if faltantes:
        raise ErrorEstructuraArchivo(
            f"Faltan columnas obligatorias {faltantes}. Encabezado recibido: {encabezado_original}"
        )

    resultado = ResultadoValidacion(codificacion=codificacion, separador=separador)
    for numero_fila, valores in enumerate(lector, start=2):  # fila 1 = encabezado
        if not any(v.strip() for v in valores):
            continue  # filas totalmente vacias al final del Excel
        fila = dict(zip(encabezado, valores, strict=False))
        limpia, errores = _validar_fila(fila, fecha_proceso)
        if limpia is not None:
            limpia["_numero_fila"] = numero_fila
            resultado.validas.append(limpia)
        else:
            rechazada = {c: fila.get(c, "") for c in COLUMNAS_CANONICAS}
            rechazada["_numero_fila"] = numero_fila
            rechazada["motivo_rechazo"] = "; ".join(errores)
            resultado.rechazadas.append(rechazada)
    return resultado


def verificar_umbral_rechazo(resultado: ResultadoValidacion, umbral: float) -> None:
    """Falla de forma explicita (nunca silenciosa) si se rechaza demasiado."""
    if resultado.total == 0:
        raise ErrorCalidadDatos("El archivo no contiene filas de datos")
    if resultado.tasa_rechazo > umbral:
        raise ErrorCalidadDatos(
            f"Se rechazo el {resultado.tasa_rechazo:.1%} de las filas "
            f"({len(resultado.rechazadas)}/{resultado.total}), sobre el umbral de {umbral:.0%}. "
            "Revisar el archivo en la capa 'cuarentena' del data lake y contactar a la clinica."
        )


def filas_a_csv(filas: list[dict], columnas: list[str]) -> bytes:
    """Serializa a CSV estandar (UTF-8, coma, encabezado) para el data lake."""
    salida = io.StringIO()
    escritor = csv.DictWriter(salida, fieldnames=columnas, extrasaction="ignore", lineterminator="\n")
    escritor.writeheader()
    escritor.writerows(filas)
    return salida.getvalue().encode("utf-8")


def csv_a_filas(contenido: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(contenido.decode("utf-8"))))
