"""Tests de la validacion del archivo de atenciones (fuente SFTP)."""

from datetime import date

import pytest

from salud_aire.atenciones import (
    ErrorCalidadDatos,
    ErrorEstructuraArchivo,
    csv_a_filas,
    filas_a_csv,
    normalizar_encabezado,
    validar_atenciones,
    verificar_umbral_rechazo,
)

FECHA = date(2026, 10, 7)


def _csv_excel(*filas: str) -> bytes:
    """Arma un CSV como lo exporta Excel en Windows (';' y cp1252)."""
    encabezado = "Fecha Atención;Cod. Clínica;Distrito;Grupo Edad;Diagnóstico CIE10;N° Atenciones"
    return "\r\n".join([encabezado, *filas]).encode("cp1252")


@pytest.mark.parametrize(
    ("original", "esperado"),
    [
        ("N° Atenciones", "n_atenciones"),
        ("Fecha Atención", "fecha_atencion"),
        (" Cod. Clínica ", "cod_clinica"),
        ("Diagnóstico CIE10", "diagnostico_cie10"),
    ],
)
def test_normalizar_encabezado_quita_tildes_y_simbolos(original, esperado):
    assert normalizar_encabezado(original) == esperado


def test_archivo_excel_cp1252_con_punto_y_coma_se_interpreta_bien():
    contenido = _csv_excel("07/10/2026;CLI-001;San Martín de Porres;0-4;J45;12")

    resultado = validar_atenciones(contenido, FECHA)

    assert resultado.codificacion == "cp1252"
    assert resultado.separador == ";"
    assert resultado.rechazadas == []
    assert resultado.validas == [
        {
            "fecha_atencion": "2026-10-07",
            "codigo_clinica": "CLI-001",
            "distrito": "San Martín de Porres",
            "grupo_edad": "0-4",
            "diagnostico_cie10": "J45",
            "num_atenciones": 12,
            "_numero_fila": 2,
        }
    ]


def test_archivo_utf8_con_coma_y_fecha_iso_tambien_se_acepta():
    contenido = (
        "fecha_atencion,codigo_clinica,distrito,grupo_edad,diagnostico_cie10,num_atenciones\n"
        "2026-10-07,CLI-002,Ate,60+,J18,3\n"
    ).encode("utf-8-sig")

    resultado = validar_atenciones(contenido, FECHA)

    assert resultado.codificacion == "utf-8"
    assert len(resultado.validas) == 1
    assert resultado.validas[0]["num_atenciones"] == 3


@pytest.mark.parametrize(
    ("fila", "motivo_esperado"),
    [
        ("07/10/2026;CLI-001;Ate;0-4;J45;N/D", "num_atenciones invalido"),
        ("07/10/2026;CLI-001;Ate;0-4;J45;-3", "num_atenciones invalido"),
        ("07/10/2026;CLI-001;Ate;0-4;Z00;5", "no respiratorio"),
        ("07/10/2026;CLI-001;Ate;bebes;J45;5", "grupo_edad invalido"),
        ("06/10/2026;CLI-001;Ate;0-4;J45;5", "distinta al dia procesado"),
        ("31/02/2026;CLI-001;Ate;0-4;J45;5", "fecha invalida"),
        ("07/10/2026;CLI-001;;0-4;J45;5", "distrito vacio"),
    ],
)
def test_filas_invalidas_van_a_cuarentena_con_su_motivo(fila, motivo_esperado):
    resultado = validar_atenciones(_csv_excel(fila), FECHA)

    assert resultado.validas == []
    assert len(resultado.rechazadas) == 1
    assert motivo_esperado in resultado.rechazadas[0]["motivo_rechazo"]
    assert resultado.rechazadas[0]["_numero_fila"] == 2


def test_espacios_en_grupo_de_edad_se_corrigen_y_filas_vacias_se_ignoran():
    contenido = _csv_excel("07/10/2026;CLI-003;Comas;60 +;J06;7", ";;;;;", "")

    resultado = validar_atenciones(contenido, FECHA)

    assert resultado.total == 1
    assert resultado.validas[0]["grupo_edad"] == "60+"


def test_archivo_sin_columna_obligatoria_falla_con_mensaje_claro():
    contenido = "Fecha;Clinica;Distrito\r\n07/10/2026;CLI-001;Ate".encode("cp1252")

    with pytest.raises(ErrorEstructuraArchivo, match="Faltan columnas obligatorias"):
        validar_atenciones(contenido, FECHA)


def test_archivo_vacio_falla():
    with pytest.raises(ErrorEstructuraArchivo, match="vacio"):
        validar_atenciones(b"   \r\n", FECHA)


def test_umbral_de_rechazo_falla_de_forma_explicita():
    contenido = _csv_excel(
        "07/10/2026;CLI-001;Ate;0-4;J45;4",
        "07/10/2026;CLI-001;Ate;0-4;J20;N/D",
        "07/10/2026;CLI-001;Ate;0-4;J18;N/D",
    )
    resultado = validar_atenciones(contenido, FECHA)

    verificar_umbral_rechazo(resultado, umbral=0.70)  # 66.7% < 70%: pasa
    with pytest.raises(ErrorCalidadDatos, match="cuarentena"):
        verificar_umbral_rechazo(resultado, umbral=0.20)


def test_csv_estandar_ida_y_vuelta_conserva_los_datos():
    filas = [{"a": "1", "b": "San Martín"}, {"a": "2", "b": "Ate"}]

    assert csv_a_filas(filas_a_csv(filas, ["a", "b"])) == filas
