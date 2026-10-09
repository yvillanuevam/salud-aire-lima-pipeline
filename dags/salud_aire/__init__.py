"""Logica de negocio del pipeline Salud-Aire Lima.

Este paquete NO importa nada de Airflow a nivel de modulo: todo lo que esta
aqui se puede probar con pytest en milisegundos, sin scheduler ni base de
metadatos. Los DAGs (dags/*.py) solo orquestan estas funciones.
"""
