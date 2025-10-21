# Proyecto_BDA

Breve README para el proyecto ETL / Data Warehouse contenido en este repositorio.

## Propósito
Repositorio de ejemplo que implementa extracción, transformación y carga (ETL) de datos
de vuelos y logbooks hacia un Data Warehouse (DuckDB). Incluye utilidades para ejecutar
consultas analíticas de ejemplo.

## Estructura principal
- `extract.py` — funciones para extraer datos desde PostgreSQL y CSVs.
- `transform.py` — transformaciones, limpieza y agregaciones con pandas.
- `load.py` — carga de dimensiones y hechos en el DW usando `pygrametl`.
- `dw.py` — definición de esquema DW y objetos `pygrametl` (CachedDimension, FactTable).
- `etl_control_flow.py` — orquestador que coordina extracción → transformación → carga.
- `query_test.py` — script para ejecutar consultas de ejemplo y mostrar resultados.
- `requirements.txt` — dependencias de terceros inferidas del código.

## Requisitos y setup (Windows PowerShell)
Recomiendo usar un entorno virtual para instalar dependencias:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r .\requirements.txt
```

Si quieres versiones exactas y reproducibles:

```powershell
pip freeze > requirements-frozen.txt
```

## Configuración de la conexión a PostgreSQL
El archivo `extract.py` espera un fichero `db_conf.txt` en la raíz con líneas en formato
`key=value` con estas claves: `dbname`, `user`, `password`, `ip`, `port`.

## Ejecución rápida
- Extraer/transformar/cargar (orquestador):

```powershell
python etl_control_flow.py
```

- Ejecutar consultas de ejemplo (usa la extracción baseline que accede a la BD):

```powershell
python query_test.py
```

## Notas sobre tipos y editores
- El proyecto usa `# type: ignore` en algunos imports (por ejemplo `tabulate`) para evitar
  errores de type checkers cuando no hay stubs disponibles.
- Si usas mypy/pyright y quieres mayor precisión, instala paquetes de tipos o crea un
  `pyrightconfig.json`/`mypy.ini` para ajustar la verificación.

## Siguientes pasos sugeridos
- Confirmar versiones de `pygrametl` y `duckdb` en el entorno de producción.
- Añadir tests unitarios (pytest) para las transformaciones.
- (Opcional) Crear `requirements-dev.txt` con herramientas de desarrollo (black, isort, mypy).
