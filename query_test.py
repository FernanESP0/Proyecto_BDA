import time
from decimal import Decimal
from tabulate import tabulate # type:ignore
from dw import DW
import extract as extract


def pretty_print_result(result, query_name: str):
    """Imprime los resultados de cada query en formato tabular y con cabeceras adecuadas."""

    if not result:
        print("(Sin resultados)\n")
        return

    # Selección automática de cabeceras según la query
    headers_map = {
        "utilization": [
            "Manufacturer", "Year", "FH", "Takeoffs", "ADOSS", "ADOSU", "ADOS",
            "ADIS", "DU", "DC", "DYR(%)", "CNR(%)", "TDR(%)", "ADD"
        ],
        "reporting": ["Manufacturer", "Year", "RRh", "RRc"],
        "reporting_per_role": ["Manufacturer", "Year", "Role", "RRh", "RRc"],
    }

    headers = headers_map.get(query_name, None)

    # Limpieza de datos (convertir Decimal → float o str)
    formatted = []
    for row in result:
        formatted.append([
            float(x) if isinstance(x, Decimal) else x for x in row
        ])

    print(tabulate(formatted, headers=headers, tablefmt="fancy_grid", numalign="right"))


def time_and_print(function, query_name: str):
    """Ejecuta la query, mide el tiempo y muestra los resultados formateados."""
    start = time.perf_counter()
    result = function()
    end = time.perf_counter()

    pretty_print_result(result, query_name)
    print(f"⏱  Execution time: {end - start:.3f} s\n")


if __name__ == "__main__":
    dw = DW(create=False)

    print("\n═══════════════════════════ Query Aircraft Utilization ═══════════════════════════")
    #print("================================ DW ======================================")
    #time_and_print(dw.query_utilization, "utilization")
    print("============================= Baseline ===================================")
    time_and_print(extract.query_utilization_baseline, "utilization")

    print("\n════════════════════════════════ Query Reporting ══════════════════════════════════")
    #print("================================ DW ======================================")
    #time_and_print(dw.query_reporting, "reporting")
    print("============================= Baseline ===================================")
    time_and_print(extract.query_reporting_baseline, "reporting")

    print("\n══════════════════════════ Query Reporting per Role ═══════════════════════════════")
    #print("================================ DW ======================================")
    #time_and_print(dw.query_reporting_per_role, "reporting_per_role")
    print("============================= Baseline ===================================")
    time_and_print(extract.query_reporting_per_role_baseline, "reporting_per_role")

    dw.close()
