"""
Ad-hoc query runner and pretty-printer

This module provides small utilities to time query execution and render results in
readable tables for quick, manual inspection during development. It can run both
baseline queries executed directly against the source database (extract module) and
placeholder DW queries (dw module).

Usage
- Run this script directly to execute the baseline queries and print a fancy table
  along with execution time for each query.
"""

import time
from decimal import Decimal
from tabulate import tabulate # type:ignore
from dw import DW
import extract as extract


def pretty_print_result(result, query_name: str):
    """
    Render a 2D result set as a formatted table with contextual headers.

    Parameters
    - result: Iterable of rows (tuples/lists) returned by a query.
    - query_name: Logical name of the query to select appropriate header labels.

    Behavior
    - Automatically picks column headers from a pre-defined map for known queries.
    - Attempts to coerce Decimal values to float for nicer display.
    - Prints "(No results)" if the sequence is empty.
    """

    if not result:
        print("(No results)\n")
        return

    # Pick headers depending on the logical query name
    headers_map = {
        "utilization": [
            "Manufacturer", "Year", "FH", "Takeoffs", "ADOSS", "ADOSU", "ADOS",
            "ADIS", "DU", "DC", "DYR(%)", "CNR(%)", "TDR(%)", "ADD"
        ],
        "reporting": ["Manufacturer", "Year", "RRh", "RRc"],
        "reporting_per_role": ["Manufacturer", "Year", "Role", "RRh", "RRc"],
    }

    headers = headers_map.get(query_name, None)

    # Convert Decimal to float for display when applicable
    formatted = []
    for row in result:
        formatted.append([
            float(x) if isinstance(x, Decimal) else x for x in row
        ])

    print(tabulate(formatted, headers=headers, tablefmt="fancy_grid", numalign="right"))


def time_and_print(function, query_name: str):
    """
    Execute a callable, measure wall-clock runtime, and pretty-print the results.

    Parameters
    - function: Zero-argument callable that returns a sequence of rows.
    - query_name: Logical name used to select headers in pretty_print_result.
    """
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
