"""Run-time expansion of `${run.date}`, `${run.id}` and `${params.<name>}` in task statements."""
from __future__ import annotations

import re

_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")


def expand(text: str, *, run_date: str, run_id: str, params: dict[str, str]) -> str:
    """Replace every placeholder; an unknown one is an error, never silently left in the SQL."""
    values = {"run.date": run_date, "run.id": run_id}
    values.update({f"params.{key}": value for key, value in params.items()})

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise ValueError(f"unknown placeholder ${{{key}}}")
        return values[key]

    return _PLACEHOLDER.sub(replace, text)
