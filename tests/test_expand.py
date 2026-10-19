import pytest
from stardelt_pipelines.expand import expand


def test_all_three_kinds_of_placeholder_are_replaced():
    out = expand(
        "SELECT * FROM t WHERE ds = DATE '${run.date}' AND run = '${run.id}' AND n = ${params.days}",
        run_date="2026-10-19",
        run_id="scheduled__2026-10-19T03:00",
        params={"days": "7"},
    )
    assert out == (
        "SELECT * FROM t WHERE ds = DATE '2026-10-19' AND run = 'scheduled__2026-10-19T03:00' AND n = 7"
    )


def test_text_without_placeholders_is_unchanged():
    assert expand("SELECT '$x' || '{y}'", run_date="d", run_id="r", params={}) == "SELECT '$x' || '{y}'"


def test_an_unknown_placeholder_is_an_error_not_left_in_the_sql():
    with pytest.raises(ValueError, match=r"unknown placeholder \$\{params.missing\}"):
        expand("SELECT ${params.missing}", run_date="d", run_id="r", params={})
