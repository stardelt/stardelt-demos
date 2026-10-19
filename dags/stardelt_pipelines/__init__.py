"""Support code for the Pipeline loader (scheduled jobs, phase J0).

`plan` and `expand` are pure; `trino` talks HTTP through an injected client. Only
`dags/pipeline_loader.py` imports Airflow.
"""
