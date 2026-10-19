import sys
from pathlib import Path

# The support package lives next to the DAG files, exactly where Airflow finds it.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
