import csv

import pytest

from src.database.database import connect, init_schema
from src.pipeline import load_reference, run_pipeline

HEADER = ["source_system", "source_id", "event_ts", "email", "phone", "full_name"]

# three clean customers + two who share a name key ("dan lee") to exercise ambiguity
MASTER = [
    ("alice.one@example.com", "(200) 000-0001", "Alice B Jones"),
    ("bob.two@example.com", "(200) 000-0002", "Bob C Brown"),
    ("carol.three@example.com", "2000000003", "Carol D White"),
    ("dan.lee4@example.com", "(200) 000-0004", "Dan A Lee"),
    ("dan.lee5@example.com", "(200) 000-0005", "Dan B Lee"),
]


def rec(system="crm", sid="crm-00000001", ts="2025-01-01T00:00:00Z", email="", phone="", name=""):
    return [system, sid, ts, email, phone, name]


@pytest.fixture
def master_file(tmp_path):
    path = tmp_path / "master.csv"
    with path.open("w", newline="") as h:
        w = csv.writer(h)
        w.writerow(["email", "phone", "full_name"])
        w.writerows(MASTER)
    return path


@pytest.fixture
def write_csv(tmp_path):
    def _write(name, rows, header=HEADER):
        path = tmp_path / name
        with path.open("w", newline="") as h:
            w = csv.writer(h)
            if header is not None:
                w.writerow(header)
            w.writerows(rows)
        return path
    return _write


@pytest.fixture
def db_path(tmp_path, master_file):
    path = tmp_path / "test.db"
    conn = connect(path)
    init_schema(conn)
    load_reference(conn, master_file)
    conn.close()
    return path


@pytest.fixture
def run(db_path, write_csv):
    """run(rows, name='f.csv', **kw) -> metrics"""
    def _run(rows, name="f.csv", **kw):
        return run_pipeline(write_csv(name, rows), db_path=db_path, **kw)
    return _run


@pytest.fixture
def db(db_path):
    conn = connect(db_path)
    yield conn
    conn.close()
