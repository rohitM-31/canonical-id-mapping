import pytest

from src.ingestion.load_data import SchemaError, read_reference_file, read_source_file
from tests.conftest import HEADER, rec


def test_reads_rows_with_numbers(write_csv):
    f = read_source_file(write_csv("a.csv", [rec(), rec(sid="crm-00000002")]))
    assert [n for n, _ in f.rows] == [1, 2]
    assert f.rows[1][1]["source_id"] == "crm-00000002"


def test_zero_byte_file_is_empty_batch(tmp_path):
    p = tmp_path / "empty.csv"
    p.write_text("")
    assert read_source_file(p).rows == []


def test_header_only_is_empty_batch(write_csv):
    assert read_source_file(write_csv("h.csv", [])).rows == []


def test_missing_required_column_is_schema_error(write_csv):
    path = write_csv("bad.csv", [["crm", "crm-00000001", "2025-01-01T00:00:00Z", "a@b.com"]], header=HEADER[:4])
    with pytest.raises(SchemaError, match="phone"):
        read_source_file(path)


def test_extra_column_is_reported_not_fatal(write_csv):
    path = write_csv("x.csv", [rec() + ["surprise"]], header=HEADER + ["new_col"])
    f = read_source_file(path)
    assert f.extra_columns == ["new_col"] and len(f.rows) == 1


def test_wrong_field_count_is_flagged_malformed(tmp_path):
    p = tmp_path / "m.csv"
    p.write_text(",".join(HEADER) + "\ncrm,crm-00000001,2025-01-01T00:00:00Z,only,three\n")
    (_, record), = read_source_file(p).rows
    assert record["_malformed"] is True


def test_bom_and_header_case_are_tolerated(tmp_path):
    p = tmp_path / "bom.csv"
    p.write_text("﻿" + ",".join(h.upper() for h in HEADER) + "\ncrm,crm-00000001,2025-01-01T00:00:00Z,,,\n",
                 encoding="utf-8")
    assert len(read_source_file(p).rows) == 1


def test_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_source_file(tmp_path / "nope.csv")


def test_reference_file(master_file):
    assert len(read_reference_file(master_file)) == 5
