"""File readers: source event files and the canonical reference file."""
import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

from config import config

log = logging.getLogger(__name__)


class SchemaError(Exception):
    """The file does not satisfy the source contract (e.g. required column missing)."""


@dataclass
class SourceFile:
    path: Path
    rows: List[Tuple[int, Dict[str, str]]] = field(default_factory=list)  # (1-based data row number, record)
    extra_columns: List[str] = field(default_factory=list)


def read_source_file(path) -> SourceFile:
    """Read a CSV of source events.

    * 0-byte file or header-only file -> empty batch (no rows, no error)
    * missing required column          -> SchemaError (whole file refused)
    * extra column                     -> kept in `extra_columns`, ignored
    * row with wrong number of fields  -> row kept, flagged `_malformed` so it lands in quarantine
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"input file not found: {path}")

    result = SourceFile(path=path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            log.warning("empty file (no header): %s", path.name)
            return result
        header = [h.strip().lower() for h in header]

        missing = [c for c in config.REQUIRED_COLUMNS if c not in header]
        if missing:
            raise SchemaError(f"missing required columns {missing}; found {header}")
        result.extra_columns = [c for c in header if c not in config.REQUIRED_COLUMNS]

        for row_number, values in enumerate(reader, start=1):
            if not values:  # blank line
                continue
            if len(values) != len(header):
                record = {c: "" for c in config.REQUIRED_COLUMNS}
                record["_malformed"] = True
                record["_raw_line"] = ",".join(values)
            else:
                record = dict(zip(header, values))
            result.rows.append((row_number, record))
    return result


def read_reference_file(path) -> List[Dict[str, str]]:
    """Canonical master: columns email, phone, full_name."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"reference file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [c for c in ("email", "full_name") if c not in (reader.fieldnames or [])]
        if missing:
            raise SchemaError(f"reference file missing columns {missing}")
        return list(reader)
