"""Performance measurement + the one optimisation we made (in-memory master index vs a SQL query per record).

  python scripts/benchmark.py --rows 100000 --customers 20000
"""
import argparse
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import generate_large_dataset as gen  # noqa: E402
from src.database.database import connect, init_schema  # noqa: E402
from src.matching.matching_rules import MasterIndex, resolve  # noqa: E402
from src.pipeline import load_reference, run_pipeline  # noqa: E402
from src.transformation.normalize_data import normalize_email, normalize_name_key, normalize_phone  # noqa: E402


def naive_resolve(conn, email, phone, name):
    """Baseline: one indexed SQL lookup per identifier per record."""
    ids = set()
    if email:
        ids |= {r[0] for r in conn.execute("SELECT canonical_id FROM canonical_customer WHERE email_norm=?", (email,))}
    if phone:
        ids |= {r[0] for r in conn.execute("SELECT canonical_id FROM canonical_customer WHERE phone_norm=?", (phone,))}
    if name:
        ids |= {r[0] for r in conn.execute("SELECT canonical_id FROM canonical_customer WHERE name_key=?", (name,))}
    return ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--customers", type=int, default=20_000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        rows, labels = gen.generate(args.rows, args.customers, args.seed)
        gen.write_outputs(tmp, rows, labels)
        gen.write_reference(tmp / "canonical_customers.csv", args.customers)
        db = tmp / "bench.db"

        conn = connect(db)
        init_schema(conn)
        load_reference(conn, tmp / "canonical_customers.csv")
        conn.close()

        t0 = time.perf_counter()
        m = run_pipeline(tmp / "dataset_large.csv", db_path=db)
        total = time.perf_counter() - t0
        print(f"full pipeline: {m['rows_read']} rows in {total:.2f}s -> {m['rows_read'] / total:,.0f} rows/s")
        print("  stage timings:", m["timings"])

        t0 = time.perf_counter()
        m2 = run_pipeline(tmp / "dataset_large.csv", db_path=db)
        print(f"rerun of same file (idempotent): {time.perf_counter() - t0:.2f}s, "
              f"inserted={m2['rows_inserted']} existing={m2['rows_existing']}")

        # optimisation: in-memory hash index vs SQL lookup per record
        conn = connect(db)
        sample = rows[:20_000]
        prepared = [(normalize_email(r["email"]), normalize_phone(r["phone"]), normalize_name_key(r["full_name"]))
                    for r in sample]
        index = MasterIndex.from_db(conn)
        t0 = time.perf_counter()
        for e, p, n in prepared:
            resolve(e, p, n, index)
        fast = time.perf_counter() - t0
        t0 = time.perf_counter()
        for e, p, n in prepared:
            naive_resolve(conn, e, p, n)
        slow = time.perf_counter() - t0
        print(f"matching {len(prepared)} records: in-memory index {fast:.3f}s vs SQL-per-record {slow:.3f}s "
              f"-> {slow / fast:.1f}x faster")
        conn.close()


if __name__ == "__main__":
    main()
