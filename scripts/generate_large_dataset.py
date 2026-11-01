"""Reproducible synthetic data generator (fixed seed).

Writes three separate things so expected answers never leak into the input:
  <out>/dataset_large.csv           input events (same schema as data/input/*.csv)
  <out>/canonical_customers.csv     canonical master used for matching
  <out>/expected_labels.csv         expected outcome per input row      (answer key)
  <out>/expected_conflicts.csv      source keys that truly map to >1 customer (answer key)

Usage:
  python scripts/generate_large_dataset.py --rows 100000 --customers 20000 --seed 42 --out data/large
  python scripts/generate_large_dataset.py --customers 50 --reference-only --out data/reference
"""
import argparse
import csv
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.transformation.normalize_data import make_canonical_id  # noqa: E402

FIRST = ["Aaron", "Abby", "Adam", "Alex", "Amir", "Anna", "Ben", "Bella", "Carl", "Cara", "Cole", "Dina", "Eli",
         "Emma", "Finn", "Fay", "Gabe", "Gina", "Hank", "Hana", "Ian", "Iris", "Jack", "Jane", "Kai", "Kate",
         "Leo", "Lena", "Max", "Maya", "Noah", "Nina", "Omar", "Olga", "Paul", "Pia", "Quin", "Rita", "Sam",
         "Sara", "Tom", "Tina", "Uma", "Vic", "Walt", "Will", "Wren", "Xan", "Yuri", "Yara"]
LAST = ["Smith", "Jones", "Brown", "Taylor", "Wilson", "Davies", "Evans", "Thomas", "Roberts", "Walker", "Wright",
        "Robinson", "Thompson", "White", "Hughes", "Edwards", "Green", "Hall", "Wood", "Harris", "Lewis", "Martin",
        "Jackson", "Clarke", "Clark", "Turner", "Hill", "Scott", "Cooper", "Morris", "Ward", "Moore", "King",
        "Watson", "Baker", "Harrison", "Morgan", "Patel", "Young", "Allen"]
SYSTEMS = ("crm", "web", "billing")


def person(n: int) -> dict:
    """Customer n. n < 50 reproduces the sample master (first.smith<n>@example.com, 'First A Smith')."""
    rest = n // len(FIRST)
    block = rest // len(LAST)  # (first, last) must be unique: the name rule ignores the middle initial
    first, last, middle = FIRST[n % len(FIRST)], LAST[rest % len(LAST)] + (str(block) if block else ""), "A"
    digits = f"200{n:07d}"
    return {
        "n": n, "first": first, "last": last, "middle": middle,
        "email": f"{first.lower()}.{last.lower()}{n}@example.com",
        "phone": f"({digits[:3]}) {digits[3:6]}-{digits[6:]}",
        "digits": digits,
        "full_name": f"{first} {middle} {last}",
    }


def noisy_email(rng, p):
    e = p["email"]
    return rng.choice([e, e.upper(), e.capitalize(), f"  {e} ", e])


def noisy_phone(rng, p):
    d = p["digits"]
    return rng.choice([d, f"{d[:3]}.{d[3:6]}.{d[6:]}", f"({d[:3]}) {d[3:6]}-{d[6:]}", f"1-{d[:3]}-{d[3:6]}-{d[6:]}",
                       f"+1 {d[:3]} {d[3:6]} {d[6:]}"])


def noisy_name(rng, p):
    f, m, l = p["first"], p["middle"], p["last"]
    return rng.choice([f"{f} {m} {l}", f"{f} {m}. {l}", f"{l}, {f} {m}", f"{f} {m} {l}".upper(),
                       f"  {f.lower()}  {m.lower()} {l.lower()} "])


def make_row(rng, p, source_id, ts):
    """Random subset of identifiers (at least one) with formatting noise."""
    use = rng.choice([(1, 0, 0), (1, 1, 0), (1, 1, 1), (0, 1, 0), (0, 0, 1), (0, 1, 1), (1, 0, 1)])
    return {
        "source_system": source_id.split("-")[0], "source_id": source_id, "event_ts": ts,
        "email": noisy_email(rng, p) if use[0] else "",
        "phone": noisy_phone(rng, p) if use[1] else "",
        "full_name": noisy_name(rng, p) if use[2] else "",
    }


def generate(rows: int, customers: int, seed: int):
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)

    def ts():
        return (base + timedelta(seconds=rng.randrange(0, 365 * 86400))).strftime("%Y-%m-%dT%H:%M:%SZ")

    def source_id(owner):  # each customer owns up to 3 ids -> many source ids per customer (normal)
        return f"{rng.choice(SYSTEMS)}-{owner * 3 + rng.randrange(3):08d}"

    items = []  # (row, expected_outcome, expected_canonical_id)
    for i in range(rows):
        roll = rng.random()
        n = rng.randrange(customers)
        p = person(n)
        cid = make_canonical_id(p["email"])
        if roll < 0.80:  # normal
            items.append((make_row(rng, p, source_id(n), ts()), "mapped", cid))
        elif roll < 0.85:  # id re-used by another customer -> one-to-many conflict candidates
            other = rng.randrange(customers)
            items.append((make_row(rng, p, source_id(other), ts()), "mapped", cid))
        elif roll < 0.88:  # nobody in the master
            row = {"source_system": rng.choice(SYSTEMS), "event_ts": ts(), "email": f"nobody{i}@nomatch.test",
                   "phone": f"900{i % 10_000_000:07d}", "full_name": f"Zed Nomatch{i}"}
            row["source_id"] = f"{row['source_system']}-{customers * 3 + i:08d}"
            items.append((row, "unresolved", ""))
        elif roll < 0.90:  # email of one customer, phone of another
            q = person((n + 1) % customers)
            row = make_row(rng, p, source_id(n), ts())
            row.update(email=p["email"], phone=q["digits"], full_name="")
            items.append((row, "unresolved", ""))
        elif roll < 0.91:  # no identifiers at all
            sys_ = rng.choice(SYSTEMS)
            items.append(({"source_system": sys_, "source_id": f"{sys_}-{n * 3:08d}", "event_ts": ts(),
                           "email": "", "phone": "", "full_name": ""}, "unresolved", ""))
        elif roll < 0.94:  # invalid records
            row = make_row(rng, p, source_id(n), ts())
            kind = rng.choice(["email", "phone", "ts", "system", "id", "sysmismatch"])
            if kind == "email":
                row["email"] = "not-an-email"
            elif kind == "phone":
                row["phone"] = "12ab"
            elif kind == "ts":
                row["event_ts"] = "not-a-timestamp"
            elif kind == "system":
                row["source_system"] = "legacy_erp"
            elif kind == "id":
                row["source_id"] = ""
            else:
                row["source_id"] = row["source_id"].replace(row["source_system"], "crm" if row["source_system"] != "crm" else "web")
            items.append((row, "rejected", ""))
        else:  # exact duplicate of an earlier item; the copy that appears later in the shuffled file is the duplicate
            if items:
                row, outcome, cid = rng.choice(items)
                items.append((dict(row), outcome, cid))
            else:
                items.append((make_row(rng, p, source_id(n), ts()), "mapped", cid))

    rng.shuffle(items)  # out-of-order arrival
    seen, rows_out, labels = set(), [], []
    for number, (row, outcome, cid) in enumerate(items, start=1):
        key = tuple(row[c].strip() for c in ("source_system", "source_id", "event_ts", "email", "phone", "full_name"))
        if outcome == "rejected":  # validation runs before duplicate detection
            final = "rejected"
        elif key in seen:
            final = "duplicate"
        else:
            seen.add(key)
            final = outcome
        rows_out.append(row)
        labels.append({"source_row_number": number, "expected_outcome": final,
                       "expected_canonical_id": cid if final == "mapped" else ""})
    return rows_out, labels


def write_outputs(out: Path, rows, labels, data_name="dataset_large.csv"):
    out.mkdir(parents=True, exist_ok=True)
    with (out / data_name).open("w", newline="", encoding="utf-8") as h:
        w = csv.DictWriter(h, fieldnames=["source_system", "source_id", "event_ts", "email", "phone", "full_name"])
        w.writeheader()
        w.writerows(rows)
    with (out / "expected_labels.csv").open("w", newline="", encoding="utf-8") as h:
        w = csv.DictWriter(h, fieldnames=["source_file", "source_row_number", "expected_outcome",
                                          "expected_canonical_id"])
        w.writeheader()
        for lab in labels:
            w.writerow({"source_file": data_name, **lab})

    owners = {}
    for row, lab in zip(rows, labels):
        if lab["expected_outcome"] == "mapped":
            owners.setdefault((row["source_system"], row["source_id"].strip()), set()).add(lab["expected_canonical_id"])
    with (out / "expected_conflicts.csv").open("w", newline="", encoding="utf-8") as h:
        w = csv.writer(h)
        w.writerow(["source_system", "source_id"])
        for key in sorted(k for k, v in owners.items() if len(v) > 1):
            w.writerow(key)
    return sum(1 for v in owners.values() if len(v) > 1)


def write_reference(path: Path, customers: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as h:
        w = csv.writer(h)
        w.writerow(["email", "phone", "full_name"])
        for n in range(customers):
            p = person(n)
            w.writerow([p["email"], p["phone"], p["full_name"]])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--customers", type=int, default=20_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/large")
    ap.add_argument("--reference-only", action="store_true", help="only write canonical_customers.csv")
    args = ap.parse_args()

    out = Path(args.out)
    write_reference(out / "canonical_customers.csv", args.customers)
    if args.reference_only:
        print(f"wrote {args.customers} customers to {out / 'canonical_customers.csv'}")
        return
    rows, labels = generate(args.rows, args.customers, args.seed)
    n_conflicts = write_outputs(out, rows, labels)
    print(f"wrote {len(rows)} rows, {args.customers} customers, {n_conflicts} expected conflicts to {out}")


if __name__ == "__main__":
    main()
