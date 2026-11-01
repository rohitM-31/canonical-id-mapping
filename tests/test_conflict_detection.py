from src.matching.conflict_detection import build_history, detect_one_to_many


def ev(ts, eid, cid):
    return (f"2025-01-{ts:02d}T00:00:00Z", eid, cid)


def test_single_canonical_collapses_to_one_open_interval():
    h = build_history([ev(1, "a", "X"), ev(5, "b", "X")])
    assert h == [{"canonical_id": "X", "valid_from": "2025-01-01T00:00:00Z", "valid_to": None,
                  "event_count": 2, "is_current": 1}]


def test_change_closes_previous_interval():
    h = build_history([ev(1, "a", "X"), ev(5, "b", "Y")])
    assert [(i["canonical_id"], i["valid_to"], i["is_current"]) for i in h] == [
        ("X", "2025-01-05T00:00:00Z", 0), ("Y", None, 1)]


def test_result_is_independent_of_arrival_order():
    events = [ev(1, "a", "X"), ev(3, "b", "Y"), ev(5, "c", "X")]
    assert build_history(events) == build_history(list(reversed(events)))
    assert [i["canonical_id"] for i in build_history(events)] == ["X", "Y", "X"]  # A -> B -> A keeps 3 rows


def test_one_to_many_detection():
    assert detect_one_to_many([ev(1, "a", "X"), ev(2, "b", "X")]) is None
    c = detect_one_to_many([ev(1, "a", "Y"), ev(9, "b", "X")])
    assert c["canonical_count"] == 2 and c["canonical_ids"] == "X,Y"
    assert (c["first_seen"], c["last_seen"]) == ("2025-01-01T00:00:00Z", "2025-01-09T00:00:00Z")
