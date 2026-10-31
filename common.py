"""Shared schema, sample data and pure-Python analytics (no Kafka needed)."""
from collections import defaultdict
from datetime import datetime

TOPIC = "research-events"
BOOTSTRAP = "localhost:9092"
DATE = "2026-10-31"          # synthetic date prepended to the HH:MM:SS event times
WINDOW_SEC = 5

SCHEMA = {"event_id": str, "timestamp": str, "research_group": str,
          "job_type": str, "duration_sec": (int, float),
          "status": str, "cpu_pct": (int, float)}
JOB_TYPES = {"simulation", "ml", "image", "text"}
STATUSES = {"completed", "failed"}

_RAW = [
    ("E001", "14:00:01", "G01", "ml", 42, "completed", 61),
    ("E002", "14:00:02", "G02", "image", 75, "completed", 74),
    ("E003", "14:00:03", "G01", "text", 18, "completed", 38),
    ("E004", "14:00:04", "G03", "simulation", 95, "failed", 92),
    ("E005", "14:00:05", "G02", "ml", 51, "completed", 68),
    ("E006", "14:00:06", "G01", "image", 88, "completed", 81),
    ("E007", "14:00:07", "G03", "ml", 64, "failed", 89),
    ("E008", "14:00:08", "G02", "text", 22, "completed", 44),
    ("E009", "14:00:09", "G01", "simulation", 120, "completed", 94),
    ("E010", "14:00:10", "G03", "image", 70, "completed", 78),
]
KEYS = list(SCHEMA)
EVENTS = [dict(zip(KEYS, (r[0], f"{DATE}T{r[1]}", *r[2:]))) for r in _RAW]


def validate(ev):
    """Return (ok, reason)."""
    for k, t in SCHEMA.items():
        if k not in ev:
            return False, f"missing field {k}"
        if not isinstance(ev[k], t):
            return False, f"bad type for {k}"
    if ev["job_type"] not in JOB_TYPES:
        return False, "bad job_type"
    if ev["status"] not in STATUSES:
        return False, "bad status"
    if not 0 <= ev["cpu_pct"] <= 100 or ev["duration_sec"] < 0:
        return False, "value out of range"
    try:
        datetime.fromisoformat(ev["timestamp"])
    except ValueError:
        return False, "bad timestamp"
    return True, ""


def by_job_type(events):
    d = defaultdict(lambda: {"completed": 0, "failed": 0, "dur": []})
    for e in events:
        d[e["job_type"]][e["status"]] += 1
        d[e["job_type"]]["dur"].append(e["duration_sec"])
    return {k: {"completed": v["completed"], "failed": v["failed"],
                "avg_duration": sum(v["dur"]) / len(v["dur"])} for k, v in d.items()}


def cpu_by_group(events):
    d = defaultdict(list)
    for e in events:
        d[e["research_group"]].append(e["cpu_pct"])
    return {k: sum(v) / len(v) for k, v in sorted(d.items())}


def window_start(ts, size=WINDOW_SEC):
    t = datetime.fromisoformat(ts)
    sec = t.second - t.second % size
    return t.replace(second=sec, microsecond=0)


def windows(events, size=WINDOW_SEC):
    d = defaultdict(list)
    for e in events:
        d[window_start(e["timestamp"], size)].append(e)
    out = {}
    for w, evs in sorted(d.items()):
        out[w] = {"ids": [e["event_id"] for e in evs], "count": len(evs),
                  "failure_rate": sum(e["status"] == "failed" for e in evs) / len(evs),
                  "avg_cpu": sum(e["cpu_pct"] for e in evs) / len(evs)}
    return out


def windows_by_job_type(events, size=WINDOW_SEC):
    d = defaultdict(list)
    for e in events:
        d[(window_start(e["timestamp"], size), e["job_type"])].append(e["duration_sec"])
    return {k: (len(v), sum(v) / len(v)) for k, v in sorted(d.items())}


if __name__ == "__main__":
    print(by_job_type(EVENTS))
    print(cpu_by_group(EVENTS))
    for w, r in windows(EVENTS).items():
        print(w.time(), r)
    for k, v in windows_by_job_type(EVENTS).items():
        print(k[0].time(), k[1], v)
