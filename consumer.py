import json
from kafka import KafkaConsumer # type: ignore
from common import *

c = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP, group_id="analytics-v1",
                  auto_offset_reset="earliest", enable_auto_commit=False,
                  value_deserializer=lambda b: json.loads(b.decode()),
                  consumer_timeout_ms=8000)
good, bad = [], []
for msg in c:
    ok, why = validate(msg.value)
    (good if ok else bad).append(msg.value if ok else (msg.value, why))
    c.commit()                      # commit only after the event was handled
print(f"valid={len(good)} invalid={len(bad)}")
for ev, why in bad:
    print("REJECTED", ev, why)

print("\nBy job_type:")
for k, v in by_job_type(good).items():
    print(f'  {k:11s} completed={v["completed"]} failed={v["failed"]} avg={v["avg_duration"]:.2f}')
print("\nAvg CPU by group:")
for k, v in cpu_by_group(good).items():
    print(f"  {k}: {v:.2f}")
print("\n5-second event-time windows:")
for w, r in windows(good).items():
    print(f'  {w.time()}  n={r["count"]} ids={r["ids"]} fail={r["failure_rate"]:.0%} cpu={r["avg_cpu"]:.2f}')
