"""Measure throughput/latency/CPU at several input rates (end-to-end, real Kafka)."""
import json, time, threading, statistics, sys, random
import psutil
from kafka import KafkaProducer, KafkaConsumer
from common import *

RATES = [10, 100, 500]
DURATION = 20  # seconds per level

def run(rate):
    topic = TOPIC
    lat, done = [], threading.Event()
    cons = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, group_id=f"load-{rate}-{time.time()}",
                         auto_offset_reset="latest",
                         value_deserializer=lambda b: json.loads(b.decode()))
    cons.poll(1000)  # join group before producing
    def consume():
        for m in cons:
            if m.value.get("load") != rate: continue
            ev = m.value
            ok, _ = validate(ev)
            if ok: lat.append((time.time() - ev["sent_at"]) * 1000)
            if done.is_set() and len(lat) >= total_sent[0]: break
    total_sent = [0]
    t = threading.Thread(target=consume, daemon=True); t.start()
    prod = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                         value_serializer=lambda v: json.dumps(v).encode(),
                         key_serializer=str.encode, linger_ms=5)
    psutil.cpu_percent(None)
    start = time.time(); n = 0
    while time.time() - start < DURATION:
        base = random.choice(EVENTS)
        ev = {**base, "event_id": f"L{rate}-{n}", "load": rate, "sent_at": time.time()}
        prod.send(topic, key=ev["research_group"], value=ev); n += 1
        nxt = start + n / rate
        time.sleep(max(0, nxt - time.time()))
    prod.flush(); total_sent[0] = n; done.set()
    t.join(timeout=30)
    elapsed = time.time() - start
    cpu = psutil.cpu_percent(None)
    return {"rate": rate, "sent": n, "received": len(lat),
            "throughput": len(lat) / elapsed,
            "avg_latency_ms": statistics.mean(lat) if lat else float("nan"),
            "p95_ms": statistics.quantiles(lat, n=20)[-1] if len(lat) > 20 else float("nan"),
            "cpu_pct": cpu}

if __name__ == "__main__":
    for r in RATES:
        print(run(r))
