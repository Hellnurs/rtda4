import json, time, sys
import kafka # type: ignore
from common import EVENTS, TOPIC, BOOTSTRAP

p = kafka.KafkaProducer(bootstrap_servers=BOOTSTRAP,
                  value_serializer=lambda v: json.dumps(v).encode(),
                  key_serializer=str.encode,
                  acks="all", linger_ms=5)
delay = float(sys.argv[1]) if len(sys.argv) > 1 else 0.0
for ev in EVENTS:
    # key = research_group -> events of one group stay ordered in one partition
    meta = p.send(TOPIC, key=ev["research_group"], value=ev).get(timeout=10)
    print(f'sent {ev["event_id"]} -> partition {meta.partition} offset {meta.offset}')
    time.sleep(delay)
p.flush()
