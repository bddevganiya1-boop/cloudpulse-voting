"""
create_indices.py — runs once at startup inside the es-init container.
Initializes the three Elasticsearch indices required for the CloudPulse Voting pipeline:
1. votes_raw        - Full audit trail of every vote event
2. votes_windowed   - 30-second windowed aggregated voting rate
3. vote_totals      - Live leaderboard running totals per option
"""

import time
import os
from elasticsearch import Elasticsearch

ES_HOST = os.environ.get("ES_HOST", "elasticsearch:9200")

INDEX_MAPPINGS = {
    "votes_raw": {
        "settings": {
            "index": {
                "refresh_interval": "500ms"
            }
        },
        "mappings": {
            "properties": {
                "voter_id": {"type": "keyword"},
                "vote": {"type": "keyword"},
                "event_timestamp": {"type": "date"},
                "is_change": {"type": "boolean"},
            }
        }
    },
    "votes_windowed": {
        "settings": {
            "index": {
                "refresh_interval": "500ms"
            }
        },
        "mappings": {
            "properties": {
                "vote": {"type": "keyword"},
                "vote_count": {"type": "integer"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
            }
        }
    },
    "vote_totals": {
        "settings": {
            "index": {
                "refresh_interval": "500ms"
            }
        },
        "mappings": {
            "properties": {
                "option": {"type": "keyword"},
                "count": {"type": "integer"},
                "last_updated": {"type": "date"},
            }
        }
    },
}


def wait_for_elasticsearch(es: Elasticsearch, max_retries: int = 30, interval: int = 2):
    print(f"Connecting to Elasticsearch at http://{ES_HOST}...")
    for attempt in range(1, max_retries + 1):
        try:
            if es.ping():
                print(f"Connected to Elasticsearch on attempt {attempt}.")
                return
        except Exception as exc:
            print(f"Waiting for ES ({exc})...")
        time.sleep(interval)
    raise RuntimeError("Elasticsearch did not become reachable in time.")


def main():
    es = Elasticsearch(f"http://{ES_HOST}")
    wait_for_elasticsearch(es)

    for index_name, body in INDEX_MAPPINGS.items():
        if es.indices.exists(index=index_name):
            print(f"[skip] index '{index_name}' already exists")
            try:
                es.indices.put_settings(index=index_name, settings={"index": {"refresh_interval": "500ms"}})
            except Exception:
                pass
            continue
        try:
            es.indices.create(index=index_name, mappings=body.get("mappings"), settings=body.get("settings"))
        except TypeError:
            es.indices.create(index=index_name, body=body)
        print(f"[created] index '{index_name}' successfully configured")


if __name__ == "__main__":
    main()
