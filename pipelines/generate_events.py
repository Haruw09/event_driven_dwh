from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

INCOMING_DIR = Path("data/incoming")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Event:
    event_id: str
    event_time: str
    ingestion_time: str
    event_name: str
    user_id: int
    session_id: str
    product_id: int | None
    price: float | None
    device: str
    payload: dict[str, Any]

    def to_jsonl(self) -> str:
        return json.dumps(
            {
                "event_id": self.event_id,
                "event_time": self.event_time,
                "ingestion_time": self.ingestion_time,
                "event_name": self.event_name,
                "user_id": self.user_id,
                "session_id": self.session_id,
                "product_id": self.product_id,
                "price": self.price,
                "device": self.device,
                "payload": self.payload,
            },
            ensure_ascii=False,
        )


def iso(dt: datetime) -> str:
    # ISO 8601 with timezone, e.g. 2026-02-18T12:34:56.123456+00:00
    return dt.astimezone(timezone.utc).isoformat()


def weighted_choice(items: list[tuple[str, float]]) -> str:
    names = [x[0] for x in items]
    weights = [x[1] for x in items]
    return random.choices(names, weights=weights, k=1)[0]


def generate_session_events(
    user_id: int,
    base_ingestion_time: datetime,
    late_event_rate: float,
    duplicate_rate: float,
) -> list[Event]:
    """
    Funnel:
      open -> view -> (cart?) -> (purchase?)
    Not every session reaches the end.
    """
    session_id = str(uuid.uuid4())
    device = weighted_choice([("web", 0.55), ("ios", 0.25), ("android", 0.20)])
    product_id = random.randint(1, 5000)

    # Decide progression probabilities
    do_view = random.random() < 0.85
    do_cart = do_view and (random.random() < 0.30)
    do_purchase = do_cart and (random.random() < 0.55)

    event_names = ["open"]
    if do_view:
        event_names.append("view")
    if do_cart:
        event_names.append("cart")
    if do_purchase:
        event_names.append("purchase")

    # Session timeline: small deltas between events
    # event_time is based on ingestion_time, with possible "late" shift backwards
    events: list[Event] = []
    event_time = base_ingestion_time - timedelta(seconds=random.randint(0, 30))

    for i, name in enumerate(event_names):
        event_time = event_time + timedelta(seconds=random.randint(2, 40))

        # Late events: shift event_time into the past, but keep ingestion_time "now-ish"
        if random.random() < late_event_rate:
            event_time_late = event_time - timedelta(
                minutes=random.randint(10, 240),  # up to 4 hours late
                seconds=random.randint(0, 59),
            )
            effective_event_time = event_time_late
            is_late = True
        else:
            effective_event_time = event_time
            is_late = False

        if name == "purchase":
            price = round(random.uniform(5, 250), 2)
        else:
            price = None

        payload = {
            "source": "synthetic_generator",
            "is_late": is_late,
            "session_step": i + 1,
        }

        e = Event(
            event_id=str(uuid.uuid4()),
            event_time=iso(effective_event_time),
            ingestion_time=iso(base_ingestion_time),
            event_name=name,
            user_id=user_id,
            session_id=session_id,
            product_id=product_id if name in ("view", "cart", "purchase") else None,
            price=price,
            device=device,
            payload=payload,
        )
        events.append(e)

        # Sometimes create an exact duplicate (same event_id) to test dedup downstream
        if random.random() < duplicate_rate:
            events.append(e)

    return events


def generate_events(
    rows: int,
    users: int,
    late_event_rate: float,
    duplicate_rate: float,
) -> list[Event]:
    """
    Generate approximately `rows` events by creating sessions.
    """
    all_events: list[Event] = []
    while len(all_events) < rows:
        user_id = random.randint(1, users)
        ingestion_time = utc_now()

        session_events = generate_session_events(
            user_id=user_id,
            base_ingestion_time=ingestion_time,
            late_event_rate=late_event_rate,
            duplicate_rate=duplicate_rate,
        )
        all_events.extend(session_events)

    return all_events[:rows]


def write_jsonl(events: list[Event], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(e.to_jsonl() + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic event stream files (JSONL).")
    parser.add_argument("--rows", type=int, default=500, help="Number of events to generate in the file")
    parser.add_argument("--users", type=int, default=200, help="Number of distinct users")
    parser.add_argument("--late-rate", type=float, default=0.05, help="Share of late events (0..1)")
    parser.add_argument("--dup-rate", type=float, default=0.01, help="Share of duplicated events (0..1)")
    args = parser.parse_args()

    ts = utc_now().strftime("%Y%m%dT%H%M%SZ")
    out_file = INCOMING_DIR / f"events_{ts}_{args.rows}.jsonl"

    events = generate_events(
        rows=args.rows,
        users=args.users,
        late_event_rate=args.late_rate,
        duplicate_rate=args.dup_rate,
    )
    write_jsonl(events, out_file)

    print(f"✅ Generated {len(events)} events -> {out_file}")


if __name__ == "__main__":
    main()