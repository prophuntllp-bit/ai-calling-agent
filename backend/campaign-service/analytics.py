from __future__ import annotations

from collections import Counter

from models import CallLogRecord


def summarize_calls(call_logs: list[CallLogRecord]) -> dict:
    attempted = len(call_logs)
    connected = sum(1 for call in call_logs if call.status not in {"no_answer", "busy"})
    interested = sum(1 for call in call_logs if call.status == "interested")
    site_visits = sum(1 for call in call_logs if call.status == "site_visit")
    duration = sum(call.duration_sec for call in call_logs)
    objections = Counter(call.call_metadata.get("objection") for call in call_logs if call.call_metadata.get("objection"))
    languages = Counter(call.call_metadata.get("language") for call in call_logs if call.call_metadata.get("language"))
    time_slots = Counter(call.started_at.strftime("%H:00") for call in call_logs)
    return {
        "calls_attempted": attempted,
        "connected": connected,
        "interested": interested,
        "site_visits_booked": site_visits,
        "conversion_rate": round((site_visits / attempted) * 100, 2) if attempted else 0.0,
        "average_call_duration": round(duration / attempted, 2) if attempted else 0.0,
        "best_time_slots": time_slots.most_common(3),
        "language_distribution": dict(languages),
        "objection_frequency": dict(objections),
    }
