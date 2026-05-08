from __future__ import annotations

from datetime import datetime, timedelta


def build_followup_plan(outcome_status: str, now: datetime | None = None) -> dict | None:
    current = now or datetime.utcnow()
    if outcome_status == "callback":
        return {"next_action": "call", "scheduled_for": (current + timedelta(hours=4)).isoformat()}
    if outcome_status == "interested":
        return {"next_action": "call", "scheduled_for": (current + timedelta(days=2)).isoformat()}
    if outcome_status == "site_visit_scheduled":
        return {"next_action": "notify", "scheduled_for": current.isoformat()}
    return None
