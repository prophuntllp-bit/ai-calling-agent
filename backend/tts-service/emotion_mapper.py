def map_emotion(reply_text: str, context: dict | None = None) -> str:
    context = context or {}
    stage = context.get("stage", "")
    status = context.get("lead_status", "")
    text = reply_text.lower()
    if stage == "opening":
        return "warm"
    if any(word in text for word in ["amenity", "feature", "benefit", "launch", "offer"]):
        return "excited"
    if any(word in text for word in ["price", "budget", "concern", "problem", "issue"]):
        return "empathetic"
    if any(word in text for word in ["visit", "schedule", "book", "meeting"]) or status == "site_visit":
        return "professional"
    return "neutral"
