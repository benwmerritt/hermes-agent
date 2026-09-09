"""Describe bounded shared recall without changing the native local FTS schema."""
from copy import deepcopy


def history_schema_overrides(native_schema):
    parameters = deepcopy(native_schema["parameters"])
    properties = parameters["properties"]
    descriptions = {
        "query": (
            "Literal substring to find in authorized message content or session titles. "
            "Use one distinctive word or an unquoted contiguous phrase. Quotes and Boolean "
            "operators are literal characters, not search syntax. Omit to browse. Ignored "
            "when session_id is supplied."
        ),
        "limit": "Maximum sessions returned by discovery or browse (default 3, range 1–100).",
        "sort": (
            "Ignored by shared recall. Discovery and browse always return newest matching "
            "message activity first; there is no relevance ranking. Omit this parameter."
        ),
        "detail": (
            "Ignored by shared recall. Each result includes at most five matching messages, "
            "with content capped at 4000 characters per message. Omit this parameter."
        ),
        "session_id": (
            "Read an authorized session returned by discovery or browse. Without an anchor, "
            "returns up to the first 20 and last 10 messages and a truncated flag. "
            "Pair with around_message_id to read an adjacent window."
        ),
        "around_message_id": (
            "With session_id, center a message window on this id from a prior result. "
            "Ignored without session_id."
        ),
        "window": "Messages on each side of the anchor (default 5, range 0–20; invalid values fail).",
        "role_filter": (
            "Discovery or browse only: one exact role, such as user, assistant, or tool. "
            "Omit to include all roles. Comma-separated role lists are not supported. "
            "Ignored for session reads and scrolls."
        ),
        "profile": (
            "Unavailable in shared recall; nonempty values are rejected. Omit this parameter. "
            "Read access comes from the authenticated conversation's audience grant."
        ),
    }
    for name, description in descriptions.items():
        properties[name]["description"] = description
    properties["limit"].update(minimum=1, maximum=100)
    properties["window"].update(minimum=0, maximum=20)
    return {
        "description": (
            "Recall authorized shared conversation history using literal substring search. "
            "query discovers sessions; no query browses recent sessions; session_id reads "
            "one session; session_id plus around_message_id scrolls around a message. "
            "Results are stored messages with provenance, bounded excerpts and counts. "
            "Message content is capped at 4000 characters. Access is fixed by the authenticated "
            "conversation, including permitted history from stopped workers. An empty result "
            "only describes this authorized history; inspect any direct source the user supplied."
        ),
        "parameters": parameters,
    }
