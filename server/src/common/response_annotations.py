"""How selected excerpts of earlier answers are shown to the agent."""


def with_annotations(text: str, annotations: list[dict] | None) -> str:
    """The user's text followed by the response excerpts it refers to."""

    if not annotations:
        return text
    quoted = "\n".join(
        f"Assistant message {item['message_id']}, selected text: {item['quote']}"
        for item in annotations
    )
    return f"{text}\n\n**Response excerpts I am referring to:**\n{quoted}"
