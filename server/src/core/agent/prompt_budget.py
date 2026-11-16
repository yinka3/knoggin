"""Deterministic prompt-memory allocation for one admitted run."""

from collections.abc import Callable, Mapping

from common.schema.agent.settings import PromptMemoryBudget


def fit_to_context(
    budget: PromptMemoryBudget, context_window: int | None
) -> PromptMemoryBudget:
    """Scale the budget to the model, keeping the configured proportions."""
    if not budget.fit_to_model or not context_window:
        return budget
    input_tokens = max(1_024, min(
        int(context_window * budget.input_share_of_context),
        budget.max_fitted_input_tokens,
    ))
    if input_tokens == budget.input_tokens:
        return budget
    scale = input_tokens / budget.input_tokens
    shares = {
        f"{key}_tokens": int(getattr(budget, f"{key}_tokens") * scale)
        for key in budget.spare_priority
    }
    return budget.model_copy(update={"input_tokens": input_tokens, **shares})


class PromptBudgetExceeded(ValueError):
    """The fixed prompt alone cannot fit the configured input budget."""


def token_count(text: str, counter: Callable[[str], int] | None) -> int:
    """Use the configured tokenizer, or the documented four-character estimate."""
    if not text:
        return 0
    if counter is not None:
        try:
            value = counter(text)
            if isinstance(value, int) and value >= 0:
                return value
        except Exception:
            pass
    return (len(text) + 3) // 4


_OMITTED_LATER = "\n[…later content omitted to fit the prompt budget]"
_OMITTED_EARLIER = "[…earlier content omitted to fit the prompt budget]\n"


def trim_to_tokens(
    text: str, limit: int, counter: Callable[[str], int] | None, *, tail: bool = False
) -> str:
    """Fit text to a token limit and say so, so the model never mistakes a cut
    Context or notebook for the complete record."""

    if limit <= 0:
        return ""
    if token_count(text, counter) <= limit:
        return text
    marker = _OMITTED_EARLIER if tail else _OMITTED_LATER
    text = text.removeprefix(marker) if tail else text.removesuffix(marker)
    room = limit - token_count(marker, counter)
    if room <= 0:
        return _cut_to_tokens(text, limit, counter, tail=tail)
    kept = _cut_to_tokens(text, room, counter, tail=tail)
    if not kept:
        return _cut_to_tokens(text, limit, counter, tail=tail)
    return f"{marker}{kept}" if tail else f"{kept}{marker}"


def _cut_to_tokens(
    text: str, limit: int, counter: Callable[[str], int] | None, *, tail: bool
) -> str:
    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        candidate = text[-middle:] if tail else text[:middle]
        if token_count(candidate, counter) <= limit:
            low = middle
        else:
            high = middle - 1
    return (text[-low:] if tail else text[:low]) if low else ""


def allocate_memory(
    parts: Mapping[str, str],
    budget: PromptMemoryBudget,
    available_tokens: int,
    counter: Callable[[str], int] | None,
) -> dict[str, str]:
    """Give categories their starting shares, then lend spare room by priority."""
    if available_tokens < 0:
        raise PromptBudgetExceeded("Fixed prompt exceeds configured input budget")
    categories = tuple(budget.spare_priority)
    actual = {key: token_count(parts.get(key, ""), counter) for key in categories}
    starting = {
        key: min(actual[key], getattr(budget, f"{key}_tokens")) for key in categories
    }
    total_starting = sum(starting.values())
    if total_starting <= available_tokens:
        granted = dict(starting)
    elif total_starting:
        granted = {
            key: min(starting[key], int(starting[key] * available_tokens / total_starting))
            for key in categories
        }
        # Preserve a visible slice of every configured, nonempty category when
        # at least one token per category is available.
        positive = [key for key in categories if starting[key] > 0]
        if available_tokens >= len(positive):
            for key in positive:
                granted[key] = max(1, granted[key])
        while sum(granted.values()) > available_tokens:
            for key in reversed(categories):
                if granted[key] > 1:
                    granted[key] -= 1
                    break
        for key in categories:
            if sum(granted.values()) >= available_tokens:
                break
            granted[key] += min(
                starting[key] - granted[key], available_tokens - sum(granted.values())
            )
    else:
        granted = {key: 0 for key in categories}
    spare = available_tokens - sum(granted.values())
    for key in categories:
        more = min(actual[key] - granted[key], spare)
        granted[key] += more
        spare -= more
    return {
        key: trim_to_tokens(parts.get(key, ""), granted[key], counter,
                            tail=key == "conversation")
        for key in categories
    }
