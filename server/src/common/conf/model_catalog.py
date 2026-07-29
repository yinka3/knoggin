"""Static model metadata used for product-facing model selection."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelCatalogEntry:
    """One supported model and its USD price per million tokens."""

    id: str
    name: str
    input_price: float
    output_price: float


CURATED_MODEL_CATALOG: tuple[ModelCatalogEntry, ...] = (
    ModelCatalogEntry(
        id="anthropic/claude-sonnet-4.5",
        name="Claude Sonnet 4.5",
        input_price=3.00,
        output_price=15.00,
    ),
    ModelCatalogEntry(
        id="anthropic/claude-opus-4.5",
        name="Claude Opus 4.5",
        input_price=5.00,
        output_price=25.00,
    ),
    ModelCatalogEntry(
        id="x-ai/grok-4.1-fast",
        name="Grok 4.1 Fast",
        input_price=0.20,
        output_price=0.50,
    ),
    ModelCatalogEntry(
        id="openai/gpt-5.1",
        name="GPT-5.1",
        input_price=1.25,
        output_price=10.00,
    ),
    ModelCatalogEntry(
        id="google/gemini-3-pro-preview",
        name="Gemini 3 Pro",
        input_price=2.00,
        output_price=12.00,
    ),
    ModelCatalogEntry(
        id="anthropic/claude-haiku-4.5",
        name="Claude Haiku 4.5",
        input_price=1.00,
        output_price=5.00,
    ),
    ModelCatalogEntry(
        id="google/gemini-2.5-flash-lite-preview-09-2025",
        name="Gemini 2.5 Flash Lite",
        input_price=0.10,
        output_price=0.40,
    ),
    ModelCatalogEntry(
        id="google/gemini-2.5-flash",
        name="Gemini 2.5 Flash",
        input_price=0.30,
        output_price=2.50,
    ),
    ModelCatalogEntry(
        id="deepseek/deepseek-v3.1",
        name="DeepSeek V3.1",
        input_price=0.60,
        output_price=1.70,
    ),
    ModelCatalogEntry(
        id="openai/gpt-oss-120b:free",
        name="GPT-OSS-120B",
        input_price=0.0,
        output_price=0.0,
    ),
)
