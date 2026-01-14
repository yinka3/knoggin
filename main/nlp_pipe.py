import re
import torch
from main.prompts import ner_reasoning_prompt
from main.service import LLMService
from schema.dtypes import *
from typing import List, Tuple
from loguru import logger
from transformers import pipeline


class EntityItem(BaseModel):
    name: str = Field(..., description="The exact text span of the Named Entity.")
    label: str = Field(..., description="A concise, lowercase semantic type describing what the entity IS.")
    topic: str = Field(..., description="The most relevant topic from the user's active topics list.")

class ExtractionResponse(BaseModel):
    entities: List[EntityItem] = Field(..., description="A list of valid Named Entities extracted from the text. Return an empty list if no specific proper nouns are found.")

class NLPPipeline:
    
    def __init__(
        self,
        llm: LLMService,
        topics_config: dict,
        emotion_model: str = "j-hartmann/emotion-english-distilroberta-base",
        device: Optional[str] = None
    ):
        self.device = torch.device(device or ("cuda" if torch.cuda.is_available() else "cpu"))
        self.llm_client = llm
        self.topics_config = topics_config
        self._label_block = self._build_label_block(topics_config)
        self._init_emotion(emotion_model)
        
    def _init_emotion(self, model_name: str):
        device_id = 0 if torch.cuda.is_available() else -1
        self.emotion_classifier = pipeline(
            "text-classification",
            model=model_name,
            top_k=None,
            device=device_id
        )
    
    def _build_label_block(self, topics_config: dict) -> str:
        lines = []
        for topic, config in topics_config.items():
            labels = config.get("labels", [])
            lines.append(f"  - **{topic}**: {', '.join(labels)}")
        return "\n".join(lines)
    
    def _parse_entities(self, reasoning: str) -> Optional[ExtractionResponse]:
        """Parse <entities> block and validate via Pydantic."""
        match = re.search(r"<entities>(.*?)</entities>", reasoning, re.DOTALL)
        if not match:
            return None
        
        entities = []
        for line in match.group(1).strip().split("\n"):
            parts = line.split("|")
            if len(parts) != 3:
                continue
            name, label, topic = [p.strip() for p in parts]
            if name:
                entities.append(EntityItem(name=name, label=label, topic=topic))
        
        if not entities:
            return None
        
        return ExtractionResponse(entities=entities)
    
    async def extract_mentions(self, user_name: str, text: str) -> List[Tuple[str, str, str]]:
        """
        Extracts entities and noun phrases.
        """
        if not text or not text.strip():
            return []

        system_01 = ner_reasoning_prompt(user_name, self._label_block)
        reasoning = await self.llm_client.call_reasoning(system_01, text)
        
        if not reasoning or "<entities>" not in reasoning:
            logger.warning("VEGAPUNK-01 returned no entities block")
            return []

        response = self._parse_entities(reasoning)
    
        if not response:
            logger.warning("No entities parsed from VEGAPUNK-01")
            return []

        return [(e.name, e.label, e.topic) for e in response.entities]

    
    def analyze_emotion(self, text: str) -> List[dict]:
        if not text or not text.strip():
            return []
        try:
            results = self.emotion_classifier(text, truncation=True)
            return results[0] if results else []
        except Exception:
            return []