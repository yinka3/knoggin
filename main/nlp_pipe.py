import torch
from main.prompts import ner_reasoning_prompt
from main.service import LLMService
from main.topics_config import TopicConfig
from main.utils import format_vp01_input, is_covered, is_generic_phrase, parse_entities, validate_entity
from schema.dtypes import *
from typing import Callable, Dict, List, Optional, Tuple
from loguru import logger
from transformers import pipeline
import spacy
from spacy.matcher import PhraseMatcher
from gliner import GLiNER


class NLPPipeline:

    PRONOUNS = {
        "my", "his", "her", "their", "our", "your", "its",
        "he", "she", "they", "we", "i", "me", "him", "them", "us",
        "this", "that", "these", "those"
    }
    
    def __init__(
        self,
        llm: LLMService,
        topic_config: TopicConfig,
        get_known_aliases: Callable[[], Dict[str, int]],
        get_profiles: Callable[[], Dict[int, dict]],
        emotion_model: str = "j-hartmann/emotion-english-distilroberta-base",
        device: Optional[str] = None
    ):
        self.device = torch.device(device or ("cuda" if torch.cuda.is_available() else "cpu"))
        self.llm_client = llm
        self.topic_config = topic_config
        self.get_known_aliases = get_known_aliases
        self.get_profiles = get_profiles
        
        # build inverse label map: label -> [topics]
        self._label_to_topics = self._build_label_to_topics()
        
        self._nlp = self._load_spacy()
        self._gliner = self._load_gliner()
        self._init_emotion(emotion_model)
        
        logger.info(f"NLPPipeline initialized | device={self.device} | spacy={self._nlp.meta['name']}")
       
    
    def _load_spacy(self) -> spacy.Language:
        exclude = ["ner", "lemmatizer", "attribute_ruler"]
        nlp = spacy.load("en_core_web_lg", exclude=exclude)
        nlp.add_pipe("doc_cleaner")
        logger.info("Loaded en_core_web_lg (CPU)")
        return nlp
    
    def _load_gliner(self) -> GLiNER:
        model = GLiNER.from_pretrained("urchade/gliner_large-v2.1")
        model.to(self.device)
        logger.info("Loaded GLiNER large-v2.1")
        return model
        
    def _init_emotion(self, model_name: str):
        device_id = 0 if torch.cuda.is_available() else -1
        self.emotion_classifier = pipeline(
            "text-classification",
            model=model_name,
            top_k=None,
            device=device_id
        )
    
    def _build_label_to_topics(self) -> Dict[str, List[str]]:
        """Invert topic_config: label -> [topics that include it]"""
        label_to_topics = {}
        
        for topic, config in self.topic_config.raw.items():
            for label in config.get("labels", []):
                label_lower = label.lower()
                if label_lower not in label_to_topics:
                    label_to_topics[label_lower] = []
                label_to_topics[label_lower].append(topic)
        
        logger.debug(f"Built label to topics map: {label_to_topics}")
        return label_to_topics

    def _build_phrase_matcher(self) -> Tuple[PhraseMatcher, Dict[str, int]]:
        """Build PhraseMatcher from current known aliases."""
        aliases = self.get_known_aliases()
        matcher = PhraseMatcher(self._nlp.vocab, attr="LOWER")
        
        if aliases:
            patterns = [self._nlp.make_doc(alias) for alias in aliases.keys()]
            matcher.add("KNOWN", patterns)
        
        return matcher, aliases
    

    
    def run_gliner(self, text: str, threshold: float = 0.85) -> List[Tuple[str, str, int, int]]:
        all_labels = list(self._label_to_topics.keys())
        if not all_labels:
            return []
        
        entities = self._gliner.predict_entities(text, all_labels, threshold=threshold)
        
        filtered = []
        for e in entities:
            span = e["text"]
            score = e.get("score", 0)
            
            logger.debug(f"GLiNER: '{span}' | label={e['label']} | score={score:.3f}")
            
            if span.lower() in self.PRONOUNS or span.split()[0].lower() in self.PRONOUNS:
                logger.debug(f"  -> Filtered (pronoun)")
                continue
            
            # for frequent names like chris
            if e["label"] == "person":
                filtered.append(e)
                continue

            if is_generic_phrase(span):
                logger.debug(f"{span}  -> Filtered (generic word)")
                continue
            
            filtered.append(e)
        
        return [(e["text"], e["label"], e["start"], e["end"]) for e in filtered]
    
    def _assign_topic(self, label: str) -> Tuple[Optional[str], bool]:
        """
        Assign topic from label.
        Returns: (topic or None, is_ambiguous)
        """
        label_lower = label.lower()
        topics = self._label_to_topics.get(label_lower, [])
        
        if len(topics) == 1:
            return topics[0], False
        elif len(topics) > 1:
            return None, True
        else:
            return "General", False
    
    
    async def extract_mentions(self, user_name: str, messages: List[Dict]) -> List[Tuple[str, str, str]]:
        """
        Extracts entities via PhraseMatcher (known) + GLiNER (labeled) + VP-01 (catch-all).
        Returns: List[(name, type, topic)]
        """
        if not messages:
            return []
        
        text = "\n".join([f"[MSG {m['id']}]: {m['message']}" for m in messages])
        matcher, aliases = self._build_phrase_matcher()
        doc = self._nlp(text)
        
        known_ents: List[Tuple[str, int]] = []
        for _, start, end in matcher(doc):
            span_text = doc[start:end].text
            eid = aliases.get(span_text.lower())
            if eid:
                known_ents.append((span_text, eid))
        
        msg_only = " ".join([m["message"] for m in messages])
        gliner_raw = self.run_gliner(msg_only)
        gliner_ents: List[Tuple[str, str]] = [(span, label) for span, label, _, _ in gliner_raw]
        
        covered_texts: set[str] = set()
        resolved: List[Tuple[str, str, str, int | None]] = []  # (text, label, topic, eid)
        ambiguous: List[Tuple[str, str, List[str]]] = []  # (text, label, candidate_topics)
        
        # process known entities first (highest priority)
        for span_text, eid in known_ents:
            profile = self.get_profiles().get(eid, {})
            covered_texts.add(span_text.lower())
            resolved.append((
                span_text,
                profile.get("type", "unknown"),
                profile.get("topic", "General"),
                eid
            ))
        
        gliner_filtered = set()
        # process GLiNER entities second
        for span_text, label in gliner_ents:
            if is_covered(span_text, covered_texts):
                continue

            if not validate_entity(span_text, "General", self.topic_config):
                logger.debug(f"Filtered invalid GLiNER entity: '{span_text}'")
                continue
            
            covered_texts.add(span_text.lower())
            topic, is_ambiguous = self._assign_topic(label)
            
            if is_ambiguous:
                topics = self._label_to_topics.get(label.lower(), [])
                ambiguous.append((span_text, label, topics))
            else:
                resolved.append((span_text, label, topic, None))
        
        output: List[Tuple[str, str, str]] = []
        
        # add already-resolved to output
        for span_text, label, topic, _ in resolved:
            output.append((span_text, label, topic))
        
        user_content = format_vp01_input(messages, known_ents, gliner_ents, ambiguous, covered_texts)
        
        system_prompt = ner_reasoning_prompt(user_name, self.topic_config.label_block)
        reasoning = await self.llm_client.call_llm(system_prompt, user_content)
        
        vp01_count = 0
        if reasoning and "<entities>" in reasoning:
            response = parse_entities(reasoning, min_confidence=0.8)
            if response:
                for entity in response.entities:
                    if validate_entity(entity.name, entity.topic, self.topic_config):
                        if entity.name.lower() in gliner_filtered:
                            logger.info(f"VP-01 recovered GLiNER-filtered entity: '{entity.name}'")
                        output.append((entity.name, entity.label, entity.topic))
                        vp01_count += 1
                    else:
                        logger.debug(f"Filtered invalid VP-01 entity: '{entity.name}'")
            else:            
                logger.warning("VP-01 returned no entities block")
        
        logger.info(
            f"Extracted {len(output)} mentions: "
            f"{len(known_ents)} known, {len(gliner_ents)} gliner, "
            f"{vp01_count} from VP-01"
        )
        
        return output


    def analyze_emotion(self, text: str) -> List[dict]:
        if not text or not text.strip():
            return []
        try:
            results = self.emotion_classifier(text, truncation=True)
            return results[0] if results else []
        except Exception:
            return []