import asyncio
from main.prompts import ner_reasoning_prompt
from shared.service import LLMService
from shared.topics_config import TopicConfig
from main.utils import PRONOUNS, format_vp01_input, is_covered, is_generic_phrase, parse_entities, validate_entity
from typing import Callable, Dict, List, Optional, Tuple
from loguru import logger
import spacy
from spacy.matcher import PhraseMatcher
from gliner import GLiNER

from shared.events import emit


class NLPPipeline:

    def __init__(
        self,
        llm: LLMService,
        topic_config: TopicConfig,
        get_known_aliases: Callable[[], Dict[str, int]],
        get_profiles: Callable[[], Dict[int, dict]],
        gliner: GLiNER,
        spacy: spacy.Language,
        gliner_threshold: float = 0.85,
        vp01_min_confidence: float = 0.8
    ):
        self.llm_client = llm
        self.topic_config = topic_config
        self.get_known_aliases = get_known_aliases
        self.get_profiles = get_profiles
        self._label_to_topics = self._build_label_to_topics()
        self._nlp = spacy
        self._gliner = gliner
        self.gliner_threshold = gliner_threshold
        self.vp01_min_confidence = vp01_min_confidence

    def update_settings(self, gliner_threshold: float = None, vp01_min_confidence: float = None):
        if gliner_threshold is not None:
            self.gliner_threshold = gliner_threshold
        if vp01_min_confidence is not None:
            self.vp01_min_confidence = vp01_min_confidence
        logger.info(f"NLPPipeline updated: gliner={self.gliner_threshold}, vp01_conf={self.vp01_min_confidence}")
       
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
    
    def _normalize_label(self, label: str) -> Tuple[str, Optional[str], bool]:
        """
        Normalize extracted label via alias lookup.
        
        Returns: (canonical_label, topic_or_none, is_ambiguous)
        """
        if not label:
            return label, None, False
        
        label_lower = label.lower()
        mappings = self.topic_config.label_alias_lookup.get(label_lower, [])
        
        if not mappings:
            return label, None, False
        
        if len(mappings) == 1:
            canonical, topic = mappings[0]
            return canonical, topic, False
        
        return label, None, True

    def _build_phrase_matcher(self) -> Tuple[PhraseMatcher, Dict[str, int]]:
        """Build PhraseMatcher from current known aliases."""
        aliases = self.get_known_aliases()
        matcher = PhraseMatcher(self._nlp.vocab, attr="LOWER")
        
        if aliases:
            patterns = [self._nlp.make_doc(alias) for alias in aliases.keys()]
            matcher.add("KNOWN", patterns)
        
        return matcher, aliases
    

    
    def run_gliner(self, text: str) -> List[Tuple[str, str]]:
        all_labels = list(self._label_to_topics.keys())
        if not all_labels:
            return []
        
        entities = self._gliner.predict_entities(text, all_labels, threshold=self.gliner_threshold)

        filtered = []
        for e in entities:
            span = e["text"]
            if not span:
                continue
            score = e.get("score", 0)
            
            logger.debug(f"GLiNER: '{span}' | label={e['label']} | score={score:.3f}")
            
            if span.lower() in PRONOUNS or span.split()[0].lower() in PRONOUNS:
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
        
        return [(e["text"], e["label"]) for e in filtered]
    
    def _assign_topic(self, label: str) -> Tuple[Optional[str], bool]:
        """
        Assign topic from label.
        Returns: (topic or None, is_ambiguous)
        """
        if not label:
            return "General", False
        
        label_lower = label.lower()
        topics = self._label_to_topics.get(label_lower, [])
        
        if len(topics) == 1:
            return topics[0], False
        elif len(topics) > 1:
            return None, True
        else:
            return "General", False
        
    
    
    async def extract_mentions(self, user_name: str, messages: List[Dict], session_id: str) -> List[Tuple[int, str, str, str]]:
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
        
        await emit(session_id, "pipeline", "known_matched", {
            "count": len(known_ents)
        }, verbose_only=True)
        
        loop = asyncio.get_running_loop()

        def _run_gliner_batch():
            results = []
            for msg in messages:
                msg_id = msg['id']
                extractions = self.run_gliner(msg['message'])
                for span, label in extractions:
                    results.append((msg_id, span, label))
            return results

        gliner_ents = await loop.run_in_executor(None, _run_gliner_batch)
        
        covered_texts: Dict[int, set] = {m['id']: set() for m in messages}
        resolved: List[Tuple[int, str, str, str]] = []
        ambiguous: List[Tuple[int, str, str, List[str]]] = []
        
        # process known entities first (highest priority)
        for span_text, eid in known_ents:
            profile = self.get_profiles().get(eid, {})
            matched_msg_ids = set()

            for msg in messages:
                if span_text.lower() in msg['message'].lower():
                    if msg['id'] not in matched_msg_ids:
                        matched_msg_ids.add(msg['id'])
                        covered_texts[msg['id']].add(span_text.lower())
                        resolved.append((
                            msg['id'],
                            span_text,
                            profile.get("type", "unknown"),
                            profile.get("topic") or "General"
                        ))
        
        gliner_filtered = set()
        # process GLiNER entities second
        for msg_id, span_text, label in gliner_ents:
            if is_covered(span_text, covered_texts[msg_id]):
                continue

            if not validate_entity(span_text, "General", self.topic_config):
                logger.debug(f"Filtered invalid GLiNER entity: '{span_text}'")
                gliner_filtered.add(span_text.lower())
                continue
            
            covered_texts[msg_id].add(span_text.lower())

            canonical_label, resolved_topic, is_alias_ambiguous = self._normalize_label(label)
            
            if resolved_topic:
                resolved.append((msg_id, span_text, canonical_label, resolved_topic))
            elif is_alias_ambiguous:
                topics = [t for _, t in self.topic_config.label_alias_lookup.get(label.lower(), [])]
                ambiguous.append((msg_id, span_text, canonical_label, topics))
            else:
                topic, is_ambiguous = self._assign_topic(canonical_label)
                
                if is_ambiguous:
                    topics = self._label_to_topics.get(canonical_label.lower(), [])
                    ambiguous.append((msg_id, span_text, canonical_label, topics))
                else:
                    resolved.append((msg_id, span_text, canonical_label, topic))
        
        await emit(session_id, "pipeline", "gliner_complete", {
            "raw_count": len(gliner_ents),
            "filtered_count": len(gliner_filtered)
        }, verbose_only=True)
        
        output: List[Tuple[int, str, str, str]] = list(resolved)
        
        user_content = format_vp01_input(messages, known_ents, gliner_ents, ambiguous, covered_texts, self.topic_config.label_block)
        
        system_prompt = ner_reasoning_prompt(user_name)
        await emit(session_id, "pipeline", "llm_call", {
            "stage": "ner",
            "prompt": user_content
        }, verbose_only=True)
        reasoning = await self.llm_client.call_llm(system_prompt, user_content)
        
        vp01_count = 0
        if reasoning and "<entities>" in reasoning:
            response = parse_entities(reasoning, min_confidence=self.vp01_min_confidence)
            if response:
                for entity in response:
                    if validate_entity(entity.name, entity.topic, self.topic_config):
                        if entity.name.lower() in gliner_filtered:
                            logger.info(f"VP-01 recovered GLiNER-filtered entity: '{entity.name}'")
                        output.append((entity.msg_id, entity.name, entity.label, entity.topic))
                        vp01_count += 1
                    else:
                        logger.debug(f"Filtered invalid VP-01 entity: '{entity.name}'")
            else:            
                logger.warning("VP-01 entities block empty or all below confidence threshold")
        
        logger.info(
            f"Extracted {len(output)} mentions: "
            f"{len(known_ents)} known, {len(gliner_ents)} gliner, "
            f"{vp01_count} from VP-01"
        )
        await emit(session_id, "pipeline", "ner_complete", {
            "total": len(output),
            "known": len(known_ents),
            "gliner": len(gliner_ents) - len(gliner_filtered),
            "vp01": vp01_count
        })
        
        return output
    
    def refresh_topic_mappings(self):
        """Rebuild label-to-topics map after TopicConfig change."""
        self._label_to_topics = self._build_label_to_topics()
        logger.info(f"NLPPipeline label mappings refreshed: {len(self._label_to_topics)} labels")
