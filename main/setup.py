import asyncio
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import Dict, List

from loguru import logger

from main.nlp_pipe import NLPPipeline
from main.prompts import get_connection_reasoning_prompt, get_profile_extraction_prompt
from main.utils import format_vp03_input, parse_connection_response
from jobs.jobs_utils import format_vp04_input, parse_new_facts
from shared.config.base import get_config_value
from shared.infra.resources import ResourceManager
from shared.config.topics_config import TopicConfig
from shared.infra.redis import RedisKeys
from shared.models.schema.dtypes import Fact

def _build_messages(responses: List[dict]) -> List[dict]:
    messages = []
    for i, r in enumerate(responses, start=1):
        answer = r.get("answer", "").strip()
        if answer:
            messages.append({
                "id": i,
                "message": answer,
                "role": "user"
            })
    return messages

async def _create_user_entity(resources, user_name: str) -> int:
    loop = asyncio.get_running_loop()

    existing = await loop.run_in_executor(
        resources.executor,
        resources.store.get_entity_by_id, 1
    )

    if existing and existing["canonical_name"] == user_name:
        logger.info(f"[SETUP] User entity already exists, reusing id=1")
        return 1
    
    # If a different entity is at ID=1, we can't assume a clean database.
    # We must use proper graph counter logic to determine the next ID
    ent_id = 1
    if existing and existing["canonical_name"] != user_name:
        logger.warning(f"[SETUP] User name changed but database not wiped. Generating new ID.")
        max_id = await loop.run_in_executor(
            resources.executor,
            resources.store.get_max_entity_id
        )
        ent_id = max_id + 1
        
    await resources.redis.set(RedisKeys.global_next_ent_id(), ent_id)
    
    user_aliases = get_config_value("user_aliases") or []
    all_aliases = [user_name] + [a.strip() for a in user_aliases if a.strip()]
    all_aliases = list(dict.fromkeys(all_aliases))
    
    embedding = await loop.run_in_executor(
        resources.executor,
        resources.embedding.encode_single,
        user_name
    )
    
    user_entity = {
        "id": ent_id,
        "canonical_name": user_name,
        "type": "person",
        "confidence": 1.0,
        "topic": "Identity",
        "embedding": embedding,
        "aliases": all_aliases,
        "session_id": "onboarding"
    }
    
    user_facts_raw = get_config_value("user_facts") or []
    fact_contents = (
        [f.strip() for f in user_facts_raw if f.strip()] 
        if user_facts_raw 
        else [f"The primary user named {user_name}"]
    )
    
    fact_embeddings = await loop.run_in_executor(
        resources.executor,
        resources.embedding.encode,
        fact_contents
    )
    
    now = datetime.now(timezone.utc)
    facts = [
        Fact(
            id=str(uuid.uuid4()),
            content=content,
            valid_at=now,
            embedding=emb,
            source_entity_id=ent_id
        )
        for content, emb in zip(fact_contents, fact_embeddings)
    ]
    
    await loop.run_in_executor(
        resources.executor,
        partial(resources.store.write_batch, [user_entity], [])
    )
    await loop.run_in_executor(
        resources.executor,
        partial(resources.store.create_facts_batch, ent_id, facts)
    )
    
    logger.info(f"[SETUP] Created user entity '{user_name}' (id={ent_id})")
    return ent_id

async def run_setup(
    resources: ResourceManager,
    topic_config: TopicConfig,
    user_name: str,
    responses: List[dict]
) -> dict:
    """
    Run onboarding extraction pipeline.
    Blocking — returns when complete.
    
    Args:
        resources: Shared ResourceManager instance
        topic_config: The finalized TopicConfig (post user review)
        user_name: The user's name (for VP prompt speaker context)
        responses: List of {"question": str, "answer": str}
    
    Returns:
        Summary dict with entity/connection/fact counts
    """
    messages = _build_messages(responses)
    if not messages:
        return {
            "success": True,
            "entities_created": 0,
            "connections_created": 0,
            "facts_created": 0,
            "entities": []
        }

    loop = asyncio.get_running_loop()

    user_id = await _create_user_entity(resources, user_name)

    user_aliases = get_config_value("user_aliases") or []

    entity_lookup: Dict[str, dict] = {}
    entity_lookup[user_name.lower()] = {
        "id": user_id,
        "canonical_name": user_name,
        "type": "person",
        "topic": "Identity"
    }
    for alias in user_aliases:
        if alias.strip():
            entity_lookup[alias.strip().lower()] = entity_lookup[user_name.lower()]

    known_aliases = {user_name.lower(): user_id}
    for alias in user_aliases:
        if alias.strip():
            known_aliases[alias.strip().lower()] = user_id

    nlp = NLPPipeline(
        llm=resources.llm_service,
        topic_config=topic_config,
        get_known_aliases=lambda: known_aliases,
        get_profiles=lambda: {user_id: {
            "canonical_name": user_name,
            "type": "person",
            "topic": "Identity"
        }},
        gliner=resources.gliner,
        spacy=resources.spacy
    )

    logger.info(f"[SETUP] Running NER on {len(messages)} responses")
    mentions = await nlp.extract_mentions(user_name, messages, "onboarding")

    if not mentions:
        logger.info("[SETUP] No entities found in onboarding responses")
        return {
            "success": True,
            "entities_created": 0,
            "connections_created": 0,
            "facts_created": 0,
            "entities": []
        }

    seen: Dict[str, dict] = {}
    for msg_id, name, typ, topic in mentions:
        key = name.strip().lower()
        if key not in seen:
            seen[key] = {
                "name": name.strip(),
                "type": typ,
                "topic": topic,
                "msg_ids": [msg_id]
            }
        else:
            if msg_id not in seen[key]["msg_ids"]:
                seen[key]["msg_ids"].append(msg_id)

    names_list = [e["name"] for e in seen.values()]
    embeddings = await loop.run_in_executor(
        resources.executor,
        resources.embedding.encode,
        names_list
    )

    entities = []

    for i, (key, entry) in enumerate(seen.items()):
        ent_id = await resources.redis.incr(RedisKeys.global_next_ent_id())

        entity = {
            "id": ent_id,
            "canonical_name": entry["name"],
            "type": entry["type"],
            "confidence": 0.9,
            "topic": entry["topic"],
            "embedding": embeddings[i],
            "aliases": [entry["name"]],
            "session_id": "onboarding"
        }
        entities.append(entity)
        entity_lookup[key] = entity

    logger.info(f"[SETUP] Registered {len(entities)} entities")

    candidates = [
        {
            "canonical_name": e["canonical_name"],
            "type": e["type"],
            "mentions": e["aliases"],
            "source_msgs": seen[e["canonical_name"].lower()]["msg_ids"]
        }
        for e in entities
    ]

    system_03 = get_connection_reasoning_prompt(user_name)
    user_03 = format_vp03_input(candidates, messages, "")

    logger.info(f"[SETUP] Running connection extraction on {len(candidates)} entities")
    reasoning_03 = await resources.llm_service.call_llm(system_03, user_03)

    relationships = []
    if reasoning_03:
        connections = parse_connection_response(reasoning_03)
        for mc in connections:
            for pair in mc.entity_pairs:
                ent_a = entity_lookup.get(pair.entity_a.lower())
                ent_b = entity_lookup.get(pair.entity_b.lower())
                if ent_a and ent_b:
                    relationships.append({
                        "entity_a": ent_a["canonical_name"],
                        "entity_b": ent_b["canonical_name"],
                        "entity_a_id": ent_a["id"],
                        "entity_b_id": ent_b["id"],
                        "message_id": f"msg_{mc.message_id}",
                        "confidence": pair.confidence,
                        "context": pair.context
                    })

    logger.info(f"[SETUP] Extracted {len(relationships)} connections")

    if entities or relationships:
        await loop.run_in_executor(
            resources.executor,
            partial(resources.store.write_batch, entities, relationships)
        )
        logger.info(f"[SETUP] Wrote {len(entities)} entities and {len(relationships)} relationships to graph")


    conversation_text = "\n".join(
        f"[USER]: {m['message']}" for m in messages
    )

    llm_input = [
        {
            "entity_name": e["canonical_name"],
            "entity_type": e["type"],
            "existing_facts": [],
            "known_aliases": e["aliases"]
        }
        for e in entities
    ]

    user_aliases_list = get_config_value("user_aliases") or []
    llm_input.append({
        "entity_name": user_name,
        "entity_type": "person",
        "existing_facts": [],
        "known_aliases": [user_name] + [a.strip() for a in user_aliases_list if a.strip()]
    })

    system_04 = get_profile_extraction_prompt(user_name)
    user_04 = format_vp04_input(llm_input, conversation_text)

    logger.info(f"[SETUP] Running profile extraction for {len(entities)} entities")
    reasoning_04 = await resources.llm_service.call_llm(system_04, user_04)

    facts_created = 0

    if reasoning_04:
        parsed_profiles = parse_new_facts(reasoning_04)

        if parsed_profiles:
            # Optimize: Batch all fact encodings together
            all_fact_contents = []
            fact_mapping = [] # (profile_idx, fact_idx)
            
            for p_idx, profile in enumerate(parsed_profiles):
                entity = entity_lookup.get(profile.canonical_name.lower())
                if not entity:
                    continue
                for f_idx, fact_content in enumerate(profile.facts):
                    all_fact_contents.append(fact_content)
                    fact_mapping.append((p_idx, f_idx))
                    
            if all_fact_contents:
                all_fact_embeddings = await loop.run_in_executor(
                    resources.executor,
                    resources.embedding.encode,
                    all_fact_contents
                )
                
                # Map embeddings back to profiles
                fact_embeddings_map = {}
                for i, (p_idx, f_idx) in enumerate(fact_mapping):
                    if p_idx not in fact_embeddings_map:
                        fact_embeddings_map[p_idx] = []
                    fact_embeddings_map[p_idx].append(all_fact_embeddings[i])

                write_tasks = []
                resolution_texts = []
                resolution_entities = []

                for p_idx, profile in enumerate(parsed_profiles):
                    entity = entity_lookup.get(profile.canonical_name.lower())
                    if not entity or p_idx not in fact_embeddings_map:
                        continue

                    ent_id = entity["id"]
                    new_facts = []
                    
                    for f_idx, fact_content in enumerate(profile.facts):
                        fact_embedding = fact_embeddings_map[p_idx][f_idx]
                        new_facts.append(Fact(
                            id=str(uuid.uuid4()),
                            content=fact_content,
                            valid_at=datetime.now(timezone.utc),
                            embedding=fact_embedding,
                            source_entity_id=ent_id
                        ))

                    if new_facts:
                        write_tasks.append(
                            loop.run_in_executor(
                                resources.executor,
                                partial(resources.store.create_facts_batch, ent_id, new_facts)
                            )
                        )
                        
                        resolution_text = f"{entity['canonical_name']}. " + " ".join(
                            f.content for f in new_facts
                        )
                        resolution_texts.append(resolution_text)
                        resolution_entities.append(ent_id)

                if write_tasks:
                    counts = await asyncio.gather(*write_tasks)
                    facts_created += sum(counts)
                    
                if resolution_texts:
                    res_embeddings = await loop.run_in_executor(
                        resources.executor,
                        resources.embedding.encode,
                        resolution_texts
                    )
                    
                    update_tasks = [
                        loop.run_in_executor(
                            resources.executor,
                            partial(
                                resources.store.update_entity_embedding,
                                ent_id,
                                emb
                            )
                        )
                        for ent_id, emb in zip(resolution_entities, res_embeddings)
                    ]
                    await asyncio.gather(*update_tasks)

    logger.info(f"[SETUP] Created {facts_created} facts across {len(entities)} entities")


    return {
        "success": True,
        "entities_created": len(entities),
        "connections_created": len(relationships),
        "facts_created": facts_created,
        "entities": [
            {
                "name": e["canonical_name"],
                "type": e["type"],
                "topic": e["topic"]
            }
            for e in entities
        ]
    }