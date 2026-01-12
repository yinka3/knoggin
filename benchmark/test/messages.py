import json

with open("../data/longmemeval_s_cleaned.json", "r") as f:
    data = json.load(f)

multi_session = []
single_user = []
tempo_session = []
abstinence = []
knowledge_updates = []
single_user_pref = []
single_assistant = []

for instance in data:
    q_type = instance["question_type"]
    entry = {
        "question_id": instance["question_id"],
        "question_type": instance["question_type"],
        "question": instance["question"],
        "answer": instance["answer"],
        "question_date": instance["question_date"],
        "haystack_dates": instance["haystack_dates"],
        "haystack_session_ids": instance.get("haystack_session_ids", []),
        "haystack_sessions": instance["haystack_sessions"],
        "answer_session_ids": instance.get("answer_session_ids", [])
    }
    
    if instance["question_id"].endswith("_abs") and len(abstinence) < 50:
        abstinence.append(entry)
    elif q_type == "single-session-user" and not instance["question_id"].endswith("_abs") and len(single_user) < 50:
        single_user.append(entry)
    elif q_type == "temporal-reasoning" and not instance["question_id"].endswith("_abs") and len(tempo_session) < 50:
        tempo_session.append(entry)
    elif q_type == "multi-session" and not instance["question_id"].endswith("_abs") and len(multi_session) < 50:
        multi_session.append(entry)
    elif q_type == "knowledge-update" and not instance["question_id"].endswith("_abs") and len(knowledge_updates) < 50:
        knowledge_updates.append(entry)
    elif q_type == "single-session-preference" and not instance["question_id"].endswith("_abs") and len(single_user_pref) < 50:
        single_user_pref.append(entry)
    elif q_type == "single-session-assistant" and not instance["question_id"].endswith("_abs") and len(single_assistant) < 50:
        single_assistant.append(entry)

    if (len(tempo_session) >= 50 and len(single_user) >= 50 and 
        len(multi_session) >= 50 and len(abstinence) >= 50 and 
        len(knowledge_updates) >= 50 and len(single_user_pref) >= 50 and
        len(single_assistant) >= 50):
        break

with open("test_tempo_session.json", "w") as f:
    json.dump(tempo_session, f, indent=2)
with open("test_single_user.json", "w") as f:
    json.dump(single_user, f, indent=2)
with open("test_multi_session.json", "w") as f:
    json.dump(multi_session, f, indent=2)
with open("test_abstinence.json", "w") as f:
    json.dump(abstinence, f, indent=2)
with open("test_know_updates.json", "w") as f:
    json.dump(knowledge_updates, f, indent=2)
with open("test_user_pref_session.json", "w") as f:
    json.dump(single_user_pref, f, indent=2)
with open("test_user_assistant.json", "w") as f:
    json.dump(single_assistant, f, indent=2)

# Stats
for name, data in [("tempo-session", tempo_session), ("single-user", single_user), 
                   ("multi-session", multi_session), ("abstinence", abstinence), 
                   ("know-updates", knowledge_updates), ("single-pref", single_user_pref), ("user-AI", single_assistant)]:
    total_sessions = sum(len(t["haystack_sessions"]) for t in data)
    total_turns = sum(len(turn) for t in data for turn in t["haystack_sessions"])
    print(f"\n{name}:")
    print(f"  Instances: {len(data)}")
    print(f"  Total sessions: {total_sessions}")
    print(f"  Total turns: {total_turns}")