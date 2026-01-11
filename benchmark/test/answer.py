import json
import sys


DATASET_FILES = {
    "multi": "test_multi_session.json",
    "single": "test_single_user.json",
    "tempo": "test_tempo_session.json",
    "abs": "test_abstinence.json",
    "know": "test_know_updates.json",
    "user-ai": "test_user_assistant.json",
    "user-pref": "test_user_pref_session.json"
}

def answer_key():

    if (len(sys.argv) < 2):
        print("2 values, index and file dataset")
        sys.exit(1)
    
    index = int(sys.argv[1])
    sess_type = sys.argv[2]

    file = DATASET_FILES[sess_type]
    with open(f"{file}", "r") as f:
        questions = json.load(f)

    q = questions[index]  # First question

    print(f"Question: {q['question']}")
    print(f"Expected answer: {q['answer']}")
    print(f"Answer session IDs: {q['answer_session_ids']}")
    print(f"\n{'='*60}\nEVIDENCE TURNS:\n{'='*60}\n")

    for index, session in enumerate(q["haystack_sessions"]):
        for j, turn in enumerate(session):
            if turn.get("has_answer"):
                print(f"Session {index}, Turn {j} ({turn['role']}):")
                print(f"{turn['content']}")
                print("-" * 40)
    
    print("-" * 40)
    print("-" * 40)


if __name__ == "__main__":
    answer_key()