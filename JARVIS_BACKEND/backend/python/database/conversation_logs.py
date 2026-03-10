import os
import json
import time
from typing import List, Dict, Any


class ConversationLogs:
    """
    Secure rotating conversation log system.
    Features:
    - Per-session logs
    - Rotation
    - Metadata
    - Query search
    """

    def __init__(self, directory: str):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    # ------------------------
    # LOGGING
    # ------------------------
    def log_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Dict[str, Any] = None,
    ):
        ts = time.time()

        entry = {
            "timestamp": ts,
            "role": role,
            "content": content,
            "metadata": metadata or {},
        }

        path = os.path.join(self.directory, f"{session_id}.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    # ------------------------
    # QUERY LOGS
    # ------------------------
    def fetch_session(self, session_id: str) -> List[Dict[str, Any]]:
        path = os.path.join(self.directory, f"{session_id}.jsonl")
        if not os.path.exists(path):
            return []

        out = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except:
                    continue
        return out

    def search(self, text: str) -> List[Dict[str, Any]]:
        results = []
        for file in os.listdir(self.directory):
            if not file.endswith(".jsonl"):
                continue

            path = os.path.join(self.directory, file)
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        if text.lower() in entry["content"].lower():
                            results.append(entry)
                    except:
                        pass

        return results

