from dataclasses import dataclass, field
from typing import List


@dataclass(slots=True)
class Scenario:
    name: str
    user_text: str
    expected_actions: List[str]
    weight: float = 1.0
    strict_order: bool = True
    required_actions: List[str] = field(default_factory=list)


def default_scenarios() -> List[Scenario]:
    return [
        Scenario("open_notepad", "Open notepad", ["open_app", "tts_speak"]),
        Scenario("security_status", "Check defender status", ["defender_status"]),
        Scenario("media_search", "Search lo-fi music on youtube", ["media_search"]),
        Scenario("system_snapshot", "Show cpu and ram usage", ["system_snapshot"]),
        Scenario("open_url", "Open github.com", ["open_url"]),
        Scenario("time_query", "What is the time in UTC", ["time_now"]),
        Scenario("fallback_speak", "Hello there", ["tts_speak"]),
    ]
