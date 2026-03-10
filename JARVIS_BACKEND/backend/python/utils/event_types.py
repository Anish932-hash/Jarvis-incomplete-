from enum import Enum, auto

class EventType(Enum):
    SYSTEM_START = auto()
    SYSTEM_SHUTDOWN = auto()
    USER_COMMAND = auto()
    SPEECH_INPUT = auto()
    SPEECH_OUTPUT = auto()
    FILE_MODIFIED = auto()
    TASK_SCHEDULED = auto()
    TASK_COMPLETED = auto()
    APP_LAUNCHED = auto()
    SECURITY_ALERT = auto()
    RESOURCE_OVERLOAD = auto()
    ERROR_OCCURRED = auto()
    CONFIG_RELOADED = auto()