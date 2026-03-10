"""
Dialogue Manager for JARVIS - handles conversational state and multi-turn interactions.

Provides:
- Conversation history management
- Context window optimization
- Turn-level state tracking
- Intent continuity
- Clarification handling
- Multi-turn understanding
"""

import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Set

from backend.python.core.episodic_memory import EpisodicMemory
from backend.python.core.runtime_memory import RuntimeMemory
from backend.python.utils.logger import Logger

from .local_llm import LLMMessage


class DialogueState(Enum):
    """Current state of dialogue."""
    GREETING = "greeting"
    CLARIFYING = "clarifying"
    EXECUTING = "executing"
    WAITING_APPROVAL = "waiting_approval"
    CONFIRMING = "confirming"
    ERROR_HANDLING = "error_handling"
    IDLE = "idle"


class TurnType(Enum):
    """Type of conversation turn."""
    COMMAND = "command"
    QUESTION = "question"
    CLARIFICATION = "clarification"
    CONFIRMATION = "confirmation"
    CORRECTION = "correction"
    FEEDBACK = "feedback"
    CHITCHAT = "chitchat"


@dataclass
class ConversationTurn:
    """Single turn in conversation."""
    turn_id: str
    timestamp: float
    speaker: str  # user or assistant
    text: str
    turn_type: TurnType
    intent: Optional[str] = None
    entities: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None


@dataclass
class DialogueContext:
    """Complete dialogue context including history and state."""
    conversation_id: str
    state: DialogueState
    turns: List[ConversationTurn]
    active_topic: Optional[str]
    pending_actions: List[Dict[str, Any]]
    waiting_for: Optional[str]  # What we're waiting for from user
    last_user_intent: Optional[str]
    clarification_needed: bool
    clarification_question: Optional[str]
    entity_cache: Dict[str, Any]  # Recently mentioned entities
    user_preferences: Dict[str, Any]


class DialogueManager:
    """
    Manages conversational state across multiple turns.
    
    Features:
    - Conversation history tracking
    - Context window management (token limits)
    - Multi-turn intent resolution
    - Clarification handling
    - Entity tracking across turns
    - Context compression for long conversations
    """

    def __init__(
        self,
        *,
        runtime_memory: RuntimeMemory,
        episodic_memory: EpisodicMemory,
        max_history_turns: int = 20,
        max_context_tokens: int = 4096,
        enable_clarifications: bool = True,
        enable_confirmations: bool = True,
    ):
        self.log = Logger("DialogueManager").get_logger()
        
        self.runtime_memory = runtime_memory
        self.episodic_memory = episodic_memory
        self.max_history_turns = max_history_turns
        self.max_context_tokens = max_context_tokens
        self.enable_clarifications = enable_clarifications
        self.enable_confirmations = enable_confirmations
        
        # Active conversations (keyed by conversation_id)
        self._conversations: Dict[str, DialogueContext] = {}
        
        # Default conversation for single-user mode
        self._default_conversation_id = "default"
        self._initialize_conversation(self._default_conversation_id)
        
        # Turn classification patterns
        self._clarification_indicators = {
            "what", "which", "where", "when", "who", "how",
            "clarify", "explain", "what do you mean",
        }
        self._correction_indicators = {
            "no", "not", "wrong", "incorrect", "actually", "instead",
            "meant", "i said", "correction",
        }
        self._confirmation_indicators = {
            "yes", "yeah", "yep", "ok", "okay", "sure", "correct",
            "right", "exactly", "confirm", "proceed", "go ahead",
        }
        
        self.log.info("DialogueManager initialized")

    def _initialize_conversation(self, conversation_id: str):
        """Initialize a new conversation."""
        self._conversations[conversation_id] = DialogueContext(
            conversation_id=conversation_id,
            state=DialogueState.IDLE,
            turns=[],
            active_topic=None,
            pending_actions=[],
            waiting_for=None,
            last_user_intent=None,
            clarification_needed=False,
            clarification_question=None,
            entity_cache={},
            user_preferences={},
        )

    def add_turn(
        self,
        text: str,
        speaker: str = "user",
        intent: Optional[str] = None,
        entities: Optional[Dict[str, Any]] = None,
        conversation_id: Optional[str] = None,
    ) -> ConversationTurn:
        """
        Add a new turn to conversation.
        
        Args:
            text: Turn text
            speaker: "user" or "assistant"
            intent: Detected intent (optional)
            entities: Extracted entities (optional)
            conversation_id: Conversation ID (uses default if None)
            
        Returns:
            Created ConversationTurn
        """
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            self._initialize_conversation(conv_id)
        
        context = self._conversations[conv_id]
        
        # Classify turn type
        turn_type = self._classify_turn_type(
            text,
            speaker,
            context.state,
            context.waiting_for,
        )
        
        # Create turn
        turn = ConversationTurn(
            turn_id=f"turn_{uuid.uuid4().hex[:8]}",
            timestamp=time.time(),
            speaker=speaker,
            text=text,
            turn_type=turn_type,
            intent=intent,
            entities=entities or {},
            context={
                "state": context.state.value,
                "active_topic": context.active_topic,
            },
        )
        
        # Add to history
        context.turns.append(turn)
        
        # Update entity cache
        if entities:
            context.entity_cache.update(entities)
            # Expire old entities after 5 turns
            if len(context.turns) > 5:
                self._expire_old_entities(context)
        
        # Update state based on turn
        self._update_state(context, turn)
        
        # Trim history if needed
        if len(context.turns) > self.max_history_turns:
            self._compress_history(context)
        
        return turn

    def _classify_turn_type(
        self,
        text: str,
        speaker: str,
        state: DialogueState,
        waiting_for: Optional[str],
    ) -> TurnType:
        """Classify the type of conversation turn."""
        if speaker == "assistant":
            return TurnType.QUESTION if "?" in text else TurnType.COMMAND
        
        text_lower = text.lower()
        
        # Check for correction
        if any(indicator in text_lower for indicator in self._correction_indicators):
            return TurnType.CORRECTION
        
        # Check for confirmation
        if waiting_for == "confirmation":
            if any(indicator in text_lower for indicator in self._confirmation_indicators):
                return TurnType.CONFIRMATION
        
        # Check for clarification
        if state == DialogueState.CLARIFYING:
            return TurnType.CLARIFICATION
        
        # Check if asking question
        if "?" in text or any(text_lower.startswith(q) for q in ["what", "where", "when", "why", "how", "who"]):
            return TurnType.QUESTION
        
        # Check for feedback
        if any(word in text_lower for word in ["thanks", "thank", "great", "good", "nice", "perfect"]):
            return TurnType.FEEDBACK
        
        # Check for command (imperative)
        imperative_verbs = ["open", "close", "start", "stop", "create", "delete", "search", "find", "show", "run"]
        if any(text_lower.startswith(verb) for verb in imperative_verbs):
            return TurnType.COMMAND
        
        # Default to chitchat
        return TurnType.CHITCHAT

    def _update_state(self, context: DialogueContext, turn: ConversationTurn):
        """Update dialogue state based on new turn."""
        if turn.speaker == "user":
            if turn.turn_type == TurnType.COMMAND:
                context.state = DialogueState.EXECUTING
                context.last_user_intent = turn.intent
            
            elif turn.turn_type == TurnType.CLARIFICATION:
                # User provided clarification
                context.state = DialogueState.EXECUTING
                context.clarification_needed = False
            
            elif turn.turn_type == TurnType.CONFIRMATION:
                # User confirmed
                context.state = DialogueState.EXECUTING
                context.waiting_for = None
            
            elif turn.turn_type == TurnType.CORRECTION:
                # User corrected something
                context.state = DialogueState.EXECUTING
            
            else:
                context.state = DialogueState.IDLE
        
        else:  # assistant turn
            if context.clarification_needed:
                context.state = DialogueState.CLARIFYING
                context.waiting_for = "clarification"
            
            elif turn.turn_type == TurnType.QUESTION:
                context.state = DialogueState.WAITING_APPROVAL
                context.waiting_for = "confirmation"

    def _expire_old_entities(self, context: DialogueContext):
        """Remove entities not mentioned in recent turns."""
        # Keep entities mentioned in last 5 turns
        recent_entities = set()
        for turn in context.turns[-5:]:
            recent_entities.update(turn.entities.keys())
        
        # Remove old entities
        expired = [key for key in context.entity_cache if key not in recent_entities]
        for key in expired:
            del context.entity_cache[key]

    def _compress_history(self, context: DialogueContext):
        """Compress old conversation history."""
        # Keep recent turns
        recent_turns = context.turns[-self.max_history_turns:]
        
        # Summarize older turns and store in episodic memory
        old_turns = context.turns[:-self.max_history_turns]
        if old_turns:
            summary = self._summarize_turns(old_turns)
            self.episodic_memory.add_episode({
                "type": "conversation_summary",
                "conversation_id": context.conversation_id,
                "turn_count": len(old_turns),
                "summary": summary,
                "timestamp": time.time(),
            })
        
        context.turns = recent_turns

    def _summarize_turns(self, turns: List[ConversationTurn]) -> str:
        """Create summary of conversation turns."""
        # Simple extractive summary
        key_turns = [t for t in turns if t.turn_type in {TurnType.COMMAND, TurnType.QUESTION}]
        
        if not key_turns:
            return "General conversation"
        
        summary_parts = []
        for turn in key_turns[:5]:  # Max 5 key turns
            if turn.intent:
                summary_parts.append(f"{turn.speaker}: {turn.intent}")
            else:
                summary_parts.append(f"{turn.speaker}: {turn.text[:50]}")
        
        return "; ".join(summary_parts)

    def get_conversation_context(
        self,
        conversation_id: Optional[str] = None,
        max_tokens: Optional[int] = None,
    ) -> List[LLMMessage]:
        """
        Get conversation history formatted for LLM.
        
        Args:
            conversation_id: Conversation ID (uses default if None)
            max_tokens: Maximum tokens to include (uses default if None)
            
        Returns:
            List of LLMMessage objects for LLM context
        """
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return []
        
        context = self._conversations[conv_id]
        max_tok = max_tokens or self.max_context_tokens
        
        messages = []
        token_count = 0
        
        # Add turns in reverse order (most recent first)
        for turn in reversed(context.turns):
            # Rough token estimation (1 token ≈ 4 chars)
            turn_tokens = len(turn.text) // 4
            
            if token_count + turn_tokens > max_tok:
                break
            
            role = turn.speaker if turn.speaker in {"user", "assistant"} else "user"
            messages.insert(0, LLMMessage(role=role, content=turn.text))
            token_count += turn_tokens
        
        return messages

    def get_entity_cache(
        self,
        conversation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get recently mentioned entities."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return {}
        
        return self._conversations[conv_id].entity_cache.copy()

    def set_pending_action(
        self,
        action: Dict[str, Any],
        conversation_id: Optional[str] = None,
    ):
        """Set an action pending user confirmation."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            self._initialize_conversation(conv_id)
        
        context = self._conversations[conv_id]
        context.pending_actions.append(action)
        context.state = DialogueState.WAITING_APPROVAL

    def get_pending_actions(
        self,
        conversation_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get actions pending confirmation."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return []
        
        return self._conversations[conv_id].pending_actions.copy()

    def clear_pending_actions(
        self,
        conversation_id: Optional[str] = None,
    ):
        """Clear pending actions."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id in self._conversations:
            self._conversations[conv_id].pending_actions = []

    def request_clarification(
        self,
        question: str,
        conversation_id: Optional[str] = None,
    ):
        """Request clarification from user."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            self._initialize_conversation(conv_id)
        
        context = self._conversations[conv_id]
        context.clarification_needed = True
        context.clarification_question = question
        context.state = DialogueState.CLARIFYING
        context.waiting_for = "clarification"

    def is_clarification_needed(
        self,
        conversation_id: Optional[str] = None,
    ) -> bool:
        """Check if clarification is needed."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return False
        
        return self._conversations[conv_id].clarification_needed

    def get_clarification_question(
        self,
        conversation_id: Optional[str] = None,
    ) -> Optional[str]:
        """Get pending clarification question."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return None
        
        return self._conversations[conv_id].clarification_question

    def get_dialogue_state(
        self,
        conversation_id: Optional[str] = None,
    ) -> DialogueState:
        """Get current dialogue state."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return DialogueState.IDLE
        
        return self._conversations[conv_id].state

    def get_last_user_intent(
        self,
        conversation_id: Optional[str] = None,
    ) -> Optional[str]:
        """Get last detected user intent."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return None
        
        return self._conversations[conv_id].last_user_intent

    def get_conversation_summary(
        self,
        conversation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get summary of conversation."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return {"turn_count": 0, "state": "idle"}
        
        context = self._conversations[conv_id]
        
        turn_types = {}
        for turn in context.turns:
            turn_types[turn.turn_type.value] = turn_types.get(turn.turn_type.value, 0) + 1
        
        return {
            "conversation_id": conv_id,
            "turn_count": len(context.turns),
            "state": context.state.value,
            "active_topic": context.active_topic,
            "entity_count": len(context.entity_cache),
            "pending_actions": len(context.pending_actions),
            "turn_types": turn_types,
            "last_turn": context.turns[-1].text if context.turns else None,
        }

    def reset_conversation(
        self,
        conversation_id: Optional[str] = None,
    ):
        """Reset conversation state."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id in self._conversations:
            self._initialize_conversation(conv_id)
            self.log.info(f"Conversation {conv_id} reset")

    def get_recent_topics(
        self,
        n: int = 5,
        conversation_id: Optional[str] = None,
    ) -> List[str]:
        """Get recent conversation topics."""
        conv_id = conversation_id or self._default_conversation_id
        
        if conv_id not in self._conversations:
            return []
        
        context = self._conversations[conv_id]
        
        # Extract topics from intents and entities
        topics = []
        for turn in reversed(context.turns):
            if turn.intent:
                topics.append(turn.intent)
            for entity_type, entity_value in turn.entities.items():
                topics.append(f"{entity_type}:{entity_value}")
            
            if len(topics) >= n:
                break
        
        return topics[:n]

    def should_confirm_action(
        self,
        action: Dict[str, Any],
        conversation_id: Optional[str] = None,
    ) -> bool:
        """
        Determine if action should be confirmed before execution.
        
        Args:
            action: Action to be executed
            conversation_id: Conversation ID
            
        Returns:
            True if confirmation is needed
        """
        if not self.enable_confirmations:
            return False
        
        conv_id = conversation_id or self._default_conversation_id
        
        # High-risk actions always need confirmation
        high_risk_actions = {"delete", "remove", "terminate", "shutdown", "format"}
        if any(risk in action.get("action", "").lower() for risk in high_risk_actions):
            return True
        
        # First-time actions need confirmation
        if conv_id in self._conversations:
            context = self._conversations[conv_id]
            if len(context.turns) < 3:  # New conversation
                return True
        
        return False
