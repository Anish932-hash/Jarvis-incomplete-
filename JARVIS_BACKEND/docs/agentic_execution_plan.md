# JARVIS Agentic Execution Plan

## Objective
Implement a production-grade autonomous desktop agent runtime that can safely transform user goals into verifiable multi-step execution.

## Runtime Flow

1. Goal Intake
- Input source: text, voice transcript, API call, or scheduled trigger.
- Goal is normalized into a `GoalRequest` object with metadata and constraints.

2. Planning
- Planner converts goal into an `ExecutionPlan`.
- Plan contains ordered `PlanStep` records with:
  - action name
  - typed arguments
  - verification criteria
  - retry profile and timeout

3. Policy + Risk Gate
- Each step is risk-scored.
- Policy guard decides allowed/blocked/confirmation-required.
- Unsafe actions are blocked before execution.

4. Execution
- Executor dispatches each step to Tool Registry.
- Tool handlers return structured `ActionResult`.
- Every step emits telemetry events.

5. Verification
- Verifier checks postconditions and structured evidence.
- Failed verification triggers Recovery Manager.

6. Recovery
- Retry with backoff up to per-step limits.
- If still failing, replan or mark goal failed.

7. Completion
- Goal status finalized as completed or failed.
- Execution trace stored for audit and evaluation.

## Model Routing Strategy

1. Wakeword: local Porcupine model.
2. STT: local Whisper primary, cloud fallback.
3. Reasoning: cloud LLM primary, local intent classifier fallback.
4. Embeddings: local embedding model for memory/retrieval.
5. TTS: local TTS primary (future), cloud fallback.

## Quality Gates

1. Python module compile pass.
2. Backend import smoke test.
3. Route loading smoke test.
4. Typecheck/lint for GUI and build tools.
5. Scenario evaluation runner with deterministic checks.
