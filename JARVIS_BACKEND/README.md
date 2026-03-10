# JARVIS Backend

## Overview
This backend provides an agentic runtime for a desktop assistant:

1. Goal intake and planning.
2. Policy-guarded tool execution.
3. Step verification and recovery.
4. Structured telemetry and evaluation hooks.

## Key Paths

- `backend/python/core`: agent kernel (planner/executor/verifier/recovery).
- `backend/python/tools`: safe action handlers and utilities.
- `backend/python/policies`: risk scoring and policy guard.
- `backend/python/inference`: model registry/router for local + cloud selection.
- `backend/python/evaluation`: scenario-based planning checks.

## Run

```powershell
./scripts/start_backend.ps1
```

## Build Rust

```powershell
./scripts/build_rust.ps1
```

## Desktop Wrapper

```powershell
./scripts/run_desktop.ps1
```

## Desktop API (for `JARVIS_GUI` Electron app)

```powershell
./scripts/start_desktop_api.ps1
```

Key runtime API routes:

- `POST /goals`, `GET /goals/{goal_id}`
- `POST /actions`
- `GET /approvals`, `POST /approvals/{approval_id}/approve`
- `POST /schedules`, `GET /schedules`, `GET /schedules/{schedule_id}`, `POST /schedules/{schedule_id}/cancel`
- `GET /memory?query=...&limit=...`

## Tests

Run all backend tests (unit + API + E2E):

```powershell
./scripts/run_tests.ps1
```

Run with coverage gate:

```powershell
./scripts/run_tests.ps1 -Coverage
```

Coverage gate focuses on runtime-critical modules:

- `backend.python.core`
- `backend.python.policies`
- `backend.python.inference`
- `backend.python.desktop_api`

Run only a layer:

```powershell
python -m pytest tests/unit -q
python -m pytest tests/api -q
python -m pytest tests/e2e -q
```

## Goal Execution Budgets

Each goal now enforces runtime and step caps (including replans) to avoid runaway autonomous loops.

- Metadata overrides per goal:
  - `max_runtime_s`
  - `max_steps`
- Environment defaults:
  - `JARVIS_GOAL_MAX_RUNTIME_S` (default: `180`)
  - `JARVIS_GOAL_MAX_STEPS` (default: `24`)
- Automation source caps (`desktop-trigger`, `desktop-schedule`):
  - `JARVIS_AUTOMATION_MAX_RUNTIME_S` (default: `120`)
  - `JARVIS_AUTOMATION_MAX_STEPS` (default: `12`)
