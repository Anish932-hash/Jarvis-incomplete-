from __future__ import annotations

from backend.python.core.contracts import ActionResult, PlanStep
from backend.python.core.verifier import Verifier


def _step(*, args: dict | None = None, verify: dict | None = None) -> PlanStep:
    return PlanStep(step_id="step-1", action="test_action", args=args or {}, verify=verify or {})


def _result(*, output: dict, status: str = "success") -> ActionResult:
    return ActionResult(action="test_action", status=status, output=output)


def test_equals_arg_with_resolve_path_allows_relative_vs_absolute(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    step = _step(
        args={"path": "notes.txt"},
        verify={
            "checks": [
                {
                    "source": "result",
                    "type": "equals_arg",
                    "key": "path",
                    "arg": "path",
                    "normalize": "lower",
                    "resolve_path": True,
                }
            ]
        },
    )
    result = _result(output={"status": "success", "path": str((tmp_path / "notes.txt").resolve())})

    ok, reason = Verifier().verify(step, result)
    assert ok, reason


def test_list_none_contains_arg_passes_when_process_is_missing() -> None:
    step = _step(
        args={"name": "notepad"},
        verify={
            "checks": [
                {
                    "source": "result",
                    "type": "list_none_contains_arg",
                    "key": "process_names",
                    "arg": "name",
                    "normalize": "lower",
                    "strip_exe": True,
                }
            ]
        },
    )
    result = _result(output={"status": "success", "process_names": ["explorer.exe", "chrome.exe"]})

    ok, reason = Verifier().verify(step, result)
    assert ok, reason


def test_list_none_contains_arg_fails_when_process_still_exists() -> None:
    step = _step(
        args={"name": "notepad"},
        verify={
            "checks": [
                {
                    "source": "result",
                    "type": "list_none_contains_arg",
                    "key": "process_names",
                    "arg": "name",
                    "normalize": "lower",
                    "strip_exe": True,
                }
            ]
        },
    )
    result = _result(output={"status": "success", "process_names": ["notepad.exe", "chrome.exe"]})

    ok, reason = Verifier().verify(step, result)
    assert not ok
    assert "Unexpected item" in reason


def test_contains_arg_supports_case_normalization() -> None:
    step = _step(
        args={"url": "GitHub.com"},
        verify={
            "checks": [
                {
                    "source": "result",
                    "type": "contains_arg",
                    "key": "url",
                    "arg": "url",
                    "normalize": "lower",
                }
            ]
        },
    )
    result = _result(output={"status": "success", "url": "https://github.com/openai"})

    ok, reason = Verifier().verify(step, result)
    assert ok, reason


def test_strict_profile_requires_output_status_key() -> None:
    step = _step(verify={"expect_key": "value"})
    result = _result(output={"value": 1})

    ok, reason = Verifier().verify(step, result, context={"policy_profile": "automation_safe"})
    assert not ok
    assert "output.status" in reason


def test_strict_profile_requires_non_empty_output() -> None:
    step = _step(verify={"expect_result_status": "success"})
    result = _result(output={})

    ok, reason = Verifier().verify(step, result, context={"policy_profile": "automation_safe"})
    assert not ok
    assert "non-empty output" in reason


def test_interactive_profile_allows_minimal_success_result() -> None:
    step = _step(verify={})
    result = _result(output={})

    ok, reason = Verifier().verify(step, result, context={"policy_profile": "interactive"})
    assert ok, reason


def test_named_template_filesystem_path_exists(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "note.txt"
    target.write_text("hello", encoding="utf-8")

    step = _step(verify={"template": "filesystem.path_exists"})
    result = _result(output={"status": "success", "path": "note.txt"})

    ok, reason = Verifier().verify(step, result)
    assert ok, reason


def test_any_of_and_all_of_nested_checks() -> None:
    step = _step(
        verify={
            "checks": [
                {
                    "type": "all_of",
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "primary"},
                        {
                            "type": "any_of",
                            "checks": [
                                {"source": "result", "type": "equals", "key": "mode", "value": "strict"},
                                {"source": "result", "type": "equals", "key": "mode", "value": "safe"},
                            ],
                        },
                    ],
                }
            ]
        }
    )
    result = _result(output={"status": "success", "primary": True, "mode": "safe"})

    ok, reason = Verifier().verify(step, result)
    assert ok, reason


def test_number_gte_arg_check_uses_runtime_step_arg() -> None:
    step = _step(
        args={"minimum": 3},
        verify={"checks": [{"source": "result", "type": "number_gte_arg", "key": "count", "arg": "minimum"}]},
    )
    result = _result(output={"status": "success", "count": 2})

    ok, reason = Verifier().verify(step, result)
    assert not ok
    assert "minimum" in reason


def test_desktop_state_changed_check_passes_from_context() -> None:
    step = _step(
        verify={"checks": [{"source": "desktop_state", "type": "desktop_state_changed"}]},
    )
    result = _result(output={"status": "success"})

    ok, reason = Verifier().verify(step, result, context={"desktop_state": {"state_changed": True}})
    assert ok, reason


def test_changed_path_contains_check_fails_when_path_missing() -> None:
    step = _step(
        verify={"checks": [{"source": "desktop_state", "type": "changed_path_contains", "value": "input.mouse"}]},
    )
    result = _result(output={"status": "success"})

    ok, reason = Verifier().verify(step, result, context={"desktop_state": {"changed_paths": ["visual.screen_hash"]}})
    assert not ok
    assert "changed path" in reason.lower()


def test_verification_pressure_escalates_strictness_even_when_policy_is_off() -> None:
    step = _step(verify={})
    result = _result(output={})

    ok, reason = Verifier().verify(
        step,
        result,
        context={
            "policy": {"profile": "interactive", "strictness": "off"},
            "verification_pressure": 0.78,
        },
    )
    assert not ok
    assert "non-empty output" in reason.lower()
