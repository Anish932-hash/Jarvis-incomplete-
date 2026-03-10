import re
from pathlib import Path
from typing import Any, Dict, Tuple

from .contracts import ActionResult, PlanStep


class Verifier:
    """
    Validates whether a step result satisfies expected postconditions.
    Supports legacy verify fields plus extensible `checks` rules.
    """

    PROFILE_STRICTNESS_BY_PROFILE = {
        "interactive": "standard",
        "automation_safe": "strict",
        "automation_power": "strict",
    }
    NAMED_TEMPLATES: Dict[str, Dict[str, Any]] = {
        "filesystem.path_exists": {
            "expect_status": "success",
            "checks": [{"source": "result", "type": "path_exists", "key": "path"}],
        },
        "filesystem.write_integrity": {
            "expect_status": "success",
            "expect_keys": ["path"],
            "checks": [
                {"source": "result", "type": "path_exists", "key": "path"},
                {"source": "result", "type": "number_gte", "key": "bytes", "value": 0},
            ],
        },
        "browser.dom_fetch": {
            "expect_status": "success",
            "expect_keys": ["url"],
            "checks": [{"source": "result", "type": "number_gte", "key": "chars", "value": 0}],
        },
        "desktop.click_effect": {
            "expect_status": "success",
            "checks": [
                {
                    "type": "any_of",
                    "checks": [
                        {"source": "result", "type": "equals", "key": "screen_changed", "value": True},
                        {"source": "result", "type": "key_truthy", "key": "result.element_id"},
                        {"source": "desktop_state", "type": "desktop_state_changed"},
                        {"source": "result", "type": "number_gte", "key": "attempt", "value": 1},
                    ],
                }
            ],
        },
        "process.termination": {
            "expect_status": "success",
            "checks": [{"source": "result", "type": "number_gte", "key": "count", "value": 1}],
        },
    }

    def verify(
        self,
        step: PlanStep,
        result: ActionResult,
        context: Dict[str, Any] | None = None,
    ) -> Tuple[bool, str]:
        if result.status == "blocked":
            return False, "Step blocked by policy."
        if result.status == "failed":
            return False, result.error or "Step failed."

        strictness = self._resolve_strictness(context or {})
        rules: Dict[str, Any] = self._apply_strictness_template(step.verify or {}, strictness)
        rules = self._apply_named_templates(rules)
        if rules.get("optional"):
            return True, "Optional verification passed."

        if strictness == "strict":
            if not isinstance(result.output, dict) or not result.output:
                return False, "Strict verification requires non-empty output payload."
            if "status" not in result.output:
                return False, "Strict verification requires output.status."

        sources: Dict[str, Dict[str, Any]] = {"result": result.output}
        if context:
            for key, value in context.items():
                if isinstance(value, dict):
                    sources[key] = value

        expect_result_status = rules.get("expect_result_status")
        if expect_result_status and result.status != expect_result_status:
            return False, f"Expected result status {expect_result_status}, got {result.status}."

        expect_status = rules.get("expect_status")
        if expect_status:
            actual = result.output.get("status")
            if actual != expect_status:
                return False, f"Expected status {expect_status}, got {actual}."

        expect_key = rules.get("expect_key")
        if expect_key and expect_key not in result.output:
            return False, f"Missing expected key: {expect_key}."

        expect_keys = rules.get("expect_keys")
        if isinstance(expect_keys, list):
            for key in expect_keys:
                if isinstance(key, str) and key not in result.output:
                    return False, f"Missing expected key: {key}."

        expect_truthy = rules.get("expect_truthy")
        if isinstance(expect_truthy, list):
            for key in expect_truthy:
                if isinstance(key, str) and not self._read_value(result.output, key):
                    return False, f"Expected truthy value for key: {key}."

        contains_map = rules.get("expect_output_contains")
        if isinstance(contains_map, dict):
            for key, expected in contains_map.items():
                if not isinstance(key, str):
                    continue
                actual = self._read_value(result.output, key)
                if str(expected) not in str(actual):
                    return False, f"Expected output[{key}] to contain {expected!r}, got {actual!r}."

        equals_map = rules.get("expect_output_equals")
        if isinstance(equals_map, dict):
            for key, expected in equals_map.items():
                if not isinstance(key, str):
                    continue
                actual = self._read_value(result.output, key)
                if actual != expected:
                    return False, f"Expected output[{key}] == {expected!r}, got {actual!r}."

        in_map = rules.get("expect_output_in")
        if isinstance(in_map, dict):
            for key, allowed_values in in_map.items():
                if not isinstance(key, str) or not isinstance(allowed_values, list):
                    continue
                actual = self._read_value(result.output, key)
                if actual not in allowed_values:
                    return False, f"Expected output[{key}] in {allowed_values!r}, got {actual!r}."

        numeric_min = rules.get("expect_numeric_min")
        if isinstance(numeric_min, dict):
            for key, min_value in numeric_min.items():
                if not isinstance(key, str):
                    continue
                actual_number = self._to_float(self._read_value(result.output, key))
                expected_number = self._to_float(min_value)
                if actual_number is None or expected_number is None or actual_number < expected_number:
                    return False, f"Expected output[{key}] >= {min_value!r}, got {self._read_value(result.output, key)!r}."

        numeric_max = rules.get("expect_numeric_max")
        if isinstance(numeric_max, dict):
            for key, max_value in numeric_max.items():
                if not isinstance(key, str):
                    continue
                actual_number = self._to_float(self._read_value(result.output, key))
                expected_number = self._to_float(max_value)
                if actual_number is None or expected_number is None or actual_number > expected_number:
                    return False, f"Expected output[{key}] <= {max_value!r}, got {self._read_value(result.output, key)!r}."

        checks = rules.get("checks")
        if isinstance(checks, list):
            for index, item in enumerate(checks, start=1):
                if not isinstance(item, dict):
                    return False, f"Invalid check at index {index}: expected object."
                ok, reason = self._evaluate_check(item, sources=sources, step=step)
                if not ok:
                    return False, f"check[{index}] failed: {reason}"

        return True, "Verification passed."

    def _resolve_strictness(self, context: Dict[str, Any]) -> str:
        policy_profile = ""
        verification_pressure = self._to_float(context.get("verification_pressure"))
        raw_policy = context.get("policy")
        if isinstance(raw_policy, dict):
            policy_profile = str(raw_policy.get("profile", "")).strip().lower()
            explicit = str(raw_policy.get("strictness", "")).strip().lower()
            if explicit in {"off", "standard", "strict"}:
                if verification_pressure is not None and verification_pressure >= 0.66:
                    return "strict"
                if verification_pressure is not None and verification_pressure >= 0.38 and explicit == "off":
                    return "standard"
                return explicit
        if not policy_profile:
            policy_profile = str(context.get("policy_profile", "")).strip().lower()
        strictness = self.PROFILE_STRICTNESS_BY_PROFILE.get(policy_profile, "standard")
        if verification_pressure is not None and verification_pressure >= 0.66:
            return "strict"
        if verification_pressure is not None and verification_pressure >= 0.38 and strictness == "off":
            return "standard"
        return strictness

    @staticmethod
    def _apply_strictness_template(rules: Dict[str, Any], strictness: str) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(rules)
        if strictness in {"standard", "strict"} and "expect_result_status" not in merged:
            merged["expect_result_status"] = "success"
        if strictness == "strict" and "expect_status" not in merged:
            merged["expect_status"] = "success"
        return merged

    def _apply_named_templates(self, rules: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(rules)
        template_names: list[str] = []

        raw_template = merged.get("template")
        if isinstance(raw_template, str):
            name = raw_template.strip()
            if name:
                template_names.append(name)
        elif isinstance(raw_template, list):
            template_names.extend(str(item).strip() for item in raw_template if str(item).strip())

        raw_templates = merged.get("templates")
        if isinstance(raw_templates, str):
            name = raw_templates.strip()
            if name:
                template_names.append(name)
        elif isinstance(raw_templates, list):
            template_names.extend(str(item).strip() for item in raw_templates if str(item).strip())

        for name in template_names:
            template = self.NAMED_TEMPLATES.get(name)
            if not isinstance(template, dict):
                continue
            merged = self._merge_rules(merged, template)

        merged.pop("template", None)
        merged.pop("templates", None)
        return merged

    @staticmethod
    def _merge_rules(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(base)
        for key, value in extra.items():
            if key in {"checks", "expect_keys", "expect_truthy"}:
                left = merged.get(key)
                rows: list[Any] = []
                if isinstance(left, list):
                    rows.extend(left)
                if isinstance(value, list):
                    rows.extend(value)
                merged[key] = rows
            elif key in {"expect_output_contains", "expect_output_equals", "expect_output_in", "expect_numeric_min", "expect_numeric_max"}:
                left_map = merged.get(key)
                next_map: Dict[str, Any] = {}
                if isinstance(left_map, dict):
                    next_map.update(left_map)
                if isinstance(value, dict):
                    next_map.update(value)
                merged[key] = next_map
            else:
                merged[key] = value
        return merged

    def _evaluate_check(
        self,
        check: Dict[str, Any],
        *,
        sources: Dict[str, Dict[str, Any]],
        step: PlanStep,
    ) -> Tuple[bool, str]:
        kind = str(check.get("type", "key_exists")).strip().lower()
        source = str(check.get("source", "result")).strip() or "result"
        payload = sources.get(source, {})
        key = str(check.get("key", "")).strip()
        actual = self._read_value(payload, key) if key else payload

        if kind == "key_exists":
            if key and actual is not None:
                return True, "ok"
            return False, f"Missing key {key!r} in source={source}."

        if kind == "key_truthy":
            if actual:
                return True, "ok"
            return False, f"Expected truthy value for key {key!r} in source={source}."

        if kind == "equals":
            expected = check.get("value")
            if actual == expected:
                return True, "ok"
            return False, f"Expected {key!r} == {expected!r}, got {actual!r}."

        if kind == "contains":
            expected = str(check.get("value", ""))
            if expected and expected in str(actual):
                return True, "ok"
            return False, f"Expected {key!r} to contain {expected!r}, got {actual!r}."

        if kind == "all_of":
            nested = check.get("checks")
            if not isinstance(nested, list) or not nested:
                return False, "checks is required for all_of."
            for idx, item in enumerate(nested, start=1):
                if not isinstance(item, dict):
                    return False, f"Invalid nested check at {idx}."
                ok, reason = self._evaluate_check(item, sources=sources, step=step)
                if not ok:
                    return False, f"all_of[{idx}] failed: {reason}"
            return True, "ok"

        if kind == "any_of":
            nested = check.get("checks")
            if not isinstance(nested, list) or not nested:
                return False, "checks is required for any_of."
            failures: list[str] = []
            for idx, item in enumerate(nested, start=1):
                if not isinstance(item, dict):
                    failures.append(f"{idx}: invalid nested check")
                    continue
                ok, reason = self._evaluate_check(item, sources=sources, step=step)
                if ok:
                    return True, "ok"
                failures.append(f"{idx}: {reason}")
            return False, f"No any_of checks matched ({'; '.join(failures[:4])})."

        if kind == "contains_arg":
            arg_name = str(check.get("arg", "")).strip()
            if not arg_name:
                return False, "arg is required for contains_arg."
            arg_value = step.args.get(arg_name)
            if arg_value is None:
                if check.get("allow_missing_arg"):
                    return True, "ok"
                return False, f"Missing step arg {arg_name!r}."

            needle = str(arg_value).strip()
            haystack = str(actual)
            if check.get("normalize") == "lower":
                needle = needle.lower()
                haystack = haystack.lower()
            if check.get("strip_exe"):
                needle = needle.replace(".exe", "").strip()
            if not needle:
                return False, f"arg {arg_name!r} resolved to empty value."
            if needle in haystack:
                return True, "ok"
            return False, f"Expected {key!r} to contain arg {arg_name!r}={needle!r}, got {actual!r}."

        if kind == "regex":
            pattern = str(check.get("pattern", "")).strip()
            if pattern and re.search(pattern, str(actual)):
                return True, "ok"
            return False, f"Expected regex {pattern!r} to match {actual!r}."

        if kind == "equals_arg":
            arg_name = str(check.get("arg", "")).strip()
            if not arg_name:
                return False, "arg is required for equals_arg."
            arg_value = step.args.get(arg_name)
            if arg_value is None:
                if check.get("allow_missing_arg"):
                    return True, "ok"
                return False, f"Missing step arg {arg_name!r}."

            left = actual
            right = arg_value
            if check.get("resolve_path"):
                left_path = self._normalize_path_value(left)
                right_path = self._normalize_path_value(right)
                if left_path is not None and right_path is not None:
                    left = left_path
                    right = right_path
            if check.get("normalize") == "lower":
                left = str(left).lower()
                right = str(right).lower()
            if check.get("strip_exe"):
                right = str(right).replace(".exe", "").strip()
            if left == right:
                return True, "ok"
            return False, f"Expected {key!r} == arg {arg_name!r} ({right!r}), got {left!r}."

        if kind == "in":
            allowed = check.get("values")
            if isinstance(allowed, list) and actual in allowed:
                return True, "ok"
            return False, f"Expected {actual!r} in {allowed!r}."

        if kind == "number_gte":
            threshold = self._to_float(check.get("value"))
            value = self._to_float(actual)
            if threshold is not None and value is not None and value >= threshold:
                return True, "ok"
            return False, f"Expected {key!r} >= {check.get('value')!r}, got {actual!r}."

        if kind == "number_lte":
            threshold = self._to_float(check.get("value"))
            value = self._to_float(actual)
            if threshold is not None and value is not None and value <= threshold:
                return True, "ok"
            return False, f"Expected {key!r} <= {check.get('value')!r}, got {actual!r}."

        if kind == "number_gte_arg":
            arg_name = str(check.get("arg", "")).strip()
            if not arg_name:
                return False, "arg is required for number_gte_arg."
            threshold = self._to_float(step.args.get(arg_name))
            value = self._to_float(actual)
            if threshold is None:
                if check.get("allow_missing_arg"):
                    return True, "ok"
                return False, f"Missing or invalid numeric step arg {arg_name!r}."
            if value is not None and value >= threshold:
                return True, "ok"
            return False, f"Expected {key!r} >= arg {arg_name!r} ({threshold}), got {actual!r}."

        if kind == "number_lte_arg":
            arg_name = str(check.get("arg", "")).strip()
            if not arg_name:
                return False, "arg is required for number_lte_arg."
            threshold = self._to_float(step.args.get(arg_name))
            value = self._to_float(actual)
            if threshold is None:
                if check.get("allow_missing_arg"):
                    return True, "ok"
                return False, f"Missing or invalid numeric step arg {arg_name!r}."
            if value is not None and value <= threshold:
                return True, "ok"
            return False, f"Expected {key!r} <= arg {arg_name!r} ({threshold}), got {actual!r}."

        if kind == "path_exists":
            if actual is None:
                if check.get("allow_missing"):
                    return True, "ok"
                return False, f"Expected path value at key {key!r}."
            path_value = self._normalize_path_value(actual)
            if path_value is None:
                return False, f"Invalid path value for key {key!r}: {actual!r}"
            path = Path(path_value)
            if not path.exists():
                return False, f"Expected existing path for key {key!r}, got {path_value!r}."
            path_type = str(check.get("path_type", "any")).strip().lower() or "any"
            if path_type == "file" and not path.is_file():
                return False, f"Expected file for key {key!r}, got {path_value!r}."
            if path_type == "dir" and not path.is_dir():
                return False, f"Expected directory for key {key!r}, got {path_value!r}."
            return True, "ok"

        if kind == "desktop_state_changed":
            expected = bool(check.get("value", True))
            if key:
                state_changed = bool(self._read_value(payload, key))
            else:
                state_changed = bool(payload.get("state_changed")) if isinstance(payload, dict) else bool(actual)
            if state_changed is expected:
                return True, "ok"
            return False, f"Expected desktop state changed={expected}, got {state_changed}."

        if kind == "changed_path_contains":
            needle = str(check.get("value", "")).strip()
            if not needle:
                return False, "value is required for changed_path_contains."
            if key:
                changed_paths = self._read_value(payload, key)
            else:
                changed_paths = payload.get("changed_paths") if isinstance(payload, dict) else actual
            if not isinstance(changed_paths, list):
                return False, f"Expected list for changed paths, got {type(changed_paths).__name__}."
            normalize = str(check.get("normalize", "")).strip().lower()
            expected_value = needle.lower() if normalize == "lower" else needle
            for item in changed_paths:
                text = str(item)
                candidate = text.lower() if normalize == "lower" else text
                if expected_value in candidate:
                    return True, "ok"
            return False, f"No changed path contains {needle!r}."

        if kind == "list_contains":
            expected = check.get("value")
            if isinstance(actual, list) and expected in actual:
                return True, "ok"
            return False, f"Expected list {key!r} to contain {expected!r}."

        if kind == "list_any_contains":
            needle = str(check.get("value", "")).strip()
            if not needle:
                return False, "value is required for list_any_contains."
            if isinstance(actual, list) and any(needle in str(item) for item in actual):
                return True, "ok"
            return False, f"Expected an item in {key!r} to contain {needle!r}."

        if kind == "list_any_contains_arg":
            arg_name = str(check.get("arg", "")).strip()
            if not arg_name:
                return False, "arg is required for list_any_contains_arg."
            arg_value = step.args.get(arg_name)
            if arg_value is None:
                return False, f"Missing step arg {arg_name!r}."

            needle = str(arg_value).strip()
            if check.get("normalize") == "lower":
                needle = needle.lower()
            needle = needle.replace(".exe", "").strip()
            if not needle:
                return False, f"arg {arg_name!r} resolved to empty value."

            if not isinstance(actual, list):
                return False, f"Expected list for key {key!r}, got {type(actual).__name__}."

            for item in actual:
                candidate = str(item)
                if check.get("normalize") == "lower":
                    candidate = candidate.lower()
                if needle in candidate:
                    return True, "ok"
            return False, f"No item in {key!r} contains arg value {needle!r}."

        if kind == "list_none_contains_arg":
            arg_name = str(check.get("arg", "")).strip()
            if not arg_name:
                return False, "arg is required for list_none_contains_arg."
            arg_value = step.args.get(arg_name)
            if arg_value is None:
                if check.get("allow_missing_arg"):
                    return True, "ok"
                return False, f"Missing step arg {arg_name!r}."

            needle = str(arg_value).strip()
            if check.get("normalize") == "lower":
                needle = needle.lower()
            if check.get("strip_exe"):
                needle = needle.replace(".exe", "").strip()
            if not needle:
                return False, f"arg {arg_name!r} resolved to empty value."

            if not isinstance(actual, list):
                return False, f"Expected list for key {key!r}, got {type(actual).__name__}."

            for item in actual:
                candidate = str(item)
                if check.get("normalize") == "lower":
                    candidate = candidate.lower()
                if needle in candidate:
                    return False, f"Unexpected item in {key!r} contains arg value {needle!r}: {item!r}."
            return True, "ok"

        return False, f"Unknown verification check type: {kind}"

    @staticmethod
    def _read_value(payload: Any, key: str) -> Any:
        if not key:
            return payload
        current = payload
        for part in key.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            return float(value)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _normalize_path_value(value: Any) -> str | None:
        if value is None:
            return None
        try:
            raw = str(value).strip()
            if not raw:
                return None
            return str(Path(raw).expanduser().resolve())
        except Exception:  # noqa: BLE001
            return None
