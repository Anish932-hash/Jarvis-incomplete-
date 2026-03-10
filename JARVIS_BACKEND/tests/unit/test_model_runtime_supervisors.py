from __future__ import annotations

import asyncio
import importlib.util
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

from backend.python.agents.local_llm import LocalLLM
from backend.python.core.planner import Planner
from backend.python.perception.vision_engine import VisionEngine


_WORKSPACE_TMP_ROOT = Path(__file__).resolve().parents[3] / '.tmp' / 'unit_runtime_supervisors'
_WORKSPACE_TMP_ROOT.mkdir(parents=True, exist_ok=True)


class _StubRegistry:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def list_by_task(self, task: str) -> list[object]:
        if str(task or '').strip().lower() == 'reasoning':
            return list(self._rows)
        return []


class _StubRouter:
    def __init__(self, rows: list[object]) -> None:
        self.registry = _StubRegistry(rows)


class _ClosableRuntime:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _make_workspace_tempdir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix, dir=str(_WORKSPACE_TMP_ROOT)))


def _build_reasoning_planner(monkeypatch) -> tuple[Planner, Path, Path]:
    temp_dir = _make_workspace_tempdir('reasoning-')
    model_path = temp_dir / 'qwen3-14b-q8_0.gguf'
    model_path.write_text('stub', encoding='utf-8')

    original_find_spec = importlib.util.find_spec

    def _fake_find_spec(name: str, package: str | None = None):  # noqa: ARG001
        if name == 'llama_cpp':
            return object()
        return original_find_spec(name)

    monkeypatch.setattr('backend.python.core.planner.importlib.util.find_spec', _fake_find_spec)

    router = _StubRouter(
        [
            SimpleNamespace(
                name='local-auto-reasoning-qwen3-14b',
                provider='local',
                metadata={'path': str(model_path)},
            )
        ]
    )
    planner = Planner(model_router=router)
    planner.local_reasoning_enabled = True
    return planner, model_path, temp_dir


def test_planner_local_reasoning_runtime_status_and_reset(monkeypatch) -> None:
    planner, model_path, temp_dir = _build_reasoning_planner(monkeypatch)
    try:
        planner._mark_local_reasoning_runtime_success(
            model_name='local-auto-reasoning-qwen3-14b',
            model_path=str(model_path),
            backend='llama_cpp',
            load_latency_s=1.27,
        )

        status = planner.local_reasoning_runtime_status(preferred_model_name='qwen', limit=4)
        assert status['status'] == 'success'
        assert status['runtime_ready'] is True
        assert status['loaded_count'] == 1
        assert status['candidate_count'] == 1
        assert status['items'][0]['runtime_loaded'] is True

        runtime = _ClosableRuntime()
        planner._local_llama_cpp_cache[str(model_path)] = runtime
        reset = planner.reset_local_reasoning_runtime(model_name='qwen', clear_all=False)
        assert reset['status'] == 'success'
        assert reset['removed_count'] == 1
        assert runtime.closed is True
        assert reset['runtime']['loaded_count'] == 0
        assert planner._local_reasoning_runtime_state[
            planner._runtime_key('local-auto-reasoning-qwen3-14b', str(model_path))
        ]['loaded'] is False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_planner_reasoning_runtime_probe_success_updates_probe_state(monkeypatch) -> None:
    planner, model_path, temp_dir = _build_reasoning_planner(monkeypatch)
    try:
        monkeypatch.setattr(planner, '_ensure_local_llama_cpp_client', lambda *, model_path: object())
        monkeypatch.setattr(
            planner,
            '_run_local_llama_cpp_reasoning',
            lambda *, prompt, model_path: f'runtime ready: {prompt}',
        )

        result = planner.probe_local_reasoning_runtime(
            preferred_model_name='qwen',
            prompt='Report readiness briefly.',
            force_reload=False,
        )

        assert result['status'] == 'success'
        assert result['model'] == 'local-auto-reasoning-qwen3-14b'
        assert result['backend'] == 'llama_cpp'
        assert result['path'] == str(model_path)
        assert result['probe_prompt'] == 'Report readiness briefly.'
        assert 'runtime ready' in str(result.get('response_preview', ''))

        runtime = result['runtime']
        assert runtime['runtime_ready'] is True
        assert runtime['probe_healthy_count'] == 1
        assert runtime['active_model'] == 'local-auto-reasoning-qwen3-14b'
        item = runtime['items'][0]
        assert item['runtime_loaded'] is True
        assert item['runtime_last_probe_ok'] is True
        assert item['runtime_probe_attempts'] == 1
        assert item['runtime_last_probe_preview'].startswith('runtime ready')
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_planner_reasoning_runtime_probe_applies_cooldown_after_repeated_failures(monkeypatch) -> None:
    planner, _model_path, temp_dir = _build_reasoning_planner(monkeypatch)
    planner.local_reasoning_failure_streak_threshold = 2
    planner.local_reasoning_failure_cooldown_s = 30.0
    try:
        def _fail_bootstrap(*, model_path: str) -> object:  # noqa: ARG001
            raise RuntimeError('bootstrap failure')

        monkeypatch.setattr(planner, '_ensure_local_llama_cpp_client', _fail_bootstrap)

        first = planner.probe_local_reasoning_runtime(
            preferred_model_name='qwen',
            prompt='Probe runtime.',
            force_reload=False,
        )
        assert first['status'] == 'error'
        assert 'bootstrap failure' in str(first.get('message', ''))

        second = planner.probe_local_reasoning_runtime(
            preferred_model_name='qwen',
            prompt='Probe runtime.',
            force_reload=False,
        )
        assert second['status'] == 'error'
        assert 'cooling down' in str(second.get('message', ''))
        assert float(second.get('cooldown_remaining_s', 0.0) or 0.0) > 0.0

        runtime = second['runtime']
        assert runtime['cooldown_count'] == 1
        item = runtime['items'][0]
        assert item['runtime_last_probe_ok'] is False
        assert item['runtime_failure_streak'] >= 2
        assert float(item.get('runtime_cooldown_remaining_s', 0.0) or 0.0) > 0.0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_vision_runtime_warm_and_reset() -> None:
    temp_dir = _make_workspace_tempdir('vision-')
    try:
        (temp_dir / 'yolov10n.pt').write_text('stub', encoding='utf-8')
        (temp_dir / 'clip').mkdir()
        engine = VisionEngine(models_dir=str(temp_dir), device='cpu', enable_gpu=False, cache_embeddings=True)

        def _fake_load_yolo():
            engine._yolo_model = object()
            engine._mark_model_loaded('yolo', artifact_path=str(temp_dir / 'yolov10n.pt'), load_latency_s=0.41)
            return engine._yolo_model

        def _fake_load_clip():
            engine._clip_model = object()
            engine._clip_processor = object()
            engine._mark_model_loaded('clip', artifact_path=str(temp_dir / 'clip'), load_latency_s=0.73)
            return (engine._clip_model, engine._clip_processor)

        engine._load_yolo = _fake_load_yolo  # type: ignore[method-assign]
        engine._load_clip = _fake_load_clip  # type: ignore[method-assign]

        warmed = engine.warm_models(models=['yolo', 'clip'], force_reload=False)
        assert warmed['status'] == 'success'
        assert warmed['count'] == 2
        assert warmed['runtime']['loaded_count'] == 2

        reset = engine.reset_models(models=['clip'], clear_cache=True)
        assert reset['status'] == 'success'
        assert 'clip' in reset['removed']
        assert reset['clear_cache'] is True
        assert engine.runtime_status()['loaded_count'] == 1
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_local_llm_close_releases_runtime() -> None:
    llm = LocalLLM.__new__(LocalLLM)
    runtime = _ClosableRuntime()
    llm._llm = runtime
    llm.log = SimpleNamespace(warning=lambda *_args, **_kwargs: None)

    llm.close()

    assert runtime.closed is True
    assert llm._llm is None


def test_planner_choose_reasoning_provider_bans_local_when_route_policy_gated(monkeypatch) -> None:
    planner, _model_path, temp_dir = _build_reasoning_planner(monkeypatch)
    captured: dict[str, object] = {}
    try:
        planner.groq_client = SimpleNamespace(api_key='groq-key', is_ready=lambda: True)
        planner.nvidia_client = None
        planner.connector_orchestrator = SimpleNamespace(
            plan_reasoning_route=lambda **_kwargs: {
                'preferred_provider': 'local',
                'fallback_providers': ['groq', 'nvidia'],
                'banned_providers': [],
                'provider_affinity': {},
            }
        )
        planner.model_router = SimpleNamespace(
            registry=SimpleNamespace(),
            choose=lambda task, **kwargs: (
                captured.update(
                    {
                        'task': task,
                        'preferred_provider': kwargs.get('preferred_provider'),
                        'banned_providers': list(kwargs.get('banned_providers', [])),
                    }
                )
                or SimpleNamespace(
                    task=task,
                    model='groq-llm',
                    provider='groq',
                    reason='policy reroute',
                    score=3.1,
                    alternatives=['nvidia-nim'],
                    diagnostics={},
                )
            ),
        )
        monkeypatch.setattr(
            planner,
            '_local_reasoning_candidates',
            lambda preferred_model_name='': [
                {
                    'name': 'local-auto-reasoning-qwen3-14b',
                    'path': 'E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf',
                }
            ],
        )
        monkeypatch.setattr(
            planner,
            'local_reasoning_runtime_status',
            lambda **_kwargs: {
                'status': 'success',
                'candidate_count': 1,
                'runtime_ready': True,
                'loaded_count': 1,
                'probe_healthy_count': 1,
                'cooldown_count': 0,
                'error_count': 0,
                'items': [],
                'bridge': {'ready': True, 'configured': True, 'running': True},
            },
        )
        monkeypatch.setattr(
            planner,
            'local_reasoning_bridge_status',
            lambda **_kwargs: {'status': 'success', 'ready': True, 'configured': True, 'running': True},
        )
        planner.update_local_reasoning_route_policy_snapshot(
            {
                'source': 'desktop_bridge_profiles',
                'blacklisted': True,
                'autonomous_allowed': False,
                'review_required': True,
                'local_route_viable': True,
                'reason_code': 'persistent_failure_streak',
                'cloud_fallback_candidates': ['groq', 'nvidia'],
            }
        )

        decision = planner._choose_reasoning_provider({})

        assert decision is not None
        assert decision.provider == 'groq'
        assert captured['preferred_provider'] == 'groq'
        assert 'local' in list(captured['banned_providers'])
        assert decision.diagnostics['local_route_policy']['blacklisted'] is True
        assert decision.diagnostics['connector_route_plan']['preferred_provider_adjusted'] == 'groq'
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_planner_query_reasoning_provider_skips_local_fallback_when_policy_gated(monkeypatch) -> None:
    planner, _model_path, temp_dir = _build_reasoning_planner(monkeypatch)
    try:
        async def _groq_fail(*_args, **_kwargs) -> str:
            raise RuntimeError('groq unavailable')

        async def _nvidia_ok(*_args, **_kwargs) -> str:
            return 'nvidia fallback response'

        async def _local_should_not_run(*_args, **_kwargs) -> tuple[str, str]:
            raise AssertionError('local reasoning fallback should have been skipped')

        planner.groq_client = SimpleNamespace(api_key='groq-key', is_ready=lambda: True, ask=_groq_fail)
        planner.nvidia_client = SimpleNamespace(is_ready=lambda: True, generate_text=_nvidia_ok)
        planner.model_router = SimpleNamespace(
            registry=SimpleNamespace(
                note_result=lambda *_args, **_kwargs: None,
                mark_outage=lambda *_args, **_kwargs: None,
            )
        )
        planner.connector_orchestrator = SimpleNamespace(report_outcome=lambda *_args, **_kwargs: None)
        planner.local_reasoning_enabled = True
        monkeypatch.setattr(planner, '_query_local_reasoning', _local_should_not_run)

        result = asyncio.run(
            planner._query_reasoning_provider(
                prompt='Plan this task.',
                decision=SimpleNamespace(
                    provider='groq',
                    model='groq-llm',
                    diagnostics={
                        'connector_route_plan': {'fallback_providers': ['local', 'nvidia']},
                        'local_route_policy': {
                            'autonomous_allowed': False,
                            'local_route_viable': False,
                            'blacklisted': True,
                        },
                    },
                ),
            )
        )

        assert result == 'nvidia fallback response'
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_planner_choose_reasoning_provider_uses_voice_route_policy_for_voice_requests(monkeypatch) -> None:
    planner, _model_path, temp_dir = _build_reasoning_planner(monkeypatch)
    captured: dict[str, object] = {}
    try:
        planner.groq_client = SimpleNamespace(api_key='groq-key', is_ready=lambda: True)
        planner.nvidia_client = None
        planner.connector_orchestrator = SimpleNamespace(
            plan_reasoning_route=lambda **_kwargs: {
                'preferred_provider': 'local',
                'fallback_providers': ['groq', 'nvidia'],
                'banned_providers': [],
                'provider_affinity': {},
            }
        )
        planner.model_router = SimpleNamespace(
            registry=SimpleNamespace(),
            choose=lambda task, **kwargs: (
                captured.update(
                    {
                        'task': task,
                        'preferred_provider': kwargs.get('preferred_provider'),
                        'banned_providers': list(kwargs.get('banned_providers', [])),
                    }
                )
                or SimpleNamespace(
                    task=task,
                    model='groq-llm',
                    provider='groq',
                    reason='voice policy reroute',
                    score=2.9,
                    alternatives=['nvidia-nim'],
                    diagnostics={},
                )
            ),
        )
        monkeypatch.setattr(
            planner,
            '_local_reasoning_candidates',
            lambda preferred_model_name='': [
                {
                    'name': 'local-auto-reasoning-qwen3-14b',
                    'path': 'E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf',
                }
            ],
        )
        monkeypatch.setattr(
            planner,
            'local_reasoning_runtime_status',
            lambda **_kwargs: {
                'status': 'success',
                'candidate_count': 1,
                'runtime_ready': True,
                'loaded_count': 1,
                'probe_healthy_count': 1,
                'cooldown_count': 0,
                'error_count': 0,
                'items': [],
                'bridge': {'ready': True, 'configured': True, 'running': True},
            },
        )
        monkeypatch.setattr(
            planner,
            'local_reasoning_bridge_status',
            lambda **_kwargs: {'status': 'success', 'ready': True, 'configured': True, 'running': True},
        )
        planner.update_voice_route_policy_snapshot(
            {
                'mission_id': 'voice-mission-1',
                'risk_level': 'medium',
                'policy_profile': 'balanced',
                'reason_code': 'voice_route_policy_pressure',
                'reason': 'Voice mission reliability prefers cloud reasoning.',
                'ban_local_reasoning': True,
                'preferred_reasoning_provider': 'groq',
                'planning_constraints': {'prefer_brief_response': True, 'max_steps_hint': 4},
            }
        )

        decision = planner._choose_reasoning_provider({'source': 'voice-loop'})

        assert decision is not None
        assert decision.provider == 'groq'
        assert captured['preferred_provider'] == 'groq'
        assert 'local' in list(captured['banned_providers'])
        assert decision.diagnostics['voice_route_policy']['mission_id'] == 'voice-mission-1'
        assert decision.diagnostics['connector_route_plan']['preferred_provider_adjustment_reason'] == 'voice_route_policy_pressure'
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_planner_connector_diagnostics_include_voice_route_policy(monkeypatch) -> None:
    planner, _model_path, temp_dir = _build_reasoning_planner(monkeypatch)
    try:
        planner.connector_orchestrator = SimpleNamespace(
            diagnostics=lambda **_kwargs: {'status': 'success'},
            plan_reasoning_route=lambda **_kwargs: {
                'preferred_provider': 'local',
                'fallback_providers': ['groq'],
                'banned_providers': [],
                'provider_affinity': {},
            },
        )
        planner.model_router = SimpleNamespace(registry=SimpleNamespace())
        planner.groq_client = SimpleNamespace(api_key='groq-key', is_ready=lambda: True)
        planner.nvidia_client = None
        monkeypatch.setattr(
            planner,
            '_local_reasoning_candidates',
            lambda preferred_model_name='': [],
        )
        monkeypatch.setattr(
            planner,
            'local_reasoning_runtime_status',
            lambda **_kwargs: {
                'status': 'success',
                'candidate_count': 0,
                'runtime_ready': False,
                'loaded_count': 0,
                'probe_healthy_count': 0,
                'cooldown_count': 0,
                'error_count': 0,
                'items': [],
                'bridge': {'ready': False, 'configured': False, 'running': False},
            },
        )
        planner.update_voice_route_policy_snapshot(
            {
                'mission_id': 'voice-mission-2',
                'risk_level': 'high',
                'policy_profile': 'automation_safe',
                'reason_code': 'mission_reliability_polling_only',
                'ban_local_reasoning': True,
                'preferred_reasoning_provider': 'groq',
            }
        )

        payload = planner.connector_diagnostics(include_route_plan=True)

        assert payload['voice_route_policy']['mission_id'] == 'voice-mission-2'
        assert payload['route_plan']['voice_route_policy']['preferred_reasoning_provider'] == 'groq'
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
