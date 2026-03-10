from __future__ import annotations

from backend.python.inference.model_registry import ModelRegistry
from backend.python.inference.model_router import ModelRouter


def test_model_router_respects_provider_outage_penalty() -> None:
    registry = ModelRegistry()
    router = ModelRouter(registry)

    registry.mark_outage(provider="groq", penalty=1.0, cooldown_s=300.0)
    decision = router.choose("reasoning", requires_offline=False, high_quality=True, privacy_mode=False)

    assert decision.provider in {"nvidia", "local"}
    assert decision.score != 0.0


def test_model_router_adapts_after_model_failures() -> None:
    registry = ModelRegistry()
    router = ModelRouter(registry)

    for _ in range(8):
        registry.note_result("nvidia-nim", success=False, latency_ms=1200.0)
    for _ in range(5):
        registry.note_result("groq-llm", success=True, latency_ms=220.0)

    decision = router.choose("reasoning", requires_offline=False, high_quality=True, privacy_mode=False)
    assert decision.model == "groq-llm"

    snapshot = registry.runtime_snapshot(task="reasoning", limit=10)
    assert snapshot["status"] == "success"
    assert int(snapshot["count"]) >= 2


def test_model_router_mission_profile_privacy_prefers_local_stt() -> None:
    registry = ModelRegistry()
    router = ModelRouter(registry)

    decision = router.choose(
        "stt",
        requires_offline=False,
        high_quality=True,
        privacy_mode=True,
        mission_profile="privacy",
    )
    assert decision.provider == "local"
    assert isinstance(decision.diagnostics, dict)


def test_model_router_cost_cap_avoids_expensive_provider() -> None:
    registry = ModelRegistry()
    router = ModelRouter(registry)

    decision = router.choose(
        "reasoning",
        requires_offline=False,
        high_quality=True,
        privacy_mode=False,
        mission_profile="balanced",
        cost_sensitive=True,
        max_cost_units=0.26,
    )
    assert decision.provider in {"groq", "local"}
    assert isinstance(decision.diagnostics, dict)
    winner = decision.diagnostics.get("winner", {}) if isinstance(decision.diagnostics, dict) else {}
    details = winner.get("details", {}) if isinstance(winner, dict) else {}
    assert float(details.get("cost_units", 0.0)) <= 0.3


def test_model_router_plan_routes_balances_provider_share() -> None:
    registry = ModelRegistry()
    router = ModelRouter(registry)

    payload = router.plan_routes(
        ["reasoning", "reasoning", "reasoning", "reasoning"],
        mission_profile="throughput",
        max_provider_share=0.55,
        high_quality=True,
        cost_sensitive=False,
    )
    assert payload["status"] in {"success", "partial"}
    assert int(payload["success_count"]) >= 2
    providers = {
        str(row.get("provider", "")).strip().lower()
        for row in payload.get("items", [])
        if str(row.get("status", "")).strip().lower() == "success"
    }
    assert len(providers) >= 2


def test_model_router_route_bundle_exposes_multicapability_stack(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    stt_dir = tmp_path / "stt" / "whisper-large-v3"
    stt_dir.mkdir(parents=True, exist_ok=True)
    (stt_dir / "config.json").write_text("{}", encoding="utf-8")
    (stt_dir / "model.safetensors").write_bytes(b"weights")

    wakeword_dir = tmp_path / "wakeword"
    wakeword_dir.mkdir(parents=True, exist_ok=True)
    (wakeword_dir / "jarvis.ppn").write_bytes(b"wake")

    embedding_dir = tmp_path / "embeddings" / "all-mpnet-base-v2(Embeddings_model)"
    embedding_dir.mkdir(parents=True, exist_ok=True)
    (embedding_dir / "config.json").write_text("{}", encoding="utf-8")
    (embedding_dir / "modules.json").write_text("[]", encoding="utf-8")

    registry = ModelRegistry(scan_local_models=True, enforce_provider_keys=False)
    router = ModelRouter(registry)

    bundle = router.route_bundle(
        stack_name="voice",
        privacy_mode=True,
        requires_offline=True,
        mission_profile="privacy",
    )

    assert bundle["status"] == "success"
    assert bundle["stack_name"] == "voice"
    items = {
        str(row.get("task", "")).strip().lower(): row
        for row in bundle.get("items", [])
        if str(row.get("status", "")).strip().lower() == "success"
    }
    assert items["stt"]["provider"] == "local"
    assert items["wakeword"]["provider"] == "local"
    assert "stt" in bundle.get("selected_local_paths", {})
