import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { backendClient } from '@/lib/backend-client';

describe('backendClient', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => ({
        ok: true,
        text: async () => JSON.stringify({ status: 'ok' }),
      }))
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('calls desktop API health endpoint', async () => {
    const payload = await backendClient.health();
    expect(payload).toEqual({ status: 'ok' });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/health');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls policy profiles endpoint', async () => {
    await backendClient.policyProfiles();

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/policy/profiles');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls model capabilities endpoint', async () => {
    await backendClient.modelsCapabilities({ limit_per_task: 5 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/models/capabilities?limit_per_task=5');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls model route bundle endpoint with routing params', async () => {
    await backendClient.modelsRouteBundle({
      stack_name: 'voice',
      tasks: ['tts', 'stt'],
      requires_offline: true,
      privacy_mode: true,
      latency_sensitive: false,
      cost_sensitive: true,
      mission_profile: 'privacy',
      max_cost_units: 0.5,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/models/route-bundle?stack_name=voice&task=tts&task=stt&requires_offline=1&privacy_mode=1&latency_sensitive=0&cost_sensitive=1&mission_profile=privacy&max_cost_units=0.5'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls model runtime supervisors endpoint with filters', async () => {
    await backendClient.modelsRuntimeSupervisors({ preferred_model_name: 'qwen', limit: 6 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/models/runtime-supervisors?preferred_model_name=qwen&limit=6');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls local model inventory and bridge profiles endpoints with filters', async () => {
    await backendClient.modelsLocalInventory({ task: 'reasoning', limit: 20 });
    await backendClient.modelBridgeProfiles({ task: 'reasoning', limit: 12 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(2);

    const [inventoryUrl, inventoryOptions] = fetchMock.mock.calls[0];
    expect(inventoryUrl).toBe('http://127.0.0.1:8765/models/local-inventory?task=reasoning&limit=20');
    expect(inventoryOptions).toMatchObject({ method: 'GET' });

    const [profilesUrl, profilesOptions] = fetchMock.mock.calls[1];
    expect(profilesUrl).toBe('http://127.0.0.1:8765/models/bridge-profiles?task=reasoning&limit=12');
    expect(profilesOptions).toMatchObject({ method: 'GET' });
  });

  it('calls model operations summary endpoint with routing params', async () => {
    await backendClient.modelOperationsSummary({
      stack_name: 'voice',
      preferred_model_name: 'qwen',
      limit_per_task: 3,
      runtime_limit: 5,
      requires_offline: true,
      privacy_mode: true,
      latency_sensitive: false,
      cost_sensitive: true,
      mission_profile: 'privacy',
      max_cost_units: 0.75,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/models/operations?stack_name=voice&preferred_model_name=qwen&limit_per_task=3&runtime_limit=5&requires_offline=1&privacy_mode=1&latency_sensitive=0&cost_sensitive=1&mission_profile=privacy&max_cost_units=0.75'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls runtime health summary endpoint with runtime and bridge flags', async () => {
    await backendClient.runtimeHealthSummary({
      stack_name: 'voice',
      preferred_model_name: 'qwen',
      limit_per_task: 3,
      runtime_limit: 5,
      requires_offline: true,
      privacy_mode: true,
      latency_sensitive: false,
      cost_sensitive: true,
      mission_profile: 'privacy',
      max_cost_units: 0.75,
      refresh_provider_credentials: true,
      include_bridge_context: true,
      refresh_rust_caps: true,
      probe_tts_bridge: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/runtime/health?stack_name=voice&preferred_model_name=qwen&limit_per_task=3&runtime_limit=5&requires_offline=1&privacy_mode=1&latency_sensitive=0&cost_sensitive=1&mission_profile=privacy&max_cost_units=0.75&refresh_provider_credentials=1&include_bridge_context=1&refresh_rust_caps=1&probe_tts_bridge=1'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts reasoning runtime warm, probe, and reset payloads', async () => {
    await backendClient.warmReasoningRuntime({
      preferred_model_name: 'qwen',
      load_all: true,
      force_reload: true,
    });
    await backendClient.probeReasoningRuntime({
      preferred_model_name: 'qwen',
      prompt: 'Summarize runtime readiness.',
      force_reload: false,
    });
    await backendClient.resetReasoningRuntime({
      model_name: 'qwen',
      clear_all: false,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(3);

    const [warmUrl, warmOptions] = fetchMock.mock.calls[0];
    expect(warmUrl).toBe('http://127.0.0.1:8765/models/runtime-supervisors/reasoning/warm');
    expect(warmOptions).toMatchObject({ method: 'POST' });
    expect((warmOptions as RequestInit).body).toBe(
      JSON.stringify({
        preferred_model_name: 'qwen',
        load_all: true,
        force_reload: true,
      })
    );

    const [probeUrl, probeOptions] = fetchMock.mock.calls[1];
    expect(probeUrl).toBe('http://127.0.0.1:8765/models/runtime-supervisors/reasoning/probe');
    expect(probeOptions).toMatchObject({ method: 'POST' });
    expect((probeOptions as RequestInit).body).toBe(
      JSON.stringify({
        preferred_model_name: 'qwen',
        prompt: 'Summarize runtime readiness.',
        force_reload: false,
      })
    );

    const [resetUrl, resetOptions] = fetchMock.mock.calls[2];
    expect(resetUrl).toBe('http://127.0.0.1:8765/models/runtime-supervisors/reasoning/reset');
    expect(resetOptions).toMatchObject({ method: 'POST' });
    expect((resetOptions as RequestInit).body).toBe(
      JSON.stringify({
        model_name: 'qwen',
        clear_all: false,
      })
    );
  });

  it('posts reasoning runtime restart payload', async () => {
    await backendClient.restartReasoningRuntime({
      preferred_model_name: 'qwen',
      prompt: 'Summarize runtime readiness after restart.',
      load_all: false,
      force_reload: true,
      probe: true,
      clear_all: false,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/models/runtime-supervisors/reasoning/restart');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        preferred_model_name: 'qwen',
        prompt: 'Summarize runtime readiness after restart.',
        load_all: false,
        force_reload: true,
        probe: true,
        clear_all: false,
      })
    );
  });

  it('posts reasoning bridge profile apply payload', async () => {
    await backendClient.applyLocalReasoningBridgeProfile({
      profile_id: 'reasoning-bridge-local-auto-reasoning-qwen3-14b',
      replace: true,
      restart: true,
      wait_ready: true,
      force: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/models/runtime-supervisors/reasoning/bridge/profile');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        profile_id: 'reasoning-bridge-local-auto-reasoning-qwen3-14b',
        replace: true,
        restart: true,
        wait_ready: true,
        force: true,
      })
    );
  });

  it('posts local neural tts profile apply payload', async () => {
    await backendClient.applyLocalNeuralTtsProfile({
      profile_id: 'tts-bridge-orpheus-3b-tts-f16',
      replace: true,
      restart: true,
      wait_ready: true,
      force: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/tts/local-neural/profile');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        profile_id: 'tts-bridge-orpheus-3b-tts-f16',
        replace: true,
        restart: true,
        wait_ready: true,
        force: true,
      })
    );
  });

  it('posts launch template execution payload', async () => {
    await backendClient.executeModelLaunchTemplate({
      profile_id: 'reasoning-bridge-local-auto-reasoning-qwen3-14b',
      template_id: 'reasoning-llama-server-local-auto-reasoning-qwen3-14b',
      replace: true,
      wait_ready: true,
      force: true,
      probe: true,
      auto_fallback: true,
      retry_on_failure: true,
      max_attempts: 3,
      retry_profile: 'stabilized',
      retry_base_delay_ms: 150,
      retry_max_delay_ms: 1200,
      retry_jitter_ms: 40,
      retry_prefer_recommended: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/models/launch-template/execute');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        profile_id: 'reasoning-bridge-local-auto-reasoning-qwen3-14b',
        template_id: 'reasoning-llama-server-local-auto-reasoning-qwen3-14b',
        replace: true,
        wait_ready: true,
        force: true,
        probe: true,
        auto_fallback: true,
        retry_on_failure: true,
        max_attempts: 3,
        retry_profile: 'stabilized',
        retry_base_delay_ms: 150,
        retry_max_delay_ms: 1200,
        retry_jitter_ms: 40,
        retry_prefer_recommended: true,
      })
    );
  });

  it('builds launch template history query params correctly', async () => {
    await backendClient.modelLaunchTemplateHistory({
      limit: 32,
      bridge_kind: 'reasoning',
      profile_id: 'reasoning-bridge-local-auto-reasoning-qwen3-14b',
      failure_like: true,
      after_event_id: 9,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/models/launch-template/history?limit=32&bridge_kind=reasoning&profile_id=reasoning-bridge-local-auto-reasoning-qwen3-14b&failure_like=1&after_event_id=9'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds launch template event detail query params correctly', async () => {
    await backendClient.modelLaunchTemplateEventDetail({
      event_id: 17,
      sibling_limit: 8,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/models/launch-template/event?event_id=17&sibling_limit=8');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts vision runtime warm and reset payloads', async () => {
    await backendClient.warmVisionRuntime({
      models: ['clip', 'sam'],
      force_reload: true,
    });
    await backendClient.resetVisionRuntime({
      models: ['clip'],
      clear_cache: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(2);

    const [warmUrl, warmOptions] = fetchMock.mock.calls[0];
    expect(warmUrl).toBe('http://127.0.0.1:8765/models/runtime-supervisors/vision/warm');
    expect(warmOptions).toMatchObject({ method: 'POST' });
    expect((warmOptions as RequestInit).body).toBe(
      JSON.stringify({
        models: ['clip', 'sam'],
        force_reload: true,
      })
    );

    const [resetUrl, resetOptions] = fetchMock.mock.calls[1];
    expect(resetUrl).toBe('http://127.0.0.1:8765/models/runtime-supervisors/vision/reset');
    expect(resetOptions).toMatchObject({ method: 'POST' });
    expect((resetOptions as RequestInit).body).toBe(
      JSON.stringify({
        models: ['clip'],
        clear_cache: true,
      })
    );
  });

  it('builds approvals query params correctly', async () => {
    await backendClient.approvals({ status: 'pending', include_expired: true, limit: 25 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/approvals?status=pending&include_expired=1&limit=25');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds telemetry events query params correctly', async () => {
    await backendClient.telemetryEvents({ event: 'goal.completed', limit: 30, after_id: 100 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/telemetry/events?event=goal.completed&limit=30&after_id=100');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds telemetry stream url correctly', () => {
    const url = backendClient.telemetryStreamUrl({
      event: 'goal.completed',
      limit: 50,
      after_id: 42,
      timeout_s: 15,
      heartbeat_s: 4,
    });
    expect(url).toBe(
      'http://127.0.0.1:8765/telemetry/stream?event=goal.completed&limit=50&after_id=42&timeout_s=15&heartbeat_s=4'
    );
  });

  it('builds voice session stream url correctly', () => {
    const url = backendClient.voiceSessionStreamUrl({
      events: ['voice.*', 'action.executed'],
      limit: 120,
      after_id: 24,
      timeout_s: 40,
      heartbeat_s: 5,
      state_interval_s: 1.2,
      include_state: true,
      include_action_events: true,
    });
    expect(url).toBe(
      'http://127.0.0.1:8765/voice/session/stream?events=voice.*&events=action.executed&limit=120&after_id=24&timeout_s=40&heartbeat_s=5&state_interval_s=1.2&include_state=1&include_action_events=1'
    );
  });

  it('calls continuous voice session routes', async () => {
    await backendClient.startVoiceContinuousSession({
      duration_s: 45,
      max_turns: 4,
      stop_on_idle_s: 12,
      stop_after: true,
      config: { wakeword_enabled: false, auto_submit: true },
    });
    await backendClient.listVoiceContinuousRuns({ limit: 8 });
    await backendClient.getVoiceContinuousRun('voice-cont-123');
    await backendClient.cancelVoiceContinuousSession('voice-cont-123', { reason: 'operator-stop' });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(4);

    const [startUrl, startOptions] = fetchMock.mock.calls[0];
    expect(startUrl).toBe('http://127.0.0.1:8765/voice/session/continuous/start');
    expect(startOptions).toMatchObject({ method: 'POST' });
    expect((startOptions as RequestInit).body).toBe(
      JSON.stringify({
        duration_s: 45,
        max_turns: 4,
        stop_on_idle_s: 12,
        stop_after: true,
        config: { wakeword_enabled: false, auto_submit: true },
      })
    );

    const [listUrl, listOptions] = fetchMock.mock.calls[1];
    expect(listUrl).toBe('http://127.0.0.1:8765/voice/session/continuous?limit=8');
    expect(listOptions).toMatchObject({ method: 'GET' });

    const [getUrl, getOptions] = fetchMock.mock.calls[2];
    expect(getUrl).toBe('http://127.0.0.1:8765/voice/session/continuous/voice-cont-123');
    expect(getOptions).toMatchObject({ method: 'GET' });

    const [cancelUrl, cancelOptions] = fetchMock.mock.calls[3];
    expect(cancelUrl).toBe('http://127.0.0.1:8765/voice/session/continuous/voice-cont-123/cancel');
    expect(cancelOptions).toMatchObject({ method: 'POST' });
    expect((cancelOptions as RequestInit).body).toBe(JSON.stringify({ reason: 'operator-stop' }));
  });

  it('posts action payload to actions endpoint', async () => {
    await backendClient.executeAction({
      action: 'copy_file',
      args: { source: 'a.txt', destination: 'b.txt' },
      approval_id: 'approval-123',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/actions');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        action: 'copy_file',
        args: { source: 'a.txt', destination: 'b.txt' },
        approval_id: 'approval-123',
      })
    );
  });

  it('posts external connector preflight simulation payload', async () => {
    await backendClient.externalConnectorPreflightSimulation({
      action: 'external_email_send',
      providers: ['google', 'graph'],
      args: { to: ['alice@example.com'], subject: 'Status update' },
      max_runs: 10,
      include_override_scenario: true,
      scenarios: [{ id: 'baseline' }, { id: 'override', external_cooldown_override: true }],
      source: 'desktop-ui',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/preflight/simulate');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        action: 'external_email_send',
        providers: ['google', 'graph'],
        args: { to: ['alice@example.com'], subject: 'Status update' },
        max_runs: 10,
        include_override_scenario: true,
        scenarios: [{ id: 'baseline' }, { id: 'override', external_cooldown_override: true }],
        source: 'desktop-ui',
      })
    );
  });

  it('calls external connector preflight simulation templates endpoint', async () => {
    await backendClient.externalConnectorPreflightSimulationTemplates({
      action: 'external_email_send',
      provider: 'google',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/preflight/simulation/templates?action=external_email_send&provider=google'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls external connector preflight simulation history endpoint with filters', async () => {
    await backendClient.externalConnectorPreflightSimulationHistory({
      action: 'external_email_send',
      provider: 'graph',
      status: 'blocked',
      limit: 25,
      include_results: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/preflight/simulations?limit=25&action=external_email_send&provider=graph&status=blocked&include_results=1'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls external connector preflight simulation compare endpoint', async () => {
    await backendClient.externalConnectorPreflightSimulationCompare({
      left_id: 'sim_fake_01',
      right_id: 'sim_fake_02',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/preflight/simulations/compare?left_id=sim_fake_01&right_id=sim_fake_02'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts external connector preflight simulation promote payload', async () => {
    await backendClient.externalConnectorPreflightSimulationPromote({
      simulation_id: 'sim_fake_03',
      compare_against_simulation_id: 'sim_fake_01',
      dry_run: false,
      require_compare: true,
      require_improvement: true,
      mission_mode: 'stable',
      reason: 'promote_test',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/preflight/simulations/promote');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        simulation_id: 'sim_fake_03',
        compare_against_simulation_id: 'sim_fake_01',
        dry_run: false,
        require_compare: true,
        require_improvement: true,
        mission_mode: 'stable',
        reason: 'promote_test',
      })
    );
  });

  it('calls external connector preflight promotions endpoint', async () => {
    await backendClient.externalConnectorPreflightSimulationPromotions({
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'stable',
      status: 'applied',
      applied_only: true,
      limit: 20,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/preflight/simulations/promotions?limit=20&action=external_email_send&provider=google&mission_mode=stable&status=applied&applied_only=1'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls external connector preflight simulation trends endpoint', async () => {
    await backendClient.externalConnectorPreflightSimulationTrends({
      action: 'external_email_send',
      provider: 'google',
      limit: 300,
      recent_window: 12,
      baseline_window: 60,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/preflight/simulations/trends?limit=300&action=external_email_send&provider=google&recent_window=12&baseline_window=60'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls external connector remediation policy status endpoint', async () => {
    await backendClient.externalConnectorRemediationPolicy({
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'degraded',
      include_history: true,
      history_limit: 60,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/remediation/policy?action=external_email_send&provider=google&mission_mode=degraded&include_history=1&history_limit=60'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds tts diagnostics query params correctly', async () => {
    await backendClient.ttsDiagnostics({
      history_limit: 18,
      source: 'voice-panel',
      mission_id: 'mission-42',
      risk_level: 'high',
      policy_profile: 'privacy',
      requires_offline: true,
      privacy_mode: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/tts/diagnostics?history_limit=18&source=voice-panel&mission_id=mission-42&risk_level=high&policy_profile=privacy&requires_offline=1&privacy_mode=1'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls local neural tts bridge status endpoint', async () => {
    await backendClient.localNeuralTtsBridgeStatus({ probe: true });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/tts/local-neural/bridge?probe=1');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts local neural tts bridge start payload', async () => {
    await backendClient.startLocalNeuralTtsBridge({
      wait_ready: true,
      timeout_s: 22,
      reason: 'voice_panel',
      force: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/tts/local-neural/bridge/start');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        wait_ready: true,
        timeout_s: 22,
        reason: 'voice_panel',
        force: true,
      })
    );
  });

  it('posts local neural tts bridge probe payload', async () => {
    await backendClient.probeLocalNeuralTtsBridge({ force: true });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/tts/local-neural/bridge/probe');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(JSON.stringify({ force: true }));
  });

  it('calls external connector execution contract status endpoint', async () => {
    await backendClient.externalConnectorExecutionContract({
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'stable',
      include_history: true,
      history_limit: 40,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/execution-contract?action=external_email_send&provider=google&mission_mode=stable&include_history=1&history_limit=40'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls external connector remediation policies endpoint', async () => {
    await backendClient.externalConnectorRemediationPolicies({
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'critical',
      include_history: true,
      history_limit: 80,
      limit: 50,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/connectors/remediation/policies?limit=50&action=external_email_send&provider=google&mission_mode=critical&include_history=1&history_limit=80'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts external connector remediation policy recommendation payload', async () => {
    await backendClient.externalConnectorRemediationPolicyRecommend({
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'stable',
      recent_window: 12,
      baseline_window: 44,
      limit: 300,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/remediation/policy/recommend');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        action: 'external_email_send',
        provider: 'google',
        mission_mode: 'stable',
        recent_window: 12,
        baseline_window: 44,
        limit: 300,
      })
    );
  });

  it('calls external reliability mission policy status endpoint', async () => {
    await backendClient.externalReliabilityMissionPolicyStatus({
      provider_limit: 12,
      history_limit: 18,
      history_window: 24,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/external/reliability/mission-policy?provider_limit=12&history_limit=18&history_window=24'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts external reliability mission policy tune payload', async () => {
    await backendClient.externalReliabilityMissionPolicyTune({
      dry_run: true,
      reason: 'mission_policy_test',
      record_analysis: true,
      tune_provider_policies: false,
      provider_limit: 180,
      history_limit: 36,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/reliability/mission-policy/tune');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        dry_run: true,
        reason: 'mission_policy_test',
        record_analysis: true,
        tune_provider_policies: false,
        provider_limit: 180,
        history_limit: 36,
      })
    );
  });

  it('posts external reliability mission policy config payload', async () => {
    await backendClient.externalReliabilityMissionPolicyConfig({
      config: {
        mission_outage_bias_gain: 0.61,
        mission_outage_profile_hysteresis: 0.12,
      },
      persist_now: true,
      provider_limit: 12,
      history_limit: 18,
      history_window: 24,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/reliability/mission-policy/config');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        config: {
          mission_outage_bias_gain: 0.61,
          mission_outage_profile_hysteresis: 0.12,
        },
        persist_now: true,
        provider_limit: 12,
        history_limit: 18,
        history_window: 24,
      })
    );
  });

  it('posts external reliability mission policy reset payload', async () => {
    await backendClient.externalReliabilityMissionPolicyReset({
      reset_history: true,
      reset_provider_biases: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/reliability/mission-policy/reset');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        reset_history: true,
        reset_provider_biases: true,
      })
    );
  });

  it('posts external connector remediation policy apply payload', async () => {
    await backendClient.externalConnectorRemediationPolicyApply({
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'degraded',
      profile: 'strict',
      controls: {
        allow_high_risk: false,
        max_steps: 4,
        require_compare: true,
        stop_on_blocked: true,
      },
      source: 'desktop-ui',
      reason: 'test-apply',
      use_recommendation: false,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/remediation/policy/apply');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        action: 'external_email_send',
        provider: 'google',
        mission_mode: 'degraded',
        profile: 'strict',
        controls: {
          allow_high_risk: false,
          max_steps: 4,
          require_compare: true,
          stop_on_blocked: true,
        },
        source: 'desktop-ui',
        reason: 'test-apply',
        use_recommendation: false,
      })
    );
  });

  it('posts external connector remediation policy restore payload', async () => {
    await backendClient.externalConnectorRemediationPolicyRestore({
      event_id: 7,
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'stable',
      source: 'desktop-ui',
      reason: 'restore_test',
      dry_run: false,
      force: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/remediation/policy/restore');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        event_id: 7,
        action: 'external_email_send',
        provider: 'google',
        mission_mode: 'stable',
        source: 'desktop-ui',
        reason: 'restore_test',
        dry_run: false,
        force: true,
      })
    );
  });

  it('posts external connector execution contract restore payload', async () => {
    await backendClient.externalConnectorExecutionContractRestore({
      event_id: 9,
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'stable',
      source: 'desktop-ui',
      reason: 'restore_contract_test',
      dry_run: false,
      force: true,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/execution-contract/restore');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        event_id: 9,
        action: 'external_email_send',
        provider: 'google',
        mission_mode: 'stable',
        source: 'desktop-ui',
        reason: 'restore_contract_test',
        dry_run: false,
        force: true,
      })
    );
  });

  it('calls external connector remediation autotune status endpoint', async () => {
    await backendClient.externalConnectorRemediationPolicyAutotuneStatus({ limit: 24 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/remediation/policy/autotune?limit=24');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts external connector remediation autotune payload', async () => {
    await backendClient.externalConnectorRemediationPolicyAutotune({
      action: 'external_email_send',
      provider: 'google',
      mission_mode: 'degraded',
      source: 'desktop-ui',
      reason: 'manual_autotune',
      dry_run: true,
      recent_window: 12,
      baseline_window: 48,
      limit: 220,
      status: 'error',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/remediation/policy/autotune');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        action: 'external_email_send',
        provider: 'google',
        mission_mode: 'degraded',
        source: 'desktop-ui',
        reason: 'manual_autotune',
        dry_run: true,
        recent_window: 12,
        baseline_window: 48,
        limit: 220,
        status: 'error',
      })
    );
  });

  it('posts external connector remediation autotune scan payload', async () => {
    await backendClient.externalConnectorRemediationPolicyAutotuneScan({
      max_pairs: 5,
      mission_mode: 'critical',
      source: 'desktop-ui',
      reason: 'manual_scan',
      dry_run: true,
      limit: 180,
      recent_window: 8,
      baseline_window: 36,
      status: 'error',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/external/connectors/remediation/policy/autotune/scan');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        max_pairs: 5,
        mission_mode: 'critical',
        source: 'desktop-ui',
        reason: 'manual_scan',
        dry_run: true,
        limit: 180,
        recent_window: 8,
        baseline_window: 36,
        status: 'error',
      })
    );
  });

  it('throws backend message on non-ok response', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => ({
        ok: false,
        status: 403,
        text: async () => JSON.stringify({ message: 'blocked by policy' }),
      }))
    );

    await expect(backendClient.executeAction({ action: 'run_script' })).rejects.toThrow('blocked by policy');
  });

  it('builds memory query parameters correctly', async () => {
    await backendClient.memory({ query: 'utc time', limit: 7 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/memory?query=utc+time&limit=7');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds advanced memory query parameters correctly', async () => {
    await backendClient.memory({
      query: 'browser timeout',
      limit: 25,
      mode: 'semantic',
      status: 'failed',
      source: 'desktop-ui',
      min_score: 0.33,
      must_tags: ['action:browser_read_dom', 'status:failed'],
      prefer_tags: ['profile:automation_safe'],
      exclude_goal_ids: ['goal-1', 'goal-2'],
      diversify_by_goal: false,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/memory?query=browser+timeout&limit=25&mode=semantic&status=failed&source=desktop-ui&min_score=0.33&must_tag=action%3Abrowser_read_dom&must_tag=status%3Afailed&prefer_tag=profile%3Aautomation_safe&exclude_goal_id=goal-1&exclude_goal_id=goal-2&diversify_by_goal=0'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds memory strategy query parameters correctly', async () => {
    await backendClient.memoryStrategy({ query: 'email automation', limit: 14, min_score: 0.2 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/memory/strategy?query=email+automation&limit=14&min_score=0.2');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls runtime circuit breakers endpoint', async () => {
    await backendClient.circuitBreakers({ limit: 80 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/runtime/circuit-breakers?limit=80');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds schedules query parameters correctly', async () => {
    await backendClient.schedules({ status: 'pending', limit: 40 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/schedules?status=pending&limit=40');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('builds goals query parameters correctly', async () => {
    await backendClient.goals({ status: 'running', limit: 12 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/goals?status=running&limit=12');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('calls mission diagnostics and mission cancel endpoints', async () => {
    await backendClient.missionDiagnostics('mission-42', { hotspot_limit: 7 });
    await backendClient.cancelMission('mission-42', 'stop mission now');

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][0]).toBe(
      'http://127.0.0.1:8765/missions/mission-42/diagnostics?hotspot_limit=7'
    );
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:8765/missions/mission-42/cancel');
    expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: 'POST' });
    expect((fetchMock.mock.calls[1][1] as RequestInit).body).toBe(
      JSON.stringify({ reason: 'stop mission now' })
    );
  });

  it('posts schedule payload to schedules endpoint', async () => {
    await backendClient.createSchedule({
      text: 'what time is it in UTC',
      run_at: '2026-02-23T16:00:00.000Z',
      max_attempts: 2,
      retry_delay_s: 30,
      repeat_interval_s: 300,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/schedules');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        text: 'what time is it in UTC',
        run_at: '2026-02-23T16:00:00.000Z',
        max_attempts: 2,
        retry_delay_s: 30,
        repeat_interval_s: 300,
      })
    );
  });

  it('posts schedule pause endpoint', async () => {
    await backendClient.pauseSchedule('schedule-1');

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/schedules/schedule-1/pause');
    expect(options).toMatchObject({ method: 'POST' });
  });

  it('posts schedule resume endpoint', async () => {
    await backendClient.resumeSchedule('schedule-1');

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/schedules/schedule-1/resume');
    expect(options).toMatchObject({ method: 'POST' });
  });

  it('posts schedule run-now endpoint', async () => {
    await backendClient.runScheduleNow('schedule-1');

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/schedules/schedule-1/run-now');
    expect(options).toMatchObject({ method: 'POST' });
  });

  it('posts goal cancel endpoint', async () => {
    await backendClient.cancelGoal('goal-9', 'stop now');

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/goals/goal-9/cancel');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(JSON.stringify({ reason: 'stop now' }));
  });

  it('posts plan preview endpoint', async () => {
    await backendClient.previewPlan({ text: 'what time is it in UTC' });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/plans/preview');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(JSON.stringify({ text: 'what time is it in UTC' }));
  });

  it('builds triggers query parameters correctly', async () => {
    await backendClient.triggers({ status: 'active', limit: 15 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/triggers?status=active&limit=15');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts create trigger payload to triggers endpoint', async () => {
    await backendClient.createTrigger({ text: 'system snapshot', interval_s: 120 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/triggers');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(JSON.stringify({ text: 'system snapshot', interval_s: 120 }));
  });

  it('posts trigger pause endpoint', async () => {
    await backendClient.pauseTrigger('trigger-1');

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/triggers/trigger-1/pause');
    expect(options).toMatchObject({ method: 'POST' });
  });

  it('builds macros query parameters correctly', async () => {
    await backendClient.macros({ query: 'time', limit: 6 });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/macros?query=time&limit=6');
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts run macro endpoint', async () => {
    await backendClient.runMacro('macro-1');

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/macros/macro-1/run');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(JSON.stringify({ source: 'desktop-macro' }));
  });

  it('posts run macro with metadata', async () => {
    await backendClient.runMacro('macro-1', 'desktop-macro', { policy_profile: 'interactive' });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/macros/macro-1/run');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({ source: 'desktop-macro', metadata: { policy_profile: 'interactive' } })
    );
  });

  it('posts goal payload with source and metadata', async () => {
    await backendClient.submitGoal('system snapshot', false, {
      source: 'desktop-ui',
      metadata: { policy_profile: 'interactive' },
      timeout_s: 12,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/goals');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(
      JSON.stringify({
        text: 'system snapshot',
        wait: false,
        source: 'desktop-ui',
        metadata: { policy_profile: 'interactive' },
        timeout_s: 12,
      })
    );
  });

  it('posts STT payload to /stt', async () => {
    await backendClient.stt({ duration_s: 5, stt_mode: 'stream', submit_goal: true });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/stt');
    expect(options).toMatchObject({ method: 'POST' });
    expect((options as RequestInit).body).toBe(JSON.stringify({ duration_s: 5, stt_mode: 'stream', submit_goal: true }));
  });

  it('calls voice session control endpoints', async () => {
    await backendClient.voiceSessionStatus();
    await backendClient.voiceRoutePolicyTimeline({ history_limit: 12, force_refresh: true });
    await backendClient.voiceRoutePolicyHistory({ history_limit: 24, task: 'wakeword', status: 'recovery', refresh: true });
    await backendClient.voiceWakewordSupervisionHistory({ history_limit: 36, status: 'hybrid_polling', refresh: true });
    await backendClient.voiceWakewordRestartHistory({ history_limit: 36, event_type: 'restart_backoff', refresh: true });
    await backendClient.voiceWakewordRestartPolicyHistory({ history_limit: 36, refresh: true });
    await backendClient.voiceMissionReliability({ mission_id: 'mission-voice-1', limit: 8 });
    await backendClient.startVoiceSession({ wakeword_enabled: false });
    await backendClient.triggerVoiceSession('manual');
    await backendClient.stopVoiceSession();

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(10);
    expect(fetchMock.mock.calls[0][0]).toBe('http://127.0.0.1:8765/voice/session');
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:8765/voice/route-policy/timeline?history_limit=12&force_refresh=1');
    expect(fetchMock.mock.calls[2][0]).toBe(
      'http://127.0.0.1:8765/voice/route-policy/history?history_limit=24&task=wakeword&status=recovery&refresh=1'
    );
    expect(fetchMock.mock.calls[3][0]).toBe(
      'http://127.0.0.1:8765/voice/wakeword-supervision/history?history_limit=36&status=hybrid_polling&refresh=1'
    );
    expect(fetchMock.mock.calls[4][0]).toBe(
      'http://127.0.0.1:8765/voice/wakeword-restart/history?history_limit=36&event_type=restart_backoff&refresh=1'
    );
    expect(fetchMock.mock.calls[5][0]).toBe(
      'http://127.0.0.1:8765/voice/wakeword-restart/policy-history?history_limit=36&refresh=1'
    );
    expect(fetchMock.mock.calls[6][0]).toBe('http://127.0.0.1:8765/voice/mission-reliability?mission_id=mission-voice-1&limit=8');
    expect(fetchMock.mock.calls[7][0]).toBe('http://127.0.0.1:8765/voice/session/start');
    expect(fetchMock.mock.calls[8][0]).toBe('http://127.0.0.1:8765/voice/session/trigger');
    expect(fetchMock.mock.calls[9][0]).toBe('http://127.0.0.1:8765/voice/session/stop');
  });

  it('calls OAuth flow endpoints', async () => {
    await backendClient.oauthProviders();
    await backendClient.oauthAuthorize({ provider: 'google', scopes: ['email'] });
    await backendClient.oauthFlow('session-123');
    await backendClient.oauthExchange({ session_id: 'session-123', code: 'abc' });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(4);
    expect(fetchMock.mock.calls[0][0]).toBe('http://127.0.0.1:8765/oauth/providers');
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:8765/oauth/authorize');
    expect(fetchMock.mock.calls[2][0]).toBe('http://127.0.0.1:8765/oauth/flows/session-123');
    expect(fetchMock.mock.calls[3][0]).toBe('http://127.0.0.1:8765/oauth/exchange');
  });

  it('builds task list query parameters correctly', async () => {
    await backendClient.tasks({
      provider: 'google',
      query: 'roadmap',
      max_results: 10,
      include_completed: false,
      status: 'not_started',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      'http://127.0.0.1:8765/tasks?provider=google&query=roadmap&status=not_started&max_results=10&include_completed=0'
    );
    expect(options).toMatchObject({ method: 'GET' });
  });

  it('posts create/update task payloads', async () => {
    await backendClient.createTask({
      provider: 'google',
      title: 'Ship desktop wrapper',
      due: '2026-03-05T17:00:00Z',
      status: 'not_started',
    });
    await backendClient.updateTask({
      provider: 'graph',
      task_id: 'task-11',
      status: 'completed',
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][0]).toBe('http://127.0.0.1:8765/tasks/create');
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:8765/tasks/update');
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: 'POST' });
    expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: 'POST' });
  });

  it('posts computer click target payload', async () => {
    await backendClient.computerClickTarget({
      query: 'Submit',
      target_mode: 'auto',
      verify_mode: 'state_or_visibility',
      attempts: 2,
    });

    const fetchMock = globalThis.fetch as unknown as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/computer/click-target');
    expect(options).toMatchObject({ method: 'POST' });
  });
});

