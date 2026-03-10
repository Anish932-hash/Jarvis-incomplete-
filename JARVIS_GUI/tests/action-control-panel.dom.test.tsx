import React from 'react';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ActionControlPanel from '../src/components/os/action-control-panel';
import { backendClient } from '../src/lib/backend-client';

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe('ActionControlPanel', () => {
  it('opens and renders the restored actions, voice, and runtime sections', async () => {
    window.localStorage.setItem(
      'jarvis.action_control_panel.state.v2',
      JSON.stringify({
        operatorQuery: 'mission-1',
        activeTab: 'run',
        selectedAction: 'time_now',
        actionArgsText: '{}',
        actionMetadataText: '{}',
        goalTimeoutS: 45,
        voiceDurationS: 45,
        voiceMaxTurns: 3,
        voiceIdleS: 10,
        voiceStopAfter: true,
        modelTaskFilter: 'all',
      })
    );

    vi.spyOn(backendClient, 'health').mockResolvedValue({ status: 'ok' });
    vi.spyOn(backendClient, 'tools').mockResolvedValue({ tools: ['time_now', 'system_snapshot'] });
    vi.spyOn(backendClient, 'approvals').mockResolvedValue({ items: [], count: 0 });
    vi.spyOn(backendClient, 'goals').mockResolvedValue({
      items: [
        {
          goal_id: 'goal-1',
          status: 'pending',
          source: 'test',
          text: 'Draft a report',
          created_at: '2026-03-08T10:00:00Z',
        },
      ],
      count: 1,
    });
    vi.spyOn(backendClient, 'missions').mockResolvedValue({
      status: 'ok',
      items: [
        {
          mission_id: 'mission-1',
          root_goal_id: 'goal-1',
          latest_goal_id: 'goal-1',
          text: 'Draft a report',
          source: 'test',
          metadata: {},
          status: 'running',
          created_at: '2026-03-08T10:00:00Z',
          updated_at: '2026-03-08T10:05:00Z',
        },
      ],
      count: 1,
    });
    vi.spyOn(backendClient, 'missionTimeline').mockResolvedValue({
      status: 'ok',
      mission_id: 'mission-1',
      items: [],
      count: 0,
      total: 0,
    });
    vi.spyOn(backendClient, 'missionDiagnostics').mockResolvedValue({ status: 'ok', mission_id: 'mission-1' });
    vi.spyOn(backendClient, 'missionResumePreview').mockResolvedValue({ status: 'ok', remaining_steps: 2 });
    vi.spyOn(backendClient, 'voiceDiagnostics').mockResolvedValue({
      status: 'ok',
      voice: {
        running: false,
        wakeword_status: 'recovery:policy_guard',
        route_policy_block_count: 1,
        route_policy_reroute_count: 2,
        route_policy_recovery_count: 3,
      },
      route_policy_timeline: {
        status: 'ok',
        items: [
          {
            event_id: 'voice-route-1',
            occurred_at: '2026-03-08T10:03:00Z',
            task: 'wakeword',
            previous_status: 'blocked',
            status: 'recovery',
            selected_provider: 'local',
            recommended_provider: 'local',
            reason_code: 'wakeword_recovery_guard',
            next_retry_at: '2026-03-08T10:05:00Z',
          },
        ],
        current: {
          summary: {
            status: 'recovery',
            reason_code: 'wakeword_recovery_guard',
            next_retry_at: '2026-03-08T10:05:00Z',
          },
          stt: { status: 'rerouted', selected_provider: 'groq', recommended_provider: 'groq' },
          wakeword: {
            status: 'recovery',
            selected_provider: 'local',
            reason_code: 'wakeword_recovery_guard',
            next_retry_at: '2026-03-08T10:05:00Z',
          },
          tts: { status: 'stable', selected_provider: 'elevenlabs', recommended_provider: 'elevenlabs' },
        },
      },
      route_policy_history: {
        status: 'ok',
        items: [{ event_id: 'voice-history-1', occurred_at: '2026-03-08T10:00:00Z', status: 'blocked' }],
        diagnostics: {
          timeline_buckets: [{ bucket_start: '2026-03-08T10:00:00Z', count: 1, paused_count: 1 }],
        },
      },
      wakeword_supervision_history: {
        status: 'ok',
        count: 1,
        items: [{ event_id: 'wakeword-history-1', occurred_at: '2026-03-08T10:02:00Z', status: 'recovery' }],
        current: {
          allow_wakeword: false,
          restart_delay_s: 3.5,
          wakeword_gate_events: 2,
          fallback_interval_s: 9.0,
          resume_stability_s: 1.8,
        },
        diagnostics: {
          timeline_buckets: [{ bucket_start: '2026-03-08T10:00:00Z', count: 1, blocked_count: 1 }],
        },
      },
      wakeword_restart_history: {
        status: 'ok',
        count: 2,
        items: [
          {
            event_id: 'wakeword-restart-1',
            occurred_at: '2026-03-08T10:01:00Z',
            event_type: 'restart_backoff',
            status: 'degraded:wakeword bootstrap failed',
            restart_delay_s: 8.0,
            next_retry_at: '2026-03-08T10:05:00Z',
            exhausted_until: '2026-03-08T10:07:00Z',
            policy: { recovery_expiry_s: 12.0 },
          },
        ],
        current: {
          exhausted: true,
          next_retry_at: '2026-03-08T10:05:00Z',
          exhausted_until: '2026-03-08T10:07:00Z',
          recovery_expiry_count: 1,
          policy: {
            recent_failures: 2,
            recent_successes: 1,
            consecutive_failures: 2,
            recovery_credit: 0.88,
            max_failures_before_polling: 4,
            cooldown_scale: 2.4,
            recommended_fallback_interval_s: 8.0,
            recommended_delay_decay_factor: 0.62,
            recommended_backoff_relaxation: 1,
            recovery_expiry_s: 12.0,
            recommended_resume_stability_s: 1.8,
          },
        },
        diagnostics: {
          recovered_events: 1,
          exhausted_events: 1,
          exhaustion_transition_count: 1,
          latest_exhausted_at: '2026-03-08T10:01:00Z',
          latest_exhausted_until: '2026-03-08T10:07:00Z',
          recovery_expiry_events: 1,
          latest_recovery_expiry_at: '2026-03-08T10:09:00Z',
          timeline_buckets: [
            {
              bucket_start: '2026-03-08T10:00:00Z',
              count: 2,
              exhausted_count: 1,
              exhaustion_transition_count: 1,
            },
          ],
        },
      },
      wakeword_restart_policy_history: {
        status: 'ok',
        count: 2,
        items: [
          {
            event_id: 'wakeword-policy-1',
            occurred_at: '2026-03-08T10:01:00Z',
            threshold_bias: -1,
            cooldown_scale: 2.4,
            recovery_credit: 0.24,
            fallback_interval_s: 8.0,
            resume_stability_s: 1.8,
            wakeword_sensitivity: 0.58,
            polling_bias: 0.32,
          },
        ],
        current: {
          threshold_bias: -1,
          max_failures_before_polling: 4,
          cooldown_scale: 2.4,
          recovery_credit: 0.88,
          fallback_interval_s: 8.0,
          resume_stability_s: 1.8,
          wakeword_sensitivity: 0.58,
          polling_bias: 0.32,
          restart_delay_s: 8.0,
        },
        diagnostics: {
          avg_threshold_bias: -0.5,
          avg_cooldown_scale: 2.1,
          avg_recovery_credit: 0.72,
          latest_recorded_at: '2026-03-08T10:09:00Z',
          drift_score: 0.22,
          recommended_profile: 'recovered_wakeword',
          profile_action: 'recover',
          profile_reason: 'Sustained recovery allows wakeword policy pressure to relax.',
          profile_transition_count: 1,
          profile_shift_timeline: [
            {
              bucket_start: '2026-03-08T10:00:00Z',
              from_profile: 'hybrid_guarded',
              to_profile: 'recovered_wakeword',
              profile_action: 'recover',
              drift_score: 0.22,
            },
          ],
          runtime_posture: {
            runtime_mode: 'recovered_wakeword',
            wakeword_supervision_mode: 'recovered_wakeword',
            continuous_resume_mode: 'resume_ready',
            barge_in_enabled: true,
            hard_barge_in: true,
          },
          profile_timeline: [
            {
              bucket_start: '2026-03-08T10:00:00Z',
              recommended_profile: 'recovered_wakeword',
              profile_action: 'recover',
              drift_score: 0.22,
              exhausted_count: 0,
              recovered_count: 1,
            },
          ],
          timeline_buckets: [
            {
              bucket_start: '2026-03-08T10:00:00Z',
              count: 2,
              avg_threshold_bias: -0.5,
              avg_cooldown_scale: 2.1,
              avg_recovery_credit: 0.72,
              avg_fallback_interval_s: 8.0,
              avg_resume_stability_s: 1.8,
              avg_wakeword_sensitivity: 0.58,
            },
          ],
        },
      },
      mission_reliability: { status: 'ok', items: [{ mission_id: 'mission-1', route_recovery_score: 0.72 }] },
      route_recovery_recommendation: { status: 'ok', strategy: 'hybrid_polling' },
      tts: {
        status: 'ok',
        providers: {
          local: { status: 'ready' },
          elevenlabs: { status: 'ready' },
          neural_local: { configured: true, ready: true, execution_backend: 'bridge', model_exists: true },
        },
        route_policy: {
          route_blocked: false,
          route_adjusted: true,
          selected_provider: 'elevenlabs',
          selected_model: 'eleven_multilingual_v2',
          route_warning: 'local_degraded',
          blacklisted: false,
          recommended_provider: 'elevenlabs',
          local_route_viable: false,
          autonomy_safe: true,
          review_required: false,
          cooldown_hint_s: 6,
          reason_code: 'local_degraded',
          fallback_candidates: ['elevenlabs', 'local'],
        },
      },
      stt: {
        status: 'ok',
        route_policy: {
          route_blocked: false,
          route_adjusted: true,
          selected_provider: 'groq',
          selected_model: 'whisper-large-v3',
          blacklisted: false,
          recommended_provider: 'groq',
          local_route_viable: false,
          autonomy_safe: true,
          review_required: false,
          reason_code: 'local_blacklisted',
          fallback_candidates: ['groq', 'local'],
        },
        policy_snapshot: {
          provider_health: 'degraded',
          fallback_rate_ema: 0.44,
          success_rate: 0.86,
          provider_failure_streak_threshold: 3,
          recommended_provider: 'groq',
        },
      },
    });
    vi.spyOn(backendClient, 'voiceSessionStatus').mockResolvedValue({ running: false, wakeword_status: 'idle' });
    vi.spyOn(backendClient, 'listVoiceContinuousRuns').mockResolvedValue({ status: 'ok', items: [], count: 0 });
    vi.spyOn(backendClient, 'voiceRoutePolicyTimeline').mockResolvedValue({
      status: 'ok',
      items: [],
      current: { summary: { status: 'recovery' } },
    });
    vi.spyOn(backendClient, 'voiceRoutePolicyHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
    });
    vi.spyOn(backendClient, 'voiceWakewordSupervisionHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
    });
    const wakewordRestartHistorySpy = vi.spyOn(backendClient, 'voiceWakewordRestartHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
      current: {},
    });
    const wakewordRestartPolicyHistorySpy = vi.spyOn(backendClient, 'voiceWakewordRestartPolicyHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
      current: {},
    });
    vi.spyOn(backendClient, 'voiceMissionReliability').mockResolvedValue({
      status: 'ok',
      items: [],
      count: 0,
    });
    vi.spyOn(backendClient, 'modelOperationsSummary').mockResolvedValue({
      status: 'ok',
      stack_name: 'default',
      mission_profile: 'desktop',
    });
    vi.spyOn(backendClient, 'runtimeHealthSummary').mockResolvedValue({ status: 'ok', score: 91, alerts: [] });
    vi.spyOn(backendClient, 'modelsRuntimeSupervisors').mockResolvedValue({
      status: 'ok',
      reasoning: { candidate_count: 2 },
    });
    vi.spyOn(backendClient, 'modelsLocalInventory').mockResolvedValue({
      status: 'ok',
      inventory: { items: [{ task: 'reasoning', name: 'Reasoning Alpha', path: 'E:/reasoning/model.gguf' }] },
    });
    vi.spyOn(backendClient, 'modelBridgeProfiles').mockResolvedValue({
      status: 'ok',
      profiles: [
        {
          profile_id: 'reasoning-1',
          bridge_kind: 'reasoning',
          task: 'reasoning',
          name: 'Reasoning Alpha',
          apply_supported: true,
          launch_templates: [{ template_id: 'tmpl-1', launcher: 'llama', ready: true }],
        },
      ],
    });
    vi.spyOn(backendClient, 'modelsRouteBundle').mockResolvedValue({
      status: 'ok',
      items: [{ task: 'reasoning', status: 'success', provider: 'local', model: 'Reasoning Alpha' }],
    });

    const user = userEvent.setup();

    render(<ActionControlPanel trigger={<button type="button">Open Panel</button>} />);

    await user.click(screen.getByRole('button', { name: 'Open Panel' }));

    expect(await screen.findByText('Tool Actions')).toBeTruthy();
    expect(screen.getByRole('tab', { name: /run actions/i }).getAttribute('data-state')).toBe('active');
    expect(await screen.findByText('time_now')).toBeTruthy();

    await user.click(screen.getByRole('tab', { name: /voice/i }));
    expect(await screen.findByText('Voice Runtime State')).toBeTruthy();
    expect(await screen.findByText('Cross-Session Wakeword Policy')).toBeTruthy();
    expect(await screen.findByText('Voice Route Gate Timeline')).toBeTruthy();
    expect(await screen.findByText('Wakeword Supervision')).toBeTruthy();
    expect(await screen.findByText('Wakeword Restart Timeline')).toBeTruthy();
    expect(await screen.findByText('Restored Runtime Tuning')).toBeTruthy();
    expect(await screen.findByText('Restart Policy Drift')).toBeTruthy();
    expect(await screen.findByText('Restart Profile Trend')).toBeTruthy();
    expect(await screen.findByText('Runtime Mode Overlay')).toBeTruthy();
    expect(await screen.findByText('Profile Shift Timeline')).toBeTruthy();
    expect(await screen.findByText((content) => /hybrid_guarded/i.test(content) && /recovered_wakeword/i.test(content))).toBeTruthy();
    expect(await screen.findByText(/profile reason:/i)).toBeTruthy();
    expect(await screen.findByText('Recovery Expiry')).toBeTruthy();
    expect(await screen.findByText('TTS Diagnostics')).toBeTruthy();
    expect(await screen.findByText('STT Diagnostics')).toBeTruthy();
    expect(await screen.findByText('STT Policy Snapshot')).toBeTruthy();
    expect(await screen.findByText('Voice Diagnostics (Raw)')).toBeTruthy();
    const restartPolicyHistoryCalls = wakewordRestartPolicyHistorySpy.mock.calls.length;
    await user.click(screen.getByRole('button', { name: /refresh cross-session policy/i }));
    await waitFor(() => {
      expect(wakewordRestartPolicyHistorySpy.mock.calls.length).toBeGreaterThan(restartPolicyHistoryCalls);
    });

    await user.click(screen.getByRole('tab', { name: /runtime/i }));
    expect(await screen.findByText('Unified Runtime Health')).toBeTruthy();
    expect(await screen.findByText('Reasoning Bridge Profiles')).toBeTruthy();

    await waitFor(() => {
      expect(backendClient.tools).toHaveBeenCalled();
      expect(backendClient.voiceDiagnostics).toHaveBeenCalled();
    });
  }, 20000);

  it('runs continuous voice actions from the main panel and shows restart history details', async () => {
    vi.stubGlobal(
      'ResizeObserver',
      class ResizeObserver {
        observe() {}
        unobserve() {}
        disconnect() {}
      }
    );
    vi.spyOn(backendClient, 'health').mockResolvedValue({ status: 'ok' });
    vi.spyOn(backendClient, 'tools').mockResolvedValue({ tools: ['time_now'] });
    vi.spyOn(backendClient, 'approvals').mockResolvedValue({ items: [], count: 0 });
    vi.spyOn(backendClient, 'goals').mockResolvedValue({ items: [], count: 0 });
    vi.spyOn(backendClient, 'missions').mockResolvedValue({ status: 'ok', items: [], count: 0 });
    vi.spyOn(backendClient, 'missionTimeline').mockResolvedValue({
      status: 'ok',
      mission_id: 'mission-voice-44',
      items: [],
      count: 0,
      total: 0,
    });
    vi.spyOn(backendClient, 'missionDiagnostics').mockResolvedValue({ status: 'ok', mission_id: 'mission-voice-44' });
    vi.spyOn(backendClient, 'missionResumePreview').mockResolvedValue({ status: 'ok', remaining_steps: 0 });
    vi.spyOn(backendClient, 'voiceDiagnostics').mockResolvedValue({
      status: 'ok',
      voice: {
        running: true,
        wakeword_status: 'recovery:policy_guard',
        route_policy_block_count: 1,
        route_policy_reroute_count: 1,
        route_policy_recovery_count: 2,
      },
      route_policy_timeline: {
        status: 'ok',
        items: [],
        current: {
          summary: { status: 'recovery', reason_code: 'wakeword_recovery_guard' },
          stt: { status: 'rerouted', selected_provider: 'groq', recommended_provider: 'groq' },
          wakeword: { status: 'recovery', selected_provider: 'local', reason_code: 'wakeword_recovery_guard' },
          tts: { status: 'stable', selected_provider: 'elevenlabs', recommended_provider: 'elevenlabs' },
        },
      },
      route_policy_history: { status: 'ok', items: [], diagnostics: {} },
      wakeword_supervision_history: {
        status: 'ok',
        count: 1,
        items: [{ event_id: 'wakeword-history-1', occurred_at: '2026-03-08T10:02:00Z', status: 'recovery' }],
        current: {
          allow_wakeword: false,
          restart_delay_s: 3.5,
          wakeword_gate_events: 2,
          fallback_interval_s: 9.0,
          resume_stability_s: 1.8,
        },
        diagnostics: {},
      },
      wakeword_restart_history: {
        status: 'ok',
        count: 2,
        items: [
          {
            event_id: 'wakeword-restart-1',
            occurred_at: '2026-03-08T10:01:00Z',
            event_type: 'restart_backoff',
            status: 'degraded:wakeword bootstrap failed',
            reason_code: 'wakeword_start_failed',
            reason: 'wakeword bootstrap failed',
            restart_delay_s: 8.0,
            next_retry_at: '2026-03-08T10:05:00Z',
            exhausted_until: '2026-03-08T10:07:00Z',
            failure_count: 2,
            recovered: false,
            exhausted: true,
            policy: { recovery_expiry_s: 12.0 },
          },
        ],
        current: {
          exhausted: true,
          next_retry_at: '2026-03-08T10:05:00Z',
          exhausted_until: '2026-03-08T10:07:00Z',
          recovery_expiry_count: 1,
          policy: {
            recent_failures: 2,
            recent_successes: 1,
            consecutive_failures: 2,
            recovery_credit: 0.88,
            max_failures_before_polling: 4,
            cooldown_scale: 2.4,
            recommended_fallback_interval_s: 8.0,
            recommended_delay_decay_factor: 0.62,
            recommended_backoff_relaxation: 1,
            recovery_expiry_s: 12.0,
            recommended_resume_stability_s: 1.8,
          },
        },
        diagnostics: {
          recovered_events: 1,
          exhausted_events: 1,
          exhaustion_transition_count: 1,
          latest_exhausted_at: '2026-03-08T10:01:00Z',
          latest_exhausted_until: '2026-03-08T10:07:00Z',
          recovery_expiry_events: 1,
          latest_recovery_expiry_at: '2026-03-08T10:09:00Z',
          timeline_buckets: [],
        },
      },
      wakeword_restart_policy_history: {
        status: 'ok',
        count: 2,
        items: [],
        current: {
          threshold_bias: -1,
          max_failures_before_polling: 4,
          cooldown_scale: 2.4,
          recovery_credit: 0.88,
          fallback_interval_s: 8.0,
          resume_stability_s: 1.8,
          wakeword_sensitivity: 0.58,
          polling_bias: 0.32,
          restart_delay_s: 8.0,
        },
        diagnostics: {
          avg_threshold_bias: -0.5,
          avg_cooldown_scale: 2.1,
          avg_recovery_credit: 0.72,
          latest_recorded_at: '2026-03-08T10:09:00Z',
          timeline_buckets: [],
        },
      },
      mission_reliability: { status: 'ok', items: [{ mission_id: 'mission-voice-44', route_recovery_score: 0.61 }] },
      route_recovery_recommendation: { status: 'ok', strategy: 'hybrid_polling' },
      tts: { status: 'ok', providers: { local: { status: 'ready' }, elevenlabs: { status: 'ready' } } },
      stt: { status: 'ok', policy_snapshot: { recommended_provider: 'groq' }, route_policy: { selected_provider: 'groq' } },
    });
    vi.spyOn(backendClient, 'voiceSessionStatus').mockResolvedValue({
      running: true,
      wakeword_status: 'recovery:policy_guard',
    });
    vi.spyOn(backendClient, 'voiceRoutePolicyTimeline').mockResolvedValue({
      status: 'ok',
      items: [],
      current: { summary: { status: 'recovery' } },
    });
    vi.spyOn(backendClient, 'voiceRoutePolicyHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
    });
    vi.spyOn(backendClient, 'voiceWakewordSupervisionHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
    });
    const wakewordRestartHistorySpy = vi.spyOn(backendClient, 'voiceWakewordRestartHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
      current: {},
    });
    const wakewordRestartPolicyHistorySpy = vi.spyOn(backendClient, 'voiceWakewordRestartPolicyHistory').mockResolvedValue({
      status: 'ok',
      items: [],
      diagnostics: {},
      count: 0,
      current: {},
    });
    vi.spyOn(backendClient, 'voiceMissionReliability').mockResolvedValue({
      status: 'ok',
      items: [],
      count: 0,
    });
    vi.spyOn(backendClient, 'listVoiceContinuousRuns').mockResolvedValue({
      status: 'ok',
      count: 1,
      items: [
        {
          session_id: 'voice-cont-44',
          status: 'paused',
          created_at: '2026-03-08T10:10:00+00:00',
          result: {
            mission_reliability: {
              current: {
                mission_id: 'mission-voice-44',
              },
            },
            route_policy_recovery_decision: {
              resume_allowed: false,
              primary_reason: 'mission_recovery_score_too_low',
            },
            next_retry_at: '2026-03-08T10:10:07+00:00',
          },
        },
      ],
    });
    const startContinuousSpy = vi
      .spyOn(backendClient, 'startVoiceContinuousSession')
      .mockResolvedValue({ status: 'success', session_id: 'voice-cont-99' });
    const cancelContinuousSpy = vi
      .spyOn(backendClient, 'cancelVoiceContinuousSession')
      .mockResolvedValue({ status: 'success', session_id: 'voice-cont-44' });
    vi.spyOn(backendClient, 'modelOperationsSummary').mockResolvedValue({
      status: 'ok',
      stack_name: 'default',
      mission_profile: 'desktop',
    });
    vi.spyOn(backendClient, 'runtimeHealthSummary').mockResolvedValue({ status: 'ok', score: 91, alerts: [] });
    vi.spyOn(backendClient, 'modelsRuntimeSupervisors').mockResolvedValue({
      status: 'ok',
      reasoning: { candidate_count: 2 },
    });
    vi.spyOn(backendClient, 'modelsLocalInventory').mockResolvedValue({
      status: 'ok',
      inventory: { items: [] },
    });
    vi.spyOn(backendClient, 'modelBridgeProfiles').mockResolvedValue({
      status: 'ok',
      profiles: [],
    });
    vi.spyOn(backendClient, 'modelsRouteBundle').mockResolvedValue({
      status: 'ok',
      items: [],
    });

    const user = userEvent.setup();

    render(<ActionControlPanel trigger={<button type="button">Open Panel</button>} />);

    await user.click(screen.getByRole('button', { name: 'Open Panel' }));
    await user.click(screen.getByRole('tab', { name: /voice/i }));

    expect(await screen.findByText('Recent Restart Events')).toBeTruthy();
    expect(await screen.findByText(/wakeword bootstrap failed/i)).toBeTruthy();

    await user.selectOptions(screen.getByLabelText(/wakeword restart event filter/i), 'restart_backoff');
    await user.click(screen.getByRole('button', { name: /refresh restart history/i }));
    await user.click(screen.getByRole('button', { name: /refresh policy drift/i }));

    await user.click(screen.getByTestId('voice-continuous-start'));
    await user.click(screen.getByTestId('voice-continuous-cancel'));

    await waitFor(() => {
      expect(startContinuousSpy).toHaveBeenCalledTimes(1);
      expect(cancelContinuousSpy).toHaveBeenCalledWith('voice-cont-44', { reason: 'operator-stop' });
      expect(wakewordRestartHistorySpy).toHaveBeenCalledWith({
        history_limit: 160,
        event_type: 'restart_backoff',
        refresh: true,
      });
      expect(wakewordRestartPolicyHistorySpy).toHaveBeenCalledWith({
        history_limit: 160,
        refresh: true,
      });
    });
  }, 40000);
});
