import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { VoiceWakewordSupervisionPanel } from '../src/components/os/voice-wakeword-supervision-panel';

describe('VoiceWakewordSupervisionPanel', () => {
  it('renders wakeword supervision analytics and active run snapshot', () => {
    const html = renderToStaticMarkup(
      <VoiceWakewordSupervisionPanel
        history={{
          count: 4,
          items: [
            {
              event_id: 'wakeword-1',
              occurred_at: '2026-03-08T10:02:00+00:00',
              mission_id: 'mission-voice-1',
              status: 'hybrid_polling',
              previous_status: 'recovery',
              strategy: 'hybrid_polling',
              next_retry_at: '2026-03-08T10:03:00+00:00',
              reason: 'Wakeword route cooling down.',
            },
          ],
          current: {
            status: 'hybrid_polling',
            strategy: 'hybrid_polling',
            allow_wakeword: false,
            restart_delay_s: 8,
            next_retry_at: '2026-03-08T10:03:00+00:00',
            resume_stability_s: 1.2,
            mission_sessions: 5,
            wakeword_gate_events: 4,
            local_voice_pressure_score: 0.71,
            reason: 'Mission recovery history prefers hybrid polling.',
          },
          diagnostics: {
            recovered_events: 1,
            deferred_events: 2,
            timeline_buckets: [
              {
                bucket_start: '2026-03-08T10:00:00+00:00',
                count: 3,
                paused_count: 2,
                active_count: 1,
                recovered_count: 1,
              },
            ],
          },
        }}
        restartHistory={{
          count: 4,
          event_type_filter: 'restart_backoff',
          history_path: 'data/runtime/wakeword_restart_history.jsonl',
          current: {
            exhausted: false,
            next_retry_at: '2026-03-08T10:03:00+00:00',
            exhausted_until: '2026-03-08T10:03:08+00:00',
            last_exhausted_at: '2026-03-08T10:02:52+00:00',
            last_exhaustion_expired_at: '2026-03-08T10:03:08+00:00',
            recovery_expiry_count: 1,
            policy: {
              recent_failures: 2,
              recent_successes: 1,
              long_failures: 4,
              long_successes: 2,
              long_exhaustions: 1,
              long_recoveries: 1,
              consecutive_failures: 1,
              threshold_bias: -1,
              max_failures_before_polling: 3,
              cooldown_scale: 1.8,
              recommended_fallback_interval_s: 2.6,
              recommended_resume_stability_s: 1.4,
              recovery_expiry_s: 8,
            },
          },
          diagnostics: {
            recovered_events: 1,
            exhausted_events: 1,
            recovery_expiry_events: 1,
            exhaustion_transition_count: 1,
            latest_exhausted_at: '2026-03-08T10:02:52+00:00',
            latest_exhausted_until: '2026-03-08T10:03:08+00:00',
            latest_recovery_expiry_at: '2026-03-08T10:03:08+00:00',
            timeline_buckets: [
              {
                bucket_start: '2026-03-08T10:00:00+00:00',
                count: 2,
                failure_count: 2,
                recovered_count: 1,
                exhausted_count: 1,
                expiry_count: 1,
                exhaustion_transition_count: 1,
              },
            ],
          },
          items: [
            {
              event_id: 'wakeword-restart-2',
              occurred_at: '2026-03-08T10:00:15+00:00',
              event_type: 'restart_backoff',
              status: 'degraded:wakeword bootstrap failed',
              failure_count: 1,
              restart_delay_s: 6,
              next_retry_at: '2026-03-08T10:00:21+00:00',
              exhausted_until: '2026-03-08T10:03:08+00:00',
              exhausted: true,
              recovered: false,
              reason: 'wakeword bootstrap failed',
              policy: {
                recovery_expiry_s: 8,
              },
            },
          ],
        }}
        restartPolicyHistory={{
          count: 4,
          history_path: 'data/runtime/wakeword_restart_policy_history.jsonl',
          current: {
            threshold_bias: -1,
            max_failures_before_polling: 3,
            cooldown_scale: 1.8,
            recovery_credit: 0.42,
            fallback_interval_s: 2.6,
            resume_stability_s: 1.4,
            wakeword_sensitivity: 0.58,
            polling_bias: 0.31,
            restart_delay_s: 6,
          },
          diagnostics: {
            avg_threshold_bias: -0.5,
            avg_cooldown_scale: 1.7,
            avg_recovery_credit: 0.36,
            latest_recorded_at: '2026-03-08T10:03:08+00:00',
            drift_score: 0.22,
            recommended_profile: 'recovered_wakeword',
            profile_action: 'recover',
            profile_reason: 'Sustained recovery allows wakeword policy pressure to relax.',
            profile_transition_count: 1,
            profile_timeline: [
              {
                bucket_start: '2026-03-08T10:00:00+00:00',
                recommended_profile: 'recovered_wakeword',
                profile_action: 'recover',
                drift_score: 0.22,
                exhausted_count: 0,
                recovered_count: 1,
              },
            ],
            timeline_buckets: [
              {
                bucket_start: '2026-03-08T10:00:00+00:00',
                count: 2,
                avg_threshold_bias: -0.5,
                avg_cooldown_scale: 1.7,
                avg_recovery_credit: 0.36,
                avg_fallback_interval_s: 2.5,
                avg_resume_stability_s: 1.35,
                avg_wakeword_sensitivity: 0.59,
              },
            ],
          },
        }}
        activeRun={{
          session_id: 'voice-cont-9',
          result: {
            wakeword_supervision_snapshot: {
              status: 'hybrid_polling',
              strategy: 'hybrid_polling',
              allow_wakeword: false,
            },
          },
        }}
        formatDateTime={(value) => String(value)}
        formatStatus={(value) => String(value)}
      />
    );

    expect(html).toContain('Wakeword Supervision');
    expect(html).toContain('Mission recovery history prefers hybrid polling.');
    expect(html).toContain('voice-cont-9');
    expect(html).toContain('Hourly Supervision Drift');
    expect(html).toContain('Recent Supervision Events');
    expect(html).toContain('Wakeword Restart Timeline');
    expect(html).toContain('max failures before polling');
    expect(html).toContain('Exhaustion Transitions');
    expect(html).toContain('Recovery Expiry');
    expect(html).toContain('policy expiry window');
    expect(html).toContain('Restored Runtime Tuning');
    expect(html).toContain('Restart Policy Drift');
    expect(html).toContain('Restart Profile Trend');
    expect(html).toContain('Restart Policy Timeline');
    expect(html).toContain('profile:recovered_wakeword');
    expect(html).toContain('action:recover');
    expect(html).toContain('Refresh Policy Drift');
    expect(html).toContain('restart_backoff');
  });
});
