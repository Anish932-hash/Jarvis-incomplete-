import React from 'react';
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { VoiceWakewordSupervisionPanel } from '../src/components/os/voice-wakeword-supervision-panel';

afterEach(() => {
  cleanup();
});

function buildProps(restartHistory: Record<string, unknown>, restartPolicyHistory?: Record<string, unknown>) {
  return {
    history: {
      count: 2,
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
      },
      diagnostics: {
        recovered_events: 1,
        deferred_events: 1,
        timeline_buckets: [],
      },
    },
    restartHistory,
    restartPolicyHistory,
    activeRun: {
      session_id: 'voice-cont-11',
      result: {
        wakeword_supervision_snapshot: {
          status: 'hybrid_polling',
          strategy: 'hybrid_polling',
          allow_wakeword: false,
        },
      },
    },
    formatDateTime: (value: unknown) => String(value),
    formatStatus: (value: unknown) => String(value),
  };
}

describe('VoiceWakewordSupervisionPanel DOM', () => {
  it('updates exhaustion and recovery expiry views when restart history changes', () => {
    const exhaustedHistory = {
      count: 3,
      history_path: 'data/runtime/wakeword_restart_history.jsonl',
      current: {
        exhausted: true,
        next_retry_at: '2026-03-08T10:03:00+00:00',
        exhausted_until: '2026-03-08T10:03:08+00:00',
        recovery_expiry_count: 0,
        policy: {
          recent_failures: 3,
          recent_successes: 0,
          max_failures_before_polling: 3,
          cooldown_scale: 2.4,
          recommended_fallback_interval_s: 3.1,
          recommended_resume_stability_s: 1.9,
          recovery_expiry_s: 8,
        },
      },
      diagnostics: {
        exhausted_events: 1,
        recovered_events: 0,
        recovery_expiry_events: 0,
        exhaustion_transition_count: 1,
        latest_exhausted_at: '2026-03-08T10:02:52+00:00',
        latest_exhausted_until: '2026-03-08T10:03:08+00:00',
        latest_recovery_expiry_at: '',
        timeline_buckets: [
          {
            bucket_start: '2026-03-08T10:00:00+00:00',
            count: 2,
            failure_count: 2,
            recovered_count: 0,
            exhausted_count: 1,
            expiry_count: 0,
            exhaustion_transition_count: 1,
          },
        ],
      },
      items: [
        {
          event_id: 'wakeword-restart-2',
          occurred_at: '2026-03-08T10:00:15+00:00',
          event_type: 'restart_exhausted',
          status: 'degraded:wakeword bootstrap failed',
          failure_count: 3,
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
    };

    const { rerender } = render(<VoiceWakewordSupervisionPanel {...buildProps(exhaustedHistory)} />);

    expect(screen.getByText('Exhaustion Transitions')).toBeTruthy();
    expect(screen.getByText(/transitions: 1/i)).toBeTruthy();
    expect(screen.getByText(/expiry events: 0/i)).toBeTruthy();
    expect(screen.getAllByText(/2026-03-08T10:03:08\+00:00/i).length).toBeGreaterThan(0);

    const recoveredHistory = {
      ...exhaustedHistory,
      current: {
        ...(exhaustedHistory.current as Record<string, unknown>),
        exhausted: false,
        exhausted_until: '',
        recovery_expiry_count: 1,
      },
      diagnostics: {
        ...(exhaustedHistory.diagnostics as Record<string, unknown>),
        recovered_events: 1,
        recovery_expiry_events: 1,
        latest_recovery_expiry_at: '2026-03-08T10:03:08+00:00',
      },
      items: [
        ...(exhaustedHistory.items as Array<Record<string, unknown>>),
        {
          event_id: 'wakeword-restart-3',
          occurred_at: '2026-03-08T10:03:08+00:00',
          event_type: 'restart_exhaustion_expired',
          status: 'recovery:mission_recovery_policy',
          failure_count: 0,
          restart_delay_s: 0,
          next_retry_at: '2026-03-08T10:03:08+00:00',
          exhausted_until: '2026-03-08T10:03:08+00:00',
          exhausted: false,
          recovered: true,
          reason: 'Wakeword restart exhaustion recovery window elapsed.',
          policy: {
            recovery_expiry_s: 8,
          },
        },
      ],
    };

    rerender(<VoiceWakewordSupervisionPanel {...buildProps(recoveredHistory)} />);

    expect(screen.getByText(/expiry events: 1/i)).toBeTruthy();
    expect(screen.getByText(/recovered: yes/i)).toBeTruthy();
    expect(screen.getAllByText(/restart_exhaustion_expired/i).length).toBeGreaterThan(0);
  });

  it('renders restored runtime tuning and restart policy drift from policy history', () => {
    const restartHistory = {
      count: 1,
      history_path: 'data/runtime/wakeword_restart_history.jsonl',
      current: {
        exhausted: false,
        next_retry_at: '',
        exhausted_until: '',
        policy: {
          max_failures_before_polling: 4,
          cooldown_scale: 1.6,
          recommended_fallback_interval_s: 2.2,
          recommended_resume_stability_s: 1.1,
          recovery_expiry_s: 6,
        },
      },
      diagnostics: {
        exhausted_events: 0,
        recovered_events: 1,
        recovery_expiry_events: 0,
        exhaustion_transition_count: 0,
        timeline_buckets: [],
      },
      items: [],
    };
    const restartPolicyHistory = {
      count: 2,
      history_path: 'data/runtime/wakeword_restart_policy_history.jsonl',
      current: {
        threshold_bias: -1,
        max_failures_before_polling: 4,
        cooldown_scale: 1.6,
        recovery_credit: 0.44,
        fallback_interval_s: 2.2,
        resume_stability_s: 1.1,
        wakeword_sensitivity: 0.61,
        polling_bias: 0.25,
        restart_delay_s: 4.5,
      },
      diagnostics: {
        avg_threshold_bias: -0.5,
        avg_cooldown_scale: 1.55,
        avg_recovery_credit: 0.4,
        latest_recorded_at: '2026-03-08T10:03:08+00:00',
        drift_score: 0.24,
        recommended_profile: 'recovered_wakeword',
        profile_action: 'recover',
        profile_reason: 'Sustained recovery allows wakeword policy pressure to relax.',
        profile_transition_count: 1,
        profile_timeline: [
          {
            bucket_start: '2026-03-08T10:00:00+00:00',
            recommended_profile: 'recovered_wakeword',
            profile_action: 'recover',
            drift_score: 0.24,
            exhausted_count: 0,
            recovered_count: 1,
          },
        ],
        timeline_buckets: [
          {
            bucket_start: '2026-03-08T10:00:00+00:00',
            count: 2,
            avg_threshold_bias: -0.5,
            avg_cooldown_scale: 1.55,
            avg_recovery_credit: 0.4,
            avg_fallback_interval_s: 2.3,
            avg_resume_stability_s: 1.15,
            avg_wakeword_sensitivity: 0.6,
          },
        ],
      },
    };

    render(<VoiceWakewordSupervisionPanel {...buildProps(restartHistory, restartPolicyHistory)} />);

    expect(screen.getByText('Restored Runtime Tuning')).toBeTruthy();
    expect(screen.getByText('Restart Policy Drift')).toBeTruthy();
    expect(screen.getByText('Restart Profile Trend')).toBeTruthy();
    expect(screen.getByText('Restart Policy Timeline')).toBeTruthy();
    expect(screen.getByText(/wakeword sensitivity: 0.61/i)).toBeTruthy();
    expect(screen.getByText(/avg threshold bias: -0.5/i)).toBeTruthy();
    expect(screen.getByText(/current profile: recovered_wakeword/i)).toBeTruthy();
    expect(screen.getByRole('button', { name: /refresh policy drift/i })).toBeTruthy();
  });
});
