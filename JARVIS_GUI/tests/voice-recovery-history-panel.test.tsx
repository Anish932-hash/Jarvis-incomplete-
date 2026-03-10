import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { VoiceRecoveryHistoryPanel } from '../src/components/os/voice-recovery-history-panel';

describe('VoiceRecoveryHistoryPanel', () => {
  it('renders recovery analytics, mission reliability, and adaptive strategy', () => {
    const html = renderToStaticMarkup(
      <VoiceRecoveryHistoryPanel
        diagnostics={{
          count: 6,
          blocked_events: 2,
          rerouted_events: 3,
          recovered_events: 2,
          latest_event_at: '2026-03-08T10:05:00+00:00',
          latest_next_retry_at: '2026-03-08T10:06:00+00:00',
          latest_blocked_at: '2026-03-08T10:04:00+00:00',
          latest_recovery_at: '2026-03-08T10:05:00+00:00',
          task_counts: { wakeword: 3, stt: 2, tts: 1 },
          status_counts: { recovery: 2, rerouted: 3, stable: 1 },
          avg_cooldown_hint_s: 18.5,
        }}
        items={[
          {
            event_id: 'voice-route-1',
            task: 'wakeword',
            status: 'recovery',
            previous_status: 'stable',
            selected_provider: 'local',
            recommended_provider: 'local',
            occurred_at: '2026-03-08T10:04:00+00:00',
          },
          {
            event_id: 'voice-route-2',
            task: 'stt',
            status: 'rerouted',
            previous_status: 'blocked',
            selected_provider: 'local',
            recommended_provider: 'groq',
            occurred_at: '2026-03-08T10:05:00+00:00',
          },
        ]}
        buckets={[
          {
            bucket_start: '2026-03-08T10:00:00+00:00',
            count: 4,
            blocked_count: 1,
            rerouted_count: 2,
            recovery_pending_count: 2,
          },
        ]}
        missionReliability={{
          mission_id: 'mission-voice-1',
          current: {
            mission_id: 'mission-voice-1',
            sessions: 4,
            route_policy_pause_count: 3,
            route_policy_resume_count: 2,
            wakeword_gate_events: 3,
          },
        }}
        routeRecoveryRecommendation={{
          recovery_profile: 'hybrid_polling',
          wakeword_strategy: 'hybrid_polling',
          confidence: 0.74,
          session_overrides: { fallback_interval_s: 2.4 },
          reasons: ['wakeword route is unstable, preferring faster fallback polling'],
        }}
        formatDateTime={(value) => String(value)}
        formatStatus={(value) => String(value)}
      />
    );

    expect(html).toContain('Long-Horizon Voice Recovery');
    expect(html).toContain('Mission Reliability');
    expect(html).toContain('mission-voice-1');
    expect(html).toContain('Adaptive Route Strategy');
    expect(html).toContain('hybrid_polling');
    expect(html).toContain('wakeword route is unstable, preferring faster fallback polling');
    expect(html).toContain('Recent Recovery Events');
  });
});
