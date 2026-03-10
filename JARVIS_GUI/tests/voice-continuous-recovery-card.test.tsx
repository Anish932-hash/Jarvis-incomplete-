import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { VoiceContinuousRecoveryCard } from '../src/components/os/voice-continuous-recovery-card';

describe('VoiceContinuousRecoveryCard', () => {
  it('renders adaptive resume state and recent pause events', () => {
    const html = renderToStaticMarkup(
      <VoiceContinuousRecoveryCard
        activeRun={{
          session_id: 'voice-cont-42',
          status: 'paused',
          created_at: '2026-03-08T10:10:00+00:00',
          result: {
            captured_turns: 1,
            route_policy_recovery_wait_s: 24,
            route_policy_resume_stability_s: 1.1,
            wakeword_supervision_snapshot: {
              status: 'hybrid_polling',
              strategy: 'hybrid_polling',
              allow_wakeword: false,
              next_retry_at: '2026-03-08T10:10:07+00:00',
              reason: 'Wakeword route is recovering.',
            },
            route_policy_pause_events: [
              {
                event_id: 'pause-1',
                task: 'stt',
                paused_at: '2026-03-08T10:10:03+00:00',
                next_retry_at: '2026-03-08T10:10:07+00:00',
                reason: 'Local STT route blocked by launcher policy.',
                wakeword_supervision_status: 'hybrid_polling',
                wakeword_supervision_strategy: 'hybrid_polling',
                recovery_decision: {
                  resume_allowed: false,
                  resume_score: 0.14,
                },
              },
            ],
            route_policy_recovery_decision: {
              resume_allowed: false,
              recovery_profile: 'hybrid_polling',
              resume_score: 0.14,
              primary_reason: 'mission_recovery_score_too_low',
              effective_recovery_wait_s: 24,
              effective_resume_stability_s: 1.1,
              effective_max_pause_count: 2,
              effective_max_pause_total_s: 45,
              remaining_pause_count_budget: 0,
              remaining_pause_total_s: 7.5,
              reasons: ['STT route is blocked, tightening auto-resume budget'],
            },
            mission_reliability: {
              current: {
                mission_id: 'mission-voice-8',
              },
            },
          },
        }}
        items={[
          { session_id: 'voice-cont-42', status: 'paused' },
          { session_id: 'voice-cont-41', status: 'completed' },
        ]}
        formatDateTime={(value) => String(value)}
        formatStatus={(value) => String(value)}
      />
    );

    expect(html).toContain('Continuous Voice Auto-Recovery');
    expect(html).toContain('voice-cont-42');
    expect(html).toContain('mission-voice-8');
    expect(html).toContain('resume:held');
    expect(html).toContain('Adaptive Resume Decision');
    expect(html).toContain('Wakeword Supervision Snapshot');
    expect(html).toContain('Wakeword route is recovering.');
    expect(html).toContain('mission_recovery_score_too_low');
    expect(html).toContain('Recent Pause Events');
  });
});
