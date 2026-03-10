import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { VoiceRuntimeOverviewPanel } from '../src/components/os/voice-runtime-overview-panel';

describe('VoiceRuntimeOverviewPanel', () => {
  it('renders cross-session wakeword profile posture and drift trend', () => {
    const html = renderToStaticMarkup(
      <VoiceRuntimeOverviewPanel
        voiceSessionState={{ running: true, wakeword_status: 'recovery' }}
        voiceLastError=""
        voiceTranscript="Resume the recovery flow."
        voiceReply="Recovery summary is ready."
        ttsLocalHealth="degraded"
        ttsElevenlabsHealth="ready"
        voiceDiagnosticsLastRefreshAt="2026-03-09T10:00:00Z"
        voiceStreamConnected
        voiceStreamLastEventAt="2026-03-09T10:00:05Z"
        voiceRoutePolicyCurrentSummary={{ status: 'recovery', reason: 'wakeword_drift_guard' }}
        voiceRoutePolicyCurrentStt={{ status: 'stable', selected_provider: 'groq', recommended_provider: 'groq' }}
        voiceRoutePolicyCurrentWakeword={{ status: 'recovery', selected_provider: 'local', reason_code: 'restart_drift_guard' }}
        voiceRoutePolicyCurrentTts={{ status: 'stable', selected_provider: 'elevenlabs', recommended_provider: 'elevenlabs' }}
        voiceRoutePolicyItems={[]}
        voiceRoutePolicyHistoryDiagnostics={{}}
        voiceRoutePolicyHistoryState={{ count: 0 }}
        voiceRoutePolicyHistoryItems={[]}
        voiceRoutePolicyHistoryBuckets={[]}
        voiceMissionReliabilityState={{
          items: [
            {
              mission_id: 'mission-voice-77',
              route_recovery_score: 0.74,
            },
          ],
        }}
        voiceRouteRecoveryRecommendation={{ strategy: 'hybrid_polling' }}
        voiceWakewordSupervisionHistoryState={{ count: 0, items: [], diagnostics: {}, current: {} }}
        voiceWakewordRestartHistoryState={{ count: 0, items: [], diagnostics: {}, current: {} }}
        voiceWakewordRestartPolicyHistoryState={{
          count: 2,
          current: {
            applied_profile: 'recovered_wakeword',
            profile_action: 'recover',
            auto_profile_applied: true,
            profile_decision_source: 'sustained_recovery',
            drift_score: 0.22,
            last_profile_shift_at: '2026-03-09T09:45:00Z',
            recent_exhaustion_rate: 0.11,
            recent_recovery_rate: 0.66,
            applied_profile_reason: 'Sustained recovery automatically relaxed wakeword restart posture.',
          },
          diagnostics: {
            applied_profile: 'recovered_wakeword',
            profile_action: 'recover',
            profile_shift_count: 2,
            profile_timeline: [
              {
                bucket_start: '2026-03-09T09:00:00Z',
                recommended_profile: 'hybrid_guarded',
                profile_action: 'guard',
                drift_score: 0.48,
                exhausted_count: 1,
                recovered_count: 0,
              },
              {
                bucket_start: '2026-03-09T10:00:00Z',
                recommended_profile: 'recovered_wakeword',
                profile_action: 'recover',
                drift_score: 0.22,
                exhausted_count: 0,
                recovered_count: 2,
              },
            ],
          },
        }}
        voiceWakewordRestartEventTypeFilter=""
        voiceWakewordRestartBusy={false}
        voiceWakewordRestartPolicyBusy={false}
        voiceContinuousActiveRun={null}
        voiceContinuousRuns={[]}
        formatRefreshStamp={(value) => String(value)}
        formatCompactDateTime={(value) => String(value)}
        formatVoiceRouteStatus={(value) => String(value)}
        formatRoutePolicyReason={(value) => String(value)}
      />
    );

    expect(html).toContain('Cross-Session Wakeword Policy');
    expect(html).toContain('profile:recovered_wakeword');
    expect(html).toContain('action:recover');
    expect(html).toContain('auto:on');
    expect(html).toContain('Applied Runtime Posture');
    expect(html).toContain('Recovery Pressure');
    expect(html).toContain('Policy Reasoning');
    expect(html).toContain('Cross-Session Drift Trend');
    expect(html).toContain('hybrid_polling');
    expect(html).toContain('mission-voice-77');
    expect(html).toContain('Sustained recovery automatically relaxed wakeword restart posture.');
  });
});
