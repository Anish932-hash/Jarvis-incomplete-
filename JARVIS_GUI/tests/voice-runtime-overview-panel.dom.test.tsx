import React from 'react';
import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { VoiceRuntimeOverviewPanel } from '../src/components/os/voice-runtime-overview-panel';

afterEach(() => {
  cleanup();
});

describe('VoiceRuntimeOverviewPanel DOM', () => {
  it('renders and refreshes the cross-session wakeword policy card', async () => {
    const user = userEvent.setup();
    const onRefresh = vi.fn();

    render(
      <VoiceRuntimeOverviewPanel
        voiceSessionState={{ running: true, wakeword_status: 'recovery' }}
        voiceLastError=""
        voiceTranscript="Resume recovery."
        voiceReply="Recovery reply ready."
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
        voiceMissionReliabilityState={{ items: [{ mission_id: 'mission-voice-88', route_recovery_score: 0.77 }] }}
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
            recent_exhaustion_rate: 0.08,
            recent_recovery_rate: 0.66,
            applied_profile_reason: 'Sustained recovery automatically relaxed wakeword restart posture.',
          },
          diagnostics: {
            profile_shift_count: 2,
            profile_shift_timeline: [
              {
                bucket_start: '2026-03-09T08:00:00Z',
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
                bucket_start: '2026-03-09T09:00:00Z',
                recommended_profile: 'hybrid_guarded',
                profile_action: 'guard',
                drift_score: 0.41,
                exhausted_count: 1,
                recovered_count: 0,
              },
            ],
          },
        }}
        voiceWakewordRestartBusy={false}
        voiceWakewordRestartPolicyBusy={false}
        voiceContinuousActiveRun={null}
        voiceContinuousRuns={[]}
        formatRefreshStamp={(value) => String(value)}
        formatCompactDateTime={(value) => String(value)}
        formatVoiceRouteStatus={(value) => String(value)}
        formatRoutePolicyReason={(value) => String(value)}
        onRefreshVoiceWakewordRestartPolicyHistory={onRefresh}
      />
    );

    expect(screen.getByText('Cross-Session Wakeword Policy')).toBeTruthy();
    expect(screen.getByText(/mission: mission-voice-88/i)).toBeTruthy();
    expect(screen.getByText(/profile:recovered_wakeword/i)).toBeTruthy();
    expect(screen.getByText('Runtime Mode Overlay')).toBeTruthy();
    expect(screen.getByText('Profile Shift Timeline')).toBeTruthy();
    expect(screen.getByText(/hybrid_guarded -> recovered_wakeword/i)).toBeTruthy();
    expect(screen.getByText('Cross-Session Drift Trend')).toBeTruthy();

    await user.click(screen.getByRole('button', { name: /refresh cross-session policy/i }));
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });
});
