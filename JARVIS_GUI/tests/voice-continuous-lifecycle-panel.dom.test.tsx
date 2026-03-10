import React from 'react';
import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { VoiceContinuousLifecyclePanel } from '../src/components/os/voice-continuous-lifecycle-panel';

afterEach(() => {
  cleanup();
});

describe('VoiceContinuousLifecyclePanel', () => {
  it('renders recovery state and supports start/cancel interactions', async () => {
    const user = userEvent.setup();
    const onStart = vi.fn();
    const onCancel = vi.fn();

    render(
      <VoiceContinuousLifecyclePanel
        runCount={4}
        durationS={45}
        maxTurns={3}
        idleS={10}
        stopAfter
        busy={false}
        loadingVoice={false}
        activeSessionId="voice-cont-44"
        cancelBusySessionId=""
        activeRun={{
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
        }}
        formatDateTime={(value) => String(value)}
        formatStatus={(value) => String(value)}
        onDurationChange={() => {}}
        onMaxTurnsChange={() => {}}
        onIdleChange={() => {}}
        onStopAfterChange={() => {}}
        onStart={onStart}
        onCancel={onCancel}
      />
    );

    expect(screen.getByTestId('voice-continuous-lifecycle-active-status').textContent).toBe('paused');
    expect(screen.getByTestId('voice-continuous-lifecycle-resume').textContent).toContain('resume:held');
    expect(screen.getByTestId('voice-continuous-lifecycle-session').textContent).toBe('voice-cont-44');
    expect(screen.getByTestId('voice-continuous-lifecycle-mission').textContent).toBe('mission-voice-44');
    expect(screen.getByTestId('voice-continuous-lifecycle-reason').textContent).toContain(
      'mission_recovery_score_too_low'
    );

    await user.click(screen.getByTestId('voice-continuous-start'));
    await user.click(screen.getByTestId('voice-continuous-cancel'));

    expect(onStart).toHaveBeenCalledTimes(1);
    expect(onCancel).toHaveBeenCalledWith('voice-cont-44');
  });
});
