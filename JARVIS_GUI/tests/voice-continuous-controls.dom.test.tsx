import React from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { VoiceContinuousControls } from '../src/components/os/voice-continuous-controls';

afterEach(() => {
  cleanup();
});

interface HarnessProps {
  activeSessionId?: string;
  busy?: boolean;
  loadingVoice?: boolean;
  cancelBusySessionId?: string;
  onStart?: () => void;
  onCancel?: (sessionId: string) => void;
  onDurationChange?: (value: number) => void;
  onMaxTurnsChange?: (value: number) => void;
  onIdleChange?: (value: number) => void;
  onStopAfterChange?: (value: boolean) => void;
}

function VoiceContinuousControlsHarness({
  activeSessionId = '',
  busy = false,
  loadingVoice = false,
  cancelBusySessionId = '',
  onStart = vi.fn(),
  onCancel = vi.fn(),
  onDurationChange = vi.fn(),
  onMaxTurnsChange = vi.fn(),
  onIdleChange = vi.fn(),
  onStopAfterChange = vi.fn(),
}: HarnessProps) {
  const [durationS, setDurationS] = React.useState(45);
  const [maxTurns, setMaxTurns] = React.useState(3);
  const [idleS, setIdleS] = React.useState(10);
  const [stopAfter, setStopAfter] = React.useState(true);

  return (
    <VoiceContinuousControls
      runCount={3}
      durationS={durationS}
      maxTurns={maxTurns}
      idleS={idleS}
      stopAfter={stopAfter}
      busy={busy}
      loadingVoice={loadingVoice}
      activeSessionId={activeSessionId}
      cancelBusySessionId={cancelBusySessionId}
      onDurationChange={(value) => {
        onDurationChange(value);
        setDurationS(value);
      }}
      onMaxTurnsChange={(value) => {
        onMaxTurnsChange(value);
        setMaxTurns(value);
      }}
      onIdleChange={(value) => {
        onIdleChange(value);
        setIdleS(value);
      }}
      onStopAfterChange={(value) => {
        onStopAfterChange(value);
        setStopAfter(value);
      }}
      onStart={onStart}
      onCancel={onCancel}
    />
  );
}

describe('VoiceContinuousControls', () => {
  it('handles parameter changes and start interaction', async () => {
    const user = userEvent.setup();
    const onDurationChange = vi.fn();
    const onMaxTurnsChange = vi.fn();
    const onIdleChange = vi.fn();
    const onStopAfterChange = vi.fn();
    const onStart = vi.fn();

    render(
      <VoiceContinuousControlsHarness
        onDurationChange={onDurationChange}
        onMaxTurnsChange={onMaxTurnsChange}
        onIdleChange={onIdleChange}
        onStopAfterChange={onStopAfterChange}
        onStart={onStart}
      />
    );

    const durationInput = screen.getByTestId('voice-continuous-duration');
    const turnsInput = screen.getByTestId('voice-continuous-max-turns');
    const idleInput = screen.getByTestId('voice-continuous-idle');

    fireEvent.change(durationInput, { target: { value: '72' } });
    fireEvent.change(turnsInput, { target: { value: '5' } });
    fireEvent.change(idleInput, { target: { value: '18' } });
    await user.click(screen.getByTestId('voice-continuous-stop-after'));
    await user.click(screen.getByTestId('voice-continuous-start'));

    expect(onDurationChange).toHaveBeenLastCalledWith(72);
    expect(onMaxTurnsChange).toHaveBeenLastCalledWith(5);
    expect(onIdleChange).toHaveBeenLastCalledWith(18);
    expect(onStopAfterChange).toHaveBeenCalledWith(false);
    expect(onStart).toHaveBeenCalledTimes(1);
  });

  it('enables cancel only when there is an active session', async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn();

    const { rerender } = render(<VoiceContinuousControlsHarness onCancel={onCancel} />);

    expect((screen.getByTestId('voice-continuous-cancel') as HTMLButtonElement).disabled).toBe(true);

    rerender(<VoiceContinuousControlsHarness activeSessionId="voice-cont-77" onCancel={onCancel} />);

    expect((screen.getByTestId('voice-continuous-cancel') as HTMLButtonElement).disabled).toBe(false);

    await user.click(screen.getByTestId('voice-continuous-cancel'));

    expect(onCancel).toHaveBeenCalledWith('voice-cont-77');
  });

  it('disables start while busy or loading voice state', () => {
    const { rerender } = render(<VoiceContinuousControlsHarness busy />);

    expect((screen.getByTestId('voice-continuous-start') as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByTestId('voice-continuous-status').textContent).toBe('starting');

    rerender(<VoiceContinuousControlsHarness loadingVoice />);

    expect((screen.getByTestId('voice-continuous-start') as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByTestId('voice-continuous-status').textContent).toBe('loading');
    expect(screen.getByTestId('voice-continuous-hint').textContent).toContain('Refreshing voice runtime state');
  });

  it('disables cancel while the active session cancellation is in progress', () => {
    render(<VoiceContinuousControlsHarness activeSessionId="voice-cont-88" cancelBusySessionId="voice-cont-88" />);

    expect((screen.getByTestId('voice-continuous-cancel') as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByTestId('voice-continuous-status').textContent).toBe('canceling');
    expect(screen.getByTestId('voice-continuous-hint').textContent).toContain('Cancelling');
  });

  it('clamps numeric inputs to supported voice session ranges', () => {
    const onDurationChange = vi.fn();
    const onMaxTurnsChange = vi.fn();
    const onIdleChange = vi.fn();

    render(
      <VoiceContinuousControlsHarness
        onDurationChange={onDurationChange}
        onMaxTurnsChange={onMaxTurnsChange}
        onIdleChange={onIdleChange}
      />
    );

    const durationInput = screen.getByTestId('voice-continuous-duration');
    const turnsInput = screen.getByTestId('voice-continuous-max-turns');
    const idleInput = screen.getByTestId('voice-continuous-idle');

    fireEvent.change(durationInput, { target: { value: '1' } });
    fireEvent.change(durationInput, { target: { value: '999' } });
    fireEvent.change(turnsInput, { target: { value: '0' } });
    fireEvent.change(turnsInput, { target: { value: '999' } });
    fireEvent.change(idleInput, { target: { value: '-4' } });
    fireEvent.change(idleInput, { target: { value: '999' } });

    expect(onDurationChange).toHaveBeenNthCalledWith(1, 3);
    expect(onDurationChange).toHaveBeenNthCalledWith(2, 300);
    expect(onMaxTurnsChange).toHaveBeenNthCalledWith(1, 1);
    expect(onMaxTurnsChange).toHaveBeenNthCalledWith(2, 24);
    expect(onIdleChange).toHaveBeenNthCalledWith(1, 0);
    expect(onIdleChange).toHaveBeenNthCalledWith(2, 180);
    expect((durationInput as HTMLInputElement).value).toBe('300');
    expect((turnsInput as HTMLInputElement).value).toBe('24');
    expect((idleInput as HTMLInputElement).value).toBe('180');
  });

  it('keeps the stop-after toggle interactive after control-state updates', async () => {
    const user = userEvent.setup();
    const onStopAfterChange = vi.fn();

    const { rerender } = render(<VoiceContinuousControlsHarness onStopAfterChange={onStopAfterChange} />);

    await user.click(screen.getByTestId('voice-continuous-stop-after'));
    rerender(<VoiceContinuousControlsHarness activeSessionId="voice-cont-91" onStopAfterChange={onStopAfterChange} />);
    await user.click(screen.getByTestId('voice-continuous-stop-after'));

    expect(onStopAfterChange).toHaveBeenCalledTimes(2);
    expect(onStopAfterChange).toHaveBeenNthCalledWith(1, false);
    expect(onStopAfterChange).toHaveBeenNthCalledWith(2, true);
  });
});
