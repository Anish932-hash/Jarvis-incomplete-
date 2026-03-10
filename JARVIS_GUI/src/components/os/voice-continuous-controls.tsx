'use client';

import React from 'react';
import { Loader2 } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Switch } from '@/components/ui/switch';

interface VoiceContinuousControlsProps {
  runCount: number;
  durationS: number;
  maxTurns: number;
  idleS: number;
  stopAfter: boolean;
  busy?: boolean;
  loadingVoice?: boolean;
  activeSessionId?: string;
  cancelBusySessionId?: string;
  onDurationChange: (value: number) => void;
  onMaxTurnsChange: (value: number) => void;
  onIdleChange: (value: number) => void;
  onStopAfterChange: (value: boolean) => void;
  onStart: () => void;
  onCancel: (sessionId: string) => void;
}

export function VoiceContinuousControls({
  runCount,
  durationS,
  maxTurns,
  idleS,
  stopAfter,
  busy = false,
  loadingVoice = false,
  activeSessionId = '',
  cancelBusySessionId = '',
  onDurationChange,
  onMaxTurnsChange,
  onIdleChange,
  onStopAfterChange,
  onStart,
  onCancel,
}: VoiceContinuousControlsProps) {
  const cleanActiveSessionId = String(activeSessionId || '').trim();
  const cancelBusy = cleanActiveSessionId.length > 0 && cancelBusySessionId === cleanActiveSessionId;
  const controlStatus = busy ? 'starting' : loadingVoice ? 'loading' : cancelBusy ? 'canceling' : cleanActiveSessionId ? 'active' : 'idle';
  const controlHint = busy
    ? 'Starting a new continuous run.'
    : loadingVoice
      ? 'Refreshing voice runtime state before allowing another run.'
      : cancelBusy
        ? 'Cancelling the active continuous run.'
        : cleanActiveSessionId
          ? `Active session: ${cleanActiveSessionId}`
          : 'No continuous voice run is active.';

  return (
    <div className="space-y-2 rounded-md border border-primary/20 bg-background/40 p-3" data-testid="voice-continuous-controls">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs uppercase tracking-wider text-muted-foreground">Continuous Voice Recovery</p>
        <Badge variant="outline">runs:{String(runCount)}</Badge>
      </div>
      <div className="flex flex-wrap items-center gap-2 rounded border border-primary/15 bg-background/30 px-2 py-2 text-[11px] text-muted-foreground">
        <Badge data-testid="voice-continuous-status" variant="secondary">
          {controlStatus}
        </Badge>
        <span data-testid="voice-continuous-hint" className="truncate">
          {controlHint}
        </span>
      </div>
      <div className="grid grid-cols-3 gap-2">
        <div className="space-y-1">
          <p className="text-[11px] uppercase tracking-wider text-muted-foreground">Duration (s)</p>
          <Input
            data-testid="voice-continuous-duration"
            type="number"
            min={3}
            max={300}
            value={durationS}
            onChange={(event) => {
              const parsed = Number.parseInt(event.target.value, 10);
              const nextValue = Number.isFinite(parsed) ? parsed : 45;
              onDurationChange(Math.max(3, Math.min(300, nextValue)));
            }}
            className="h-9 border-primary/20 bg-background/60"
          />
        </div>
        <div className="space-y-1">
          <p className="text-[11px] uppercase tracking-wider text-muted-foreground">Max Turns</p>
          <Input
            data-testid="voice-continuous-max-turns"
            type="number"
            min={1}
            max={24}
            value={maxTurns}
            onChange={(event) => {
              const parsed = Number.parseInt(event.target.value, 10);
              const nextValue = Number.isFinite(parsed) ? parsed : 3;
              onMaxTurnsChange(Math.max(1, Math.min(24, nextValue)));
            }}
            className="h-9 border-primary/20 bg-background/60"
          />
        </div>
        <div className="space-y-1">
          <p className="text-[11px] uppercase tracking-wider text-muted-foreground">Idle Stop (s)</p>
          <Input
            data-testid="voice-continuous-idle"
            type="number"
            min={0}
            max={180}
            value={idleS}
            onChange={(event) => {
              const parsed = Number.parseFloat(event.target.value);
              const nextValue = Number.isFinite(parsed) ? parsed : 10;
              onIdleChange(Math.max(0, Math.min(180, nextValue)));
            }}
            className="h-9 border-primary/20 bg-background/60"
          />
        </div>
      </div>
      <div className="flex items-center justify-between rounded border border-primary/20 bg-background/30 px-2 py-2">
        <p className="text-[11px] text-muted-foreground">Stop Voice Session After Run</p>
        <Switch data-testid="voice-continuous-stop-after" checked={stopAfter} onCheckedChange={onStopAfterChange} />
      </div>
      <div className="grid grid-cols-2 gap-2">
        <Button data-testid="voice-continuous-start" type="button" onClick={onStart} disabled={busy || loadingVoice}>
          {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Start Continuous
        </Button>
        <Button
          data-testid="voice-continuous-cancel"
          type="button"
          variant="secondary"
          onClick={() => onCancel(cleanActiveSessionId)}
          disabled={!cleanActiveSessionId || cancelBusy}
        >
          {cancelBusy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Cancel Active
        </Button>
      </div>
    </div>
  );
}

export default VoiceContinuousControls;
