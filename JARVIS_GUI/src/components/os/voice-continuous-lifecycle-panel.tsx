'use client';

import React from 'react';

import { Badge } from '@/components/ui/badge';
import VoiceContinuousControls from './voice-continuous-controls';

type RecordLike = Record<string, unknown>;

function asRecord(value: unknown): RecordLike {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as RecordLike) : {};
}

export interface VoiceContinuousLifecyclePanelProps {
  runCount: number;
  durationS: number;
  maxTurns: number;
  idleS: number;
  stopAfter: boolean;
  busy?: boolean;
  loadingVoice?: boolean;
  activeSessionId?: string;
  cancelBusySessionId?: string;
  activeRun: RecordLike | null;
  formatDateTime: (value: unknown) => string;
  formatStatus: (value: unknown) => string;
  onDurationChange: (value: number) => void;
  onMaxTurnsChange: (value: number) => void;
  onIdleChange: (value: number) => void;
  onStopAfterChange: (value: boolean) => void;
  onStart: () => void;
  onCancel: (sessionId: string) => void;
}

export function VoiceContinuousLifecyclePanel({
  runCount,
  durationS,
  maxTurns,
  idleS,
  stopAfter,
  busy = false,
  loadingVoice = false,
  activeSessionId = '',
  cancelBusySessionId = '',
  activeRun,
  formatDateTime,
  formatStatus,
  onDurationChange,
  onMaxTurnsChange,
  onIdleChange,
  onStopAfterChange,
  onStart,
  onCancel,
}: VoiceContinuousLifecyclePanelProps) {
  const currentRun = asRecord(activeRun);
  const result = asRecord(currentRun.result);
  const decision = asRecord(result.route_policy_recovery_decision ?? currentRun.route_policy_recovery_decision);
  const mission = asRecord(result.mission_reliability);
  const missionRow = asRecord(mission.current);
  const cleanSessionId = String(activeSessionId || currentRun.session_id || '').trim();
  const lifecycleStatus = formatStatus(currentRun.status ?? result.end_reason ?? 'idle');
  const lifecycleReason = String(
    decision.primary_reason ?? result.route_policy_end_reason ?? currentRun.end_reason ?? 'n/a'
  ).trim();
  const resumeAllowed = Boolean(decision.resume_allowed);
  const retryAt = decision.next_retry_at ?? result.next_retry_at ?? currentRun.next_retry_at ?? '';

  return (
    <div className="space-y-3" data-testid="voice-continuous-lifecycle-panel">
      <VoiceContinuousControls
        runCount={runCount}
        durationS={durationS}
        maxTurns={maxTurns}
        idleS={idleS}
        stopAfter={stopAfter}
        busy={busy}
        loadingVoice={loadingVoice}
        activeSessionId={cleanSessionId}
        cancelBusySessionId={cancelBusySessionId}
        onDurationChange={onDurationChange}
        onMaxTurnsChange={onMaxTurnsChange}
        onIdleChange={onIdleChange}
        onStopAfterChange={onStopAfterChange}
        onStart={onStart}
        onCancel={onCancel}
      />
      <div
        className="space-y-2 rounded-md border border-primary/20 bg-background/30 p-3 text-[11px] text-muted-foreground"
        data-testid="voice-continuous-lifecycle-summary"
      >
        <div className="flex flex-wrap items-center gap-2">
          <p className="text-xs uppercase tracking-wider text-muted-foreground">Continuous Lifecycle Snapshot</p>
          <Badge data-testid="voice-continuous-lifecycle-active-status" variant="outline">
            {lifecycleStatus}
          </Badge>
          <Badge data-testid="voice-continuous-lifecycle-resume" variant={resumeAllowed ? 'outline' : 'secondary'}>
            resume:{resumeAllowed ? 'allowed' : 'held'}
          </Badge>
        </div>
        <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Session</p>
            <p data-testid="voice-continuous-lifecycle-session">{cleanSessionId || 'none'}</p>
            <p>{currentRun.created_at ? formatDateTime(currentRun.created_at) : 'No active run yet.'}</p>
          </div>
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Mission Recovery</p>
            <p data-testid="voice-continuous-lifecycle-mission">
              {String(missionRow.mission_id ?? mission.mission_id ?? 'n/a')}
            </p>
            <p data-testid="voice-continuous-lifecycle-reason">{lifecycleReason || 'n/a'}</p>
            <p>{retryAt ? `retry: ${formatDateTime(retryAt)}` : 'retry: n/a'}</p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default VoiceContinuousLifecyclePanel;
