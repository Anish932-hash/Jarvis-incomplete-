'use client';

import React from 'react';

import { Badge } from '@/components/ui/badge';

type RecordLike = Record<string, unknown>;

export interface VoiceContinuousRecoveryCardProps {
  activeRun: RecordLike | null;
  items: RecordLike[];
  formatDateTime: (value: unknown) => string;
  formatStatus: (value: unknown) => string;
}

function asRecord(value: unknown): RecordLike {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as RecordLike) : {};
}

export function VoiceContinuousRecoveryCard({
  activeRun,
  items,
  formatDateTime,
  formatStatus,
}: VoiceContinuousRecoveryCardProps) {
  const currentRun = asRecord(activeRun);
  const currentResult = asRecord(currentRun.result);
  const currentDecision = asRecord(currentResult.route_policy_recovery_decision ?? currentRun.route_policy_recovery_decision);
  const currentMission = asRecord(currentResult.mission_reliability);
  const currentMissionRow = asRecord(currentMission.current);
  const currentWakewordSnapshot = asRecord(
    currentResult.wakeword_supervision_snapshot ?? currentRun.wakeword_supervision_snapshot
  );
  const pauseEvents = Array.isArray(currentResult.route_policy_pause_events)
    ? currentResult.route_policy_pause_events.slice(-4).reverse()
    : [];

  return (
    <div className="rounded-md border border-primary/20 bg-black/30 p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <p className="text-xs uppercase tracking-wider text-muted-foreground">Continuous Voice Auto-Recovery</p>
        <Badge variant="outline">runs:{String(items.length)}</Badge>
        <Badge variant="outline">{formatStatus(currentRun.status ?? currentResult.end_reason ?? 'idle')}</Badge>
        <Badge variant={Boolean(currentDecision.resume_allowed) ? 'outline' : 'secondary'}>
          resume:{Boolean(currentDecision.resume_allowed) ? 'allowed' : 'held'}
        </Badge>
      </div>
      <div className="grid grid-cols-1 gap-2 text-[11px] text-muted-foreground md:grid-cols-4">
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Session</p>
          <p>{String(currentRun.session_id ?? 'n/a')}</p>
          <p>{currentRun.created_at ? formatDateTime(currentRun.created_at) : 'no start time'}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Mission</p>
          <p>{String(currentMissionRow.mission_id ?? currentMission.mission_id ?? 'n/a')}</p>
          <p>turns: {String(currentResult.captured_turns ?? currentRun.captured_turns ?? 0)}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Pause Budget</p>
          <p>count left: {String(currentDecision.remaining_pause_count_budget ?? 'n/a')}</p>
          <p>time left: {String(currentDecision.remaining_pause_total_s ?? 'n/a')}s</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Recovery Score</p>
          <p>score: {String(currentDecision.resume_score ?? 'n/a')}</p>
          <p>reason: {String(currentDecision.primary_reason ?? currentResult.route_policy_end_reason ?? 'n/a')}</p>
        </div>
      </div>
      <div className="mt-2 rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
        <p className="mb-2 font-medium text-primary/90">Wakeword Supervision Snapshot</p>
        <div className="grid grid-cols-1 gap-2 md:grid-cols-4">
          <p>status: {formatStatus(currentWakewordSnapshot.status ?? 'n/a')}</p>
          <p>strategy: {String(currentWakewordSnapshot.strategy ?? 'n/a')}</p>
          <p>retry: {currentWakewordSnapshot.next_retry_at ? formatDateTime(currentWakewordSnapshot.next_retry_at) : 'n/a'}</p>
          <p>allow wakeword: {Boolean(currentWakewordSnapshot.allow_wakeword) ? 'yes' : 'no'}</p>
        </div>
        {currentWakewordSnapshot.reason || currentWakewordSnapshot.reason_code ? (
          <p className="mt-1">
            reason: {String(currentWakewordSnapshot.reason ?? currentWakewordSnapshot.reason_code ?? 'n/a')}
          </p>
        ) : null}
      </div>
      <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Adaptive Resume Decision</p>
          <p>profile: {String(currentDecision.recovery_profile ?? 'balanced')}</p>
          <p>wait window: {String(currentDecision.effective_recovery_wait_s ?? currentResult.route_policy_recovery_wait_s ?? 'n/a')}s</p>
          <p>
            stability gate:{' '}
            {String(currentDecision.effective_resume_stability_s ?? currentResult.route_policy_resume_stability_s ?? 'n/a')}s
          </p>
          <p>max pauses: {String(currentDecision.effective_max_pause_count ?? 'n/a')}</p>
          <p>max pause total: {String(currentDecision.effective_max_pause_total_s ?? 'n/a')}s</p>
          {Array.isArray(currentDecision.reasons) && currentDecision.reasons.length > 0 ? (
            <p>reasons: {currentDecision.reasons.join(' | ')}</p>
          ) : null}
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Recent Pause Events</p>
          <div className="space-y-1">
            {pauseEvents.map((item, index) => {
              const row = asRecord(item);
              const decision = asRecord(row.recovery_decision);
              return (
                <div
                  key={`voice-cont-pause-${String(row.event_id ?? index)}`}
                  className="rounded border border-primary/10 bg-background/20 p-2"
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline">{String(row.task ?? 'voice')}</Badge>
                    <span>{formatDateTime(row.paused_at)}</span>
                  </div>
                  <p className="mt-1">
                    {String(row.reason ?? row.reason_code ?? 'route gate')} | retry:{' '}
                    {row.next_retry_at ? formatDateTime(row.next_retry_at) : 'n/a'}
                  </p>
                  <p>
                    resume:{Boolean(decision.resume_allowed) ? 'allowed' : 'held'} | score:
                    {String(decision.resume_score ?? 'n/a')}
                  </p>
                  {(row.wakeword_supervision_status || row.wakeword_supervision_strategy) ? (
                    <p>
                      wakeword:{' '}
                      {String(row.wakeword_supervision_status ?? 'n/a')} / {String(row.wakeword_supervision_strategy ?? 'n/a')}
                    </p>
                  ) : null}
                </div>
              );
            })}
            {pauseEvents.length === 0 ? <p>No continuous-session pause events yet.</p> : null}
          </div>
        </div>
      </div>
    </div>
  );
}

export default VoiceContinuousRecoveryCard;
