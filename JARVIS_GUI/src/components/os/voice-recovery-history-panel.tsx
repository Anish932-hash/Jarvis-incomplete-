'use client';

import React from 'react';

import { Badge } from '@/components/ui/badge';

type RecordLike = Record<string, unknown>;

export interface VoiceRecoveryHistoryPanelProps {
  diagnostics: RecordLike;
  items: RecordLike[];
  buckets: RecordLike[];
  missionReliability: RecordLike;
  routeRecoveryRecommendation: RecordLike;
  formatDateTime: (value: unknown) => string;
  formatStatus: (value: unknown) => string;
}

function stringifyJson(value: unknown): string {
  if (!value || typeof value !== 'object') return '{}';
  try {
    return JSON.stringify(value);
  } catch {
    return '{}';
  }
}

export function VoiceRecoveryHistoryPanel({
  diagnostics,
  items,
  buckets,
  missionReliability,
  routeRecoveryRecommendation,
  formatDateTime,
  formatStatus,
}: VoiceRecoveryHistoryPanelProps) {
  const missionCurrent = (missionReliability?.current ?? {}) as RecordLike;
  const reasons = Array.isArray(routeRecoveryRecommendation?.reasons)
    ? routeRecoveryRecommendation.reasons.filter((item) => typeof item === 'string' && item.trim())
    : [];

  return (
    <div className="rounded-md border border-primary/20 bg-black/30 p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <p className="text-xs uppercase tracking-wider text-muted-foreground">Long-Horizon Voice Recovery</p>
        <Badge variant="outline">events:{String(diagnostics?.count ?? items.length ?? 0)}</Badge>
        <Badge variant="outline">blocked:{String(diagnostics?.blocked_events ?? 0)}</Badge>
        <Badge variant="outline">rerouted:{String(diagnostics?.rerouted_events ?? 0)}</Badge>
        <Badge variant="outline">recovered:{String(diagnostics?.recovered_events ?? 0)}</Badge>
      </div>
      <div className="grid grid-cols-1 gap-2 text-[11px] text-muted-foreground md:grid-cols-4">
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Latest Event</p>
          <p>{diagnostics?.latest_event_at ? formatDateTime(diagnostics.latest_event_at) : 'n/a'}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Next Retry</p>
          <p>{diagnostics?.latest_next_retry_at ? formatDateTime(diagnostics.latest_next_retry_at) : 'n/a'}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Last Blocked</p>
          <p>{diagnostics?.latest_blocked_at ? formatDateTime(diagnostics.latest_blocked_at) : 'n/a'}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Last Recovery</p>
          <p>{diagnostics?.latest_recovery_at ? formatDateTime(diagnostics.latest_recovery_at) : 'n/a'}</p>
        </div>
      </div>
      <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-3">
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Task Pressure</p>
          <p>tasks: {stringifyJson(diagnostics?.task_counts)}</p>
          <p>statuses: {stringifyJson(diagnostics?.status_counts)}</p>
          <p>avg cooldown: {String(diagnostics?.avg_cooldown_hint_s ?? 0)}s</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Mission Reliability</p>
          <p>mission: {String(missionCurrent?.mission_id ?? missionReliability?.mission_id ?? 'n/a')}</p>
          <p>sessions: {String(missionCurrent?.sessions ?? 0)}</p>
          <p>pauses: {String(missionCurrent?.route_policy_pause_count ?? 0)}</p>
          <p>resumes: {String(missionCurrent?.route_policy_resume_count ?? 0)}</p>
          <p>wakeword gates: {String(missionCurrent?.wakeword_gate_events ?? 0)}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Adaptive Route Strategy</p>
          <p>profile: {String(routeRecoveryRecommendation?.recovery_profile ?? 'balanced')}</p>
          <p>wakeword: {String(routeRecoveryRecommendation?.wakeword_strategy ?? 'wakeword')}</p>
          <p>confidence: {String(routeRecoveryRecommendation?.confidence ?? 0)}</p>
          <p>overrides: {stringifyJson(routeRecoveryRecommendation?.session_overrides)}</p>
          {reasons.length > 0 ? <p>reason: {reasons.join(' | ')}</p> : null}
        </div>
      </div>
      <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Hourly Recovery Drift</p>
          <div className="space-y-1">
            {buckets.slice(-6).map((bucket, index) => {
              const row = bucket as RecordLike;
              return (
                <div
                  key={`voice-route-history-bucket-${String(row.bucket_start ?? index)}`}
                  className="rounded border border-primary/10 bg-background/20 p-2"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span>{formatDateTime(row.bucket_start)}</span>
                    <span>events:{String(row.count ?? 0)}</span>
                  </div>
                  <p>
                    blocked:{String(row.blocked_count ?? 0)} | rerouted:{String(row.rerouted_count ?? 0)} | recovery:
                    {String(row.recovery_pending_count ?? 0)}
                  </p>
                </div>
              );
            })}
            {buckets.length === 0 ? <p>No long-horizon voice recovery buckets yet.</p> : null}
          </div>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Recent Recovery Events</p>
          <div className="space-y-1">
            {items.slice(-6).reverse().map((item, index) => {
              const row = item as RecordLike;
              return (
                <div
                  key={`voice-route-history-${String(row.event_id ?? index)}`}
                  className="rounded border border-primary/10 bg-background/20 p-2"
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline">{String(row.task ?? 'voice')}</Badge>
                    <Badge variant={String(row.status ?? '').trim().toLowerCase() === 'blocked' ? 'secondary' : 'outline'}>
                      {formatStatus(row.status)}
                    </Badge>
                    <span>{formatDateTime(row.occurred_at)}</span>
                  </div>
                  <p className="mt-1">
                    {String(row.previous_status ?? 'unknown')} {' -> '} {String(row.status ?? 'unknown')} | selected:{' '}
                    {String(row.selected_provider ?? 'n/a')} | recommended: {String(row.recommended_provider ?? 'n/a')}
                  </p>
                </div>
              );
            })}
            {items.length === 0 ? <p>No mission-linked recovery events yet.</p> : null}
          </div>
        </div>
      </div>
    </div>
  );
}

export default VoiceRecoveryHistoryPanel;
