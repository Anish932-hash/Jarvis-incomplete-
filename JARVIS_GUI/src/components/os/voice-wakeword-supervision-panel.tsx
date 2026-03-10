'use client';

import React from 'react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';

type RecordLike = Record<string, unknown>;

export interface VoiceWakewordSupervisionPanelProps {
  history: RecordLike;
  restartHistory: RecordLike | null;
  restartPolicyHistory?: RecordLike | null;
  activeRun: RecordLike | null;
  formatDateTime: (value: unknown) => string;
  formatStatus: (value: unknown) => string;
  restartEventTypeFilter?: string;
  restartHistoryBusy?: boolean;
  restartPolicyHistoryBusy?: boolean;
  onRestartEventTypeFilterChange?: (value: string) => void;
  onRefreshRestartHistory?: () => void;
  onRefreshRestartPolicyHistory?: () => void;
}

function asRecord(value: unknown): RecordLike {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as RecordLike) : {};
}

export function VoiceWakewordSupervisionPanel({
  history,
  restartHistory,
  restartPolicyHistory,
  activeRun,
  formatDateTime,
  formatStatus,
  restartEventTypeFilter = '',
  restartHistoryBusy = false,
  restartPolicyHistoryBusy = false,
  onRestartEventTypeFilterChange,
  onRefreshRestartHistory,
  onRefreshRestartPolicyHistory,
}: VoiceWakewordSupervisionPanelProps) {
  const diagnostics = asRecord(history.diagnostics);
  const current = asRecord(history.current);
  const activeRunRecord = asRecord(activeRun);
  const activeRunResult = asRecord(activeRunRecord.result);
  const activeWakewordSnapshot = asRecord(
    activeRunResult.wakeword_supervision_snapshot ?? activeRunRecord.wakeword_supervision_snapshot
  );
  const items = Array.isArray(history.items)
    ? history.items.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const buckets = Array.isArray(diagnostics.timeline_buckets)
    ? diagnostics.timeline_buckets.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const restartHistoryRecord = asRecord(restartHistory);
  const restartDiagnostics = asRecord(restartHistoryRecord.diagnostics);
  const restartCurrent = asRecord(restartHistoryRecord.current);
  const restartItems = Array.isArray(restartHistoryRecord.items)
    ? restartHistoryRecord.items.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const restartBuckets = Array.isArray(restartDiagnostics.timeline_buckets)
    ? restartDiagnostics.timeline_buckets.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const restartPolicyHistoryRecord = asRecord(restartPolicyHistory);
  const restartPolicyDiagnostics = asRecord(restartPolicyHistoryRecord.diagnostics);
  const restartPolicyCurrent = asRecord(restartPolicyHistoryRecord.current);
  const restartPolicyBuckets = Array.isArray(restartPolicyDiagnostics.timeline_buckets)
    ? restartPolicyDiagnostics.timeline_buckets.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const restartProfileTimeline = Array.isArray(restartPolicyDiagnostics.profile_timeline)
    ? restartPolicyDiagnostics.profile_timeline.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const restartPolicy = asRecord(restartCurrent.policy);

  return (
    <div className="rounded-md border border-primary/20 bg-black/30 p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <p className="text-xs uppercase tracking-wider text-muted-foreground">Wakeword Supervision</p>
        <Badge variant="outline">events:{String(history.count ?? items.length ?? 0)}</Badge>
        <Badge variant="outline">recovered:{String(diagnostics.recovered_events ?? 0)}</Badge>
        <Badge variant="outline">deferred:{String(diagnostics.deferred_events ?? 0)}</Badge>
        <Badge variant="outline">{formatStatus(current.status ?? 'unknown')}</Badge>
      </div>

      <div className="grid grid-cols-1 gap-2 text-[11px] text-muted-foreground md:grid-cols-4">
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Current Gate</p>
          <p>status: {formatStatus(current.status ?? 'unknown')}</p>
          <p>strategy: {String(current.strategy ?? 'n/a')}</p>
          <p>allow wakeword: {Boolean(current.allow_wakeword) ? 'yes' : 'no'}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Recovery Window</p>
          <p>restart delay: {String(current.restart_delay_s ?? 0)}s</p>
          <p>retry: {current.next_retry_at ? formatDateTime(current.next_retry_at) : 'n/a'}</p>
          <p>resume stability: {String(current.resume_stability_s ?? 0)}s</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Mission Pressure</p>
          <p>sessions: {String(current.mission_sessions ?? 0)}</p>
          <p>wakeword gates: {String(current.wakeword_gate_events ?? 0)}</p>
          <p>voice pressure: {String(current.local_voice_pressure_score ?? 0)}</p>
        </div>
        <div className="rounded border border-primary/20 bg-background/20 p-2">
          <p className="font-medium text-primary/90">Active Continuous Run</p>
          <p>session: {String(activeRunRecord.session_id ?? 'n/a')}</p>
          <p>snapshot: {formatStatus(activeWakewordSnapshot.status ?? 'n/a')}</p>
          <p>strategy: {String(activeWakewordSnapshot.strategy ?? 'n/a')}</p>
        </div>
      </div>

      {current.reason || current.reason_code ? (
        <div className="mt-2 rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p>reason: {String(current.reason ?? current.reason_code ?? 'n/a')}</p>
          {current.fallback_interval_s ? <p>poll fallback: {String(current.fallback_interval_s)}s</p> : null}
        </div>
      ) : null}

      <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Hourly Supervision Drift</p>
          <div className="space-y-1">
            {buckets.slice(-6).map((bucket, index) => (
              <div
                key={`wakeword-supervision-bucket-${String(bucket.bucket_start ?? index)}`}
                className="rounded border border-primary/10 bg-background/20 p-2"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span>{formatDateTime(bucket.bucket_start)}</span>
                  <span>events:{String(bucket.count ?? 0)}</span>
                </div>
                <p>
                  paused:{String(bucket.paused_count ?? 0)} | active:{String(bucket.active_count ?? 0)} | recovered:
                  {String(bucket.recovered_count ?? 0)}
                </p>
              </div>
            ))}
            {buckets.length === 0 ? <p>No wakeword supervision buckets yet.</p> : null}
          </div>
        </div>

        <div className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Recent Supervision Events</p>
          <div className="space-y-1">
            {items.slice(-6).reverse().map((item, index) => (
              <div
                key={`wakeword-supervision-event-${String(item.event_id ?? index)}`}
                className="rounded border border-primary/10 bg-background/20 p-2"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline">{formatStatus(item.status ?? 'unknown')}</Badge>
                  <span>{formatDateTime(item.occurred_at)}</span>
                </div>
                <p className="mt-1">
                  {String(item.previous_status ?? 'unknown')} {' -> '} {String(item.status ?? 'unknown')} | strategy:{' '}
                  {String(item.strategy ?? 'n/a')}
                </p>
                <p>
                  retry: {item.next_retry_at ? formatDateTime(item.next_retry_at) : 'n/a'} | mission:{' '}
                  {String(item.mission_id ?? 'n/a')}
                </p>
                {item.reason || item.reason_code ? <p>reason: {String(item.reason ?? item.reason_code ?? '')}</p> : null}
              </div>
            ))}
            {items.length === 0 ? <p>No wakeword supervision events recorded yet.</p> : null}
          </div>
        </div>
      </div>

      <div className="mt-2 rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
        <div className="mb-2 flex flex-wrap items-center gap-2">
          <p className="font-medium text-primary/90">Wakeword Restart Timeline</p>
          <Badge variant="outline">events:{String(restartHistoryRecord.count ?? restartItems.length ?? 0)}</Badge>
          <Badge variant="outline">recovered:{String(restartDiagnostics.recovered_events ?? 0)}</Badge>
          <Badge variant="outline">exhausted:{String(restartDiagnostics.exhausted_events ?? 0)}</Badge>
          {restartHistoryRecord.event_type_filter ? (
            <Badge variant="outline">filter:{String(restartHistoryRecord.event_type_filter)}</Badge>
          ) : null}
          <label className="ml-auto flex items-center gap-2 text-[11px] text-muted-foreground">
            <span>event</span>
            <select
              aria-label="Wakeword restart event filter"
              className="rounded border border-primary/20 bg-background/30 px-2 py-1 text-[11px] text-foreground"
              value={restartEventTypeFilter}
              onChange={(event) => onRestartEventTypeFilterChange?.(event.target.value)}
            >
              <option value="">all</option>
              <option value="start_failed">start_failed</option>
              <option value="restart_backoff">restart_backoff</option>
              <option value="restart_exhausted">restart_exhausted</option>
              <option value="restart_exhaustion_expired">restart_exhaustion_expired</option>
              <option value="recovery_window_elapsed">recovery_window_elapsed</option>
              <option value="restart_policy_relaxed">restart_policy_relaxed</option>
              <option value="started">started</option>
              <option value="recovered">recovered</option>
            </select>
          </label>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-7 px-2 text-[11px]"
            onClick={() => onRefreshRestartHistory?.()}
            disabled={restartHistoryBusy}
          >
            {restartHistoryBusy ? 'Refreshing...' : 'Refresh Restart History'}
          </Button>
        </div>

        <div className="grid grid-cols-1 gap-2 md:grid-cols-3">
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Adaptive Policy</p>
            <p>recent failures: {String(restartPolicy.recent_failures ?? 0)}</p>
            <p>recent successes: {String(restartPolicy.recent_successes ?? 0)}</p>
            <p>consecutive failures: {String(restartPolicy.consecutive_failures ?? 0)}</p>
            <p>recovery credit: {String(restartPolicy.recovery_credit ?? 'n/a')}</p>
          </div>
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Restart Limits</p>
            <p>max failures before polling: {String(restartPolicy.max_failures_before_polling ?? 'n/a')}</p>
            <p>cooldown scale: {String(restartPolicy.cooldown_scale ?? 'n/a')}</p>
            <p>fallback interval: {String(restartPolicy.recommended_fallback_interval_s ?? 'n/a')}s</p>
            <p>delay decay: {String(restartPolicy.recommended_delay_decay_factor ?? 'n/a')}</p>
          </div>
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Current Restart State</p>
            <p>exhausted: {Boolean(restartCurrent.exhausted) ? 'yes' : 'no'}</p>
            <p>retry: {restartCurrent.next_retry_at ? formatDateTime(restartCurrent.next_retry_at) : 'n/a'}</p>
            <p>exhausted until: {restartCurrent.exhausted_until ? formatDateTime(restartCurrent.exhausted_until) : 'n/a'}</p>
            <p>history: {String(restartHistoryRecord.history_path ?? 'n/a')}</p>
            <p>relax backoff by: {String(restartPolicy.recommended_backoff_relaxation ?? 'n/a')}</p>
          </div>
        </div>

        <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-3">
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Restored Runtime Tuning</p>
            <p>wakeword sensitivity: {String(restartPolicyCurrent.wakeword_sensitivity ?? restartCurrent.wakeword_sensitivity ?? 'n/a')}</p>
            <p>fallback interval: {String(restartPolicyCurrent.fallback_interval_s ?? restartCurrent.fallback_interval_s ?? 'n/a')}s</p>
            <p>resume stability: {String(restartPolicyCurrent.resume_stability_s ?? restartCurrent.resume_stability_s ?? 'n/a')}s</p>
            <p>polling bias: {String(restartPolicyCurrent.polling_bias ?? restartCurrent.polling_bias ?? 'n/a')}</p>
            <p>restart delay: {String(restartPolicyCurrent.restart_delay_s ?? restartCurrent.restart_delay_s ?? 'n/a')}s</p>
          </div>
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <div className="mb-2 flex flex-wrap items-center gap-2">
              <p className="font-medium text-primary/90">Restart Policy Drift</p>
              {restartPolicyDiagnostics.recommended_profile ? (
                <Badge variant="outline">profile:{String(restartPolicyDiagnostics.recommended_profile)}</Badge>
              ) : null}
              {restartPolicyDiagnostics.profile_action ? (
                <Badge variant="outline">action:{String(restartPolicyDiagnostics.profile_action)}</Badge>
              ) : null}
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="ml-auto h-7 px-2 text-[11px]"
                onClick={() => onRefreshRestartPolicyHistory?.()}
                disabled={restartPolicyHistoryBusy}
              >
                {restartPolicyHistoryBusy ? 'Refreshing...' : 'Refresh Policy Drift'}
              </Button>
            </div>
            <p>avg threshold bias: {String(restartPolicyDiagnostics.avg_threshold_bias ?? restartPolicyCurrent.threshold_bias ?? 'n/a')}</p>
            <p>avg cooldown scale: {String(restartPolicyDiagnostics.avg_cooldown_scale ?? restartPolicyCurrent.cooldown_scale ?? 'n/a')}</p>
            <p>avg recovery credit: {String(restartPolicyDiagnostics.avg_recovery_credit ?? restartPolicyCurrent.recovery_credit ?? 'n/a')}</p>
            <p>drift score: {String(restartPolicyDiagnostics.drift_score ?? restartPolicyCurrent.drift_score ?? 'n/a')}</p>
            <p>profile reason: {String(restartPolicyDiagnostics.profile_reason ?? restartPolicyCurrent.profile_reason ?? 'n/a')}</p>
            <p>latest recorded: {restartPolicyDiagnostics.latest_recorded_at ? formatDateTime(restartPolicyDiagnostics.latest_recorded_at) : 'n/a'}</p>
            <p>policy history: {String(restartPolicyHistoryRecord.history_path ?? 'n/a')}</p>
          </div>
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="mb-2 font-medium text-primary/90">Restart Profile Trend</p>
            <p>transitions: {String(restartPolicyDiagnostics.profile_transition_count ?? 0)}</p>
            <p>current profile: {String(restartPolicyDiagnostics.recommended_profile ?? restartPolicyCurrent.recommended_profile ?? 'n/a')}</p>
            <p>current action: {String(restartPolicyDiagnostics.profile_action ?? restartPolicyCurrent.profile_action ?? 'n/a')}</p>
            <div className="mt-2 space-y-1">
              {restartProfileTimeline.slice(-5).map((item, index) => (
                  <div
                    key={`wakeword-restart-profile-trend-${String(item.bucket_start ?? index)}`}
                    className="rounded border border-primary/10 bg-background/20 p-2"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <span>{formatDateTime(item.bucket_start)}</span>
                      <Badge variant="outline">{String(item.recommended_profile ?? 'unknown')}</Badge>
                    </div>
                    <p>
                      action: {String(item.profile_action ?? 'n/a')} | drift: {String(item.drift_score ?? 'n/a')}
                    </p>
                    <p>
                      exhausted: {String(item.exhausted_count ?? 0)} | recovered: {String(item.recovered_count ?? 0)}
                    </p>
                  </div>
                ))}
              {restartProfileTimeline.length === 0 ? <p>No restart profile drift buckets yet.</p> : null}
            </div>
          </div>
        </div>

        <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Exhaustion Transitions</p>
            <p>transitions: {String(restartDiagnostics.exhaustion_transition_count ?? 0)}</p>
            <p>latest exhausted: {restartDiagnostics.latest_exhausted_at ? formatDateTime(restartDiagnostics.latest_exhausted_at) : 'n/a'}</p>
            <p>
              latest exhausted until:{' '}
              {restartDiagnostics.latest_exhausted_until ? formatDateTime(restartDiagnostics.latest_exhausted_until) : 'n/a'}
            </p>
          </div>
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Recovery Expiry</p>
            <p>expiry events: {String(restartDiagnostics.recovery_expiry_events ?? restartCurrent.recovery_expiry_count ?? 0)}</p>
            <p>
              latest expiry:{' '}
              {restartDiagnostics.latest_recovery_expiry_at ? formatDateTime(restartDiagnostics.latest_recovery_expiry_at) : 'n/a'}
            </p>
            <p>
              policy expiry window: {String(restartPolicy.recovery_expiry_s ?? 'n/a')}s | resume stability:{' '}
              {String(restartPolicy.recommended_resume_stability_s ?? 'n/a')}s
            </p>
          </div>
        </div>

        <div className="mt-2 rounded border border-primary/10 bg-background/20 p-2">
          <p className="mb-2 font-medium text-primary/90">Restart Policy Timeline</p>
          <div className="space-y-1">
            {restartPolicyBuckets.slice(-6).map((bucket, index) => (
              <div
                key={`wakeword-restart-policy-bucket-${String(bucket.bucket_start ?? index)}`}
                className="rounded border border-primary/10 bg-background/20 p-2"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span>{formatDateTime(bucket.bucket_start)}</span>
                  <span>events:{String(bucket.count ?? 0)}</span>
                </div>
                <p>
                  threshold:{String(bucket.avg_threshold_bias ?? 'n/a')} | cooldown:
                  {String(bucket.avg_cooldown_scale ?? 'n/a')} | recovery:
                  {String(bucket.avg_recovery_credit ?? 'n/a')}
                </p>
                <p>
                  fallback:{String(bucket.avg_fallback_interval_s ?? 'n/a')}s | resume:
                  {String(bucket.avg_resume_stability_s ?? 'n/a')}s | sensitivity:
                  {String(bucket.avg_wakeword_sensitivity ?? 'n/a')}
                </p>
              </div>
            ))}
            {restartPolicyBuckets.length === 0 ? <p>No restart policy drift recorded yet.</p> : null}
          </div>
        </div>

        <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="mb-2 font-medium text-primary/90">Restart Drift</p>
            <div className="space-y-1">
              {restartBuckets.slice(-6).map((bucket, index) => (
                <div
                  key={`wakeword-restart-bucket-${String(bucket.bucket_start ?? index)}`}
                  className="rounded border border-primary/10 bg-background/20 p-2"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span>{formatDateTime(bucket.bucket_start)}</span>
                    <span>events:{String(bucket.count ?? 0)}</span>
                  </div>
                  <p>
                    failures:{String(bucket.failure_count ?? 0)} | recovered:{String(bucket.recovered_count ?? 0)} |
                    exhausted:{String(bucket.exhausted_count ?? 0)}
                  </p>
                  <p>
                    expiry:{String(bucket.expiry_count ?? 0)} | transitions:
                    {String(bucket.exhaustion_transition_count ?? 0)}
                  </p>
                </div>
              ))}
              {restartBuckets.length === 0 ? <p>No wakeword restart buckets yet.</p> : null}
            </div>
          </div>

          <div className="rounded border border-primary/10 bg-background/20 p-2">
            <p className="mb-2 font-medium text-primary/90">Recent Restart Events</p>
            <div className="space-y-1">
              {restartItems.slice(-6).reverse().map((item, index) => (
                <div
                  key={`wakeword-restart-event-${String(item.event_id ?? index)}`}
                  className="rounded border border-primary/10 bg-background/20 p-2"
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline">{String(item.event_type ?? 'unknown')}</Badge>
                    <span>{formatDateTime(item.occurred_at)}</span>
                  </div>
                  <p className="mt-1">
                    status: {formatStatus(item.status ?? 'unknown')} | failures:{' '}
                    {String(item.failure_count ?? 0)}
                  </p>
                  <p>
                    delay: {String(item.restart_delay_s ?? 0)}s | retry:{' '}
                    {item.next_retry_at ? formatDateTime(item.next_retry_at) : 'n/a'}
                  </p>
                  <p>
                    exhausted until: {item.exhausted_until ? formatDateTime(item.exhausted_until) : 'n/a'} | policy expiry:{' '}
                    {String(asRecord(item.policy).recovery_expiry_s ?? 'n/a')}s
                  </p>
                  <p>
                    recovered: {Boolean(item.recovered) ? 'yes' : 'no'} | exhausted:{' '}
                    {Boolean(item.exhausted) ? 'yes' : 'no'}
                  </p>
                  {item.reason || item.reason_code ? <p>reason: {String(item.reason ?? item.reason_code ?? '')}</p> : null}
                </div>
              ))}
              {restartItems.length === 0 ? <p>No wakeword restart events recorded yet.</p> : null}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default VoiceWakewordSupervisionPanel;
