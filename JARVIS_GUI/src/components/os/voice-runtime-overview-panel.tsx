'use client';

import React from 'react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import VoiceContinuousRecoveryCard from './voice-continuous-recovery-card';
import VoiceRecoveryHistoryPanel from './voice-recovery-history-panel';
import VoiceWakewordSupervisionPanel from './voice-wakeword-supervision-panel';

type RecordLike = Record<string, unknown>;

export interface VoiceRuntimeOverviewPanelProps {
  voiceSessionState: RecordLike | null;
  voiceLastError: string;
  voiceTranscript: string;
  voiceReply: string;
  ttsLocalHealth: string;
  ttsElevenlabsHealth: string;
  voiceDiagnosticsLastRefreshAt: unknown;
  voiceStreamConnected: boolean;
  voiceStreamLastEventAt: unknown;
  voiceRoutePolicyCurrentSummary: RecordLike;
  voiceRoutePolicyCurrentStt: RecordLike;
  voiceRoutePolicyCurrentWakeword: RecordLike;
  voiceRoutePolicyCurrentTts: RecordLike;
  voiceRoutePolicyItems: RecordLike[];
  voiceRoutePolicyHistoryDiagnostics: RecordLike;
  voiceRoutePolicyHistoryState: RecordLike;
  voiceRoutePolicyHistoryItems: RecordLike[];
  voiceRoutePolicyHistoryBuckets: RecordLike[];
  voiceMissionReliabilityState: RecordLike;
  voiceRouteRecoveryRecommendation: RecordLike;
  voiceWakewordSupervisionHistoryState: RecordLike;
  voiceWakewordRestartHistoryState: RecordLike | null;
  voiceWakewordRestartPolicyHistoryState: RecordLike | null;
  voiceWakewordRestartEventTypeFilter?: string;
  voiceWakewordRestartBusy?: boolean;
  voiceWakewordRestartPolicyBusy?: boolean;
  voiceContinuousActiveRun: RecordLike | null;
  voiceContinuousRuns: RecordLike[];
  formatRefreshStamp: (value: unknown) => string;
  formatCompactDateTime: (value: unknown) => string;
  formatVoiceRouteStatus: (value: unknown) => string;
  formatRoutePolicyReason: (value: string) => string;
  onVoiceWakewordRestartEventTypeFilterChange?: (value: string) => void;
  onRefreshVoiceWakewordRestartHistory?: () => void;
  onRefreshVoiceWakewordRestartPolicyHistory?: () => void;
}

export function VoiceRuntimeOverviewPanel({
  voiceSessionState,
  voiceLastError,
  voiceTranscript,
  voiceReply,
  ttsLocalHealth,
  ttsElevenlabsHealth,
  voiceDiagnosticsLastRefreshAt,
  voiceStreamConnected,
  voiceStreamLastEventAt,
  voiceRoutePolicyCurrentSummary,
  voiceRoutePolicyCurrentStt,
  voiceRoutePolicyCurrentWakeword,
  voiceRoutePolicyCurrentTts,
  voiceRoutePolicyItems,
  voiceRoutePolicyHistoryDiagnostics,
  voiceRoutePolicyHistoryState,
  voiceRoutePolicyHistoryItems,
  voiceRoutePolicyHistoryBuckets,
  voiceMissionReliabilityState,
  voiceRouteRecoveryRecommendation,
  voiceWakewordSupervisionHistoryState,
  voiceWakewordRestartHistoryState,
  voiceWakewordRestartPolicyHistoryState,
  voiceWakewordRestartEventTypeFilter = '',
  voiceWakewordRestartBusy = false,
  voiceWakewordRestartPolicyBusy = false,
  voiceContinuousActiveRun,
  voiceContinuousRuns,
  formatRefreshStamp,
  formatCompactDateTime,
  formatVoiceRouteStatus,
  formatRoutePolicyReason,
  onVoiceWakewordRestartEventTypeFilterChange,
  onRefreshVoiceWakewordRestartHistory,
  onRefreshVoiceWakewordRestartPolicyHistory,
}: VoiceRuntimeOverviewPanelProps) {
  const restartPolicyHistory = voiceWakewordRestartPolicyHistoryState && typeof voiceWakewordRestartPolicyHistoryState === 'object'
    ? voiceWakewordRestartPolicyHistoryState
    : {};
  const restartPolicyDiagnostics =
    restartPolicyHistory && typeof restartPolicyHistory.diagnostics === 'object' && !Array.isArray(restartPolicyHistory.diagnostics)
      ? (restartPolicyHistory.diagnostics as RecordLike)
      : {};
  const restartPolicyCurrent =
    restartPolicyHistory && typeof restartPolicyHistory.current === 'object' && !Array.isArray(restartPolicyHistory.current)
      ? (restartPolicyHistory.current as RecordLike)
      : {};
  const restartPolicyTimeline = Array.isArray(restartPolicyDiagnostics.profile_timeline)
    ? restartPolicyDiagnostics.profile_timeline.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const restartPolicyShiftTimelineSource = Array.isArray(restartPolicyCurrent.profile_shift_timeline)
    ? restartPolicyCurrent.profile_shift_timeline
    : Array.isArray(restartPolicyDiagnostics.profile_shift_timeline)
      ? restartPolicyDiagnostics.profile_shift_timeline
      : [];
  const restartPolicyShiftTimeline = restartPolicyShiftTimelineSource.filter(
    (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
  );
  const restartPolicyRuntimePosture =
    restartPolicyCurrent && typeof restartPolicyCurrent.runtime_posture === 'object' && !Array.isArray(restartPolicyCurrent.runtime_posture)
      ? (restartPolicyCurrent.runtime_posture as RecordLike)
      : restartPolicyDiagnostics && typeof restartPolicyDiagnostics.runtime_posture === 'object' && !Array.isArray(restartPolicyDiagnostics.runtime_posture)
        ? (restartPolicyDiagnostics.runtime_posture as RecordLike)
        : {};
  const missionReliability =
    voiceMissionReliabilityState && typeof voiceMissionReliabilityState === 'object' ? voiceMissionReliabilityState : {};
  const missionReliabilityItems = Array.isArray(missionReliability.items)
    ? missionReliability.items.filter(
        (item): item is RecordLike => Boolean(item && typeof item === 'object' && !Array.isArray(item))
      )
    : [];
  const latestMissionReliability = missionReliabilityItems.length > 0 ? missionReliabilityItems[0] : {};

  return (
    <div className="space-y-3 p-4" data-testid="voice-runtime-overview-panel">
      <div className="rounded-md border border-primary/20 bg-black/30 p-3">
        <div className="mb-2 flex flex-wrap items-center gap-2">
          <p className="text-sm font-medium text-primary">Voice Runtime State</p>
          <Badge variant="outline">{voiceSessionState?.running ? 'running' : 'stopped'}</Badge>
          <Badge variant="secondary">{String(voiceSessionState?.wakeword_status || 'unknown')}</Badge>
          <Badge variant={ttsLocalHealth === 'ready' ? 'outline' : ttsLocalHealth === 'degraded' ? 'destructive' : 'secondary'}>
            local:{ttsLocalHealth}
          </Badge>
          <Badge
            variant={
              ttsElevenlabsHealth === 'ready'
                ? 'outline'
                : ttsElevenlabsHealth === 'degraded'
                  ? 'destructive'
                  : 'secondary'
            }
          >
            elevenlabs:{ttsElevenlabsHealth}
          </Badge>
          <Badge variant="outline">diag:{formatRefreshStamp(voiceDiagnosticsLastRefreshAt)}</Badge>
          <Badge variant={voiceStreamConnected ? 'outline' : 'secondary'}>stream:{voiceStreamConnected ? 'live' : 'idle'}</Badge>
          <Badge variant="outline">stream_evt:{formatRefreshStamp(voiceStreamLastEventAt)}</Badge>
        </div>
        {voiceLastError ? <p className="text-xs text-destructive/90">{voiceLastError}</p> : null}
        <div className="mt-2 rounded-md border border-primary/20 bg-background/20 p-3">
          <p className="mb-2 text-xs uppercase tracking-wider text-muted-foreground">Transcript</p>
          <p className="text-sm text-foreground/90">{voiceTranscript || 'No transcript yet.'}</p>
        </div>
        <div className="mt-2 rounded-md border border-primary/20 bg-background/20 p-3">
          <p className="mb-2 text-xs uppercase tracking-wider text-muted-foreground">Reply</p>
          <p className="text-sm text-foreground/90">{voiceReply || 'No reply yet.'}</p>
        </div>
      </div>

      <div className="rounded-md border border-primary/20 bg-black/30 p-3">
        <div className="mb-2 flex flex-wrap items-center gap-2">
          <p className="text-xs uppercase tracking-wider text-muted-foreground">Cross-Session Wakeword Policy</p>
          <Badge variant="outline">
            profile:
            {String(
              restartPolicyCurrent.applied_profile ??
                restartPolicyDiagnostics.applied_profile ??
                restartPolicyDiagnostics.recommended_profile ??
                'unknown'
            )}
          </Badge>
          <Badge variant="outline">
            action:
            {String(
              restartPolicyCurrent.profile_action ??
                restartPolicyDiagnostics.profile_action ??
                'hold'
            )}
          </Badge>
          <Badge variant={Boolean(restartPolicyCurrent.auto_profile_applied ?? restartPolicyDiagnostics.auto_profile_applied) ? 'secondary' : 'outline'}>
            auto:{Boolean(restartPolicyCurrent.auto_profile_applied ?? restartPolicyDiagnostics.auto_profile_applied) ? 'on' : 'hold'}
          </Badge>
          <Badge variant="outline">
            shifts:{String(restartPolicyDiagnostics.profile_shift_count ?? restartPolicyCurrent.profile_shift_count ?? 0)}
          </Badge>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="ml-auto h-7 px-2 text-[11px]"
            onClick={() => onRefreshVoiceWakewordRestartPolicyHistory?.()}
            disabled={voiceWakewordRestartPolicyBusy}
          >
            {voiceWakewordRestartPolicyBusy ? 'Refreshing...' : 'Refresh Cross-Session Policy'}
          </Button>
        </div>
        <div className="grid grid-cols-1 gap-2 text-[11px] text-muted-foreground md:grid-cols-3">
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Applied Runtime Posture</p>
            <p>applied: {String(restartPolicyCurrent.applied_profile ?? restartPolicyDiagnostics.applied_profile ?? 'n/a')}</p>
            <p>source: {String(restartPolicyCurrent.profile_decision_source ?? restartPolicyDiagnostics.profile_decision_source ?? 'n/a')}</p>
            <p>drift: {String(restartPolicyCurrent.drift_score ?? restartPolicyDiagnostics.drift_score ?? 'n/a')}</p>
            <p>last shift: {(restartPolicyCurrent.last_profile_shift_at ?? restartPolicyDiagnostics.last_profile_shift_at) ? formatCompactDateTime(restartPolicyCurrent.last_profile_shift_at ?? restartPolicyDiagnostics.last_profile_shift_at) : 'n/a'}</p>
          </div>
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Recovery Pressure</p>
            <p>mission: {String(latestMissionReliability.mission_id ?? 'n/a')}</p>
            <p>recent exhaustion: {String(restartPolicyCurrent.recent_exhaustion_rate ?? restartPolicyDiagnostics.recent_exhaustion_rate ?? 'n/a')}</p>
            <p>recent recovery: {String(restartPolicyCurrent.recent_recovery_rate ?? restartPolicyDiagnostics.recent_recovery_rate ?? 'n/a')}</p>
            <p>route strategy: {String(voiceRouteRecoveryRecommendation.strategy ?? 'n/a')}</p>
            <p>mission score: {String(latestMissionReliability.route_recovery_score ?? latestMissionReliability.reliability_score ?? 'n/a')}</p>
          </div>
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Policy Reasoning</p>
            <p>{String(restartPolicyCurrent.applied_profile_reason ?? restartPolicyDiagnostics.applied_profile_reason ?? restartPolicyDiagnostics.profile_reason ?? 'No long-horizon wakeword policy explanation yet.')}</p>
          </div>
        </div>
        <div className="mt-2 grid grid-cols-1 gap-2 text-[11px] text-muted-foreground md:grid-cols-2">
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="mb-2 font-medium text-primary/90">Runtime Mode Overlay</p>
            <p>mode: {String(restartPolicyRuntimePosture.runtime_mode ?? restartPolicyCurrent.runtime_mode ?? 'n/a')}</p>
            <p>supervision: {String(restartPolicyRuntimePosture.wakeword_supervision_mode ?? restartPolicyCurrent.wakeword_supervision_mode ?? 'n/a')}</p>
            <p>resume: {String(restartPolicyRuntimePosture.continuous_resume_mode ?? restartPolicyCurrent.continuous_resume_mode ?? 'n/a')}</p>
            <p>barge-in: {String(restartPolicyRuntimePosture.barge_in_enabled ?? restartPolicyCurrent.barge_in_enabled ?? 'n/a')}</p>
            <p>hard barge-in: {String(restartPolicyRuntimePosture.hard_barge_in ?? restartPolicyCurrent.hard_barge_in ?? 'n/a')}</p>
          </div>
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="mb-2 font-medium text-primary/90">Profile Shift Timeline</p>
            <div className="space-y-1">
              {restartPolicyShiftTimeline.slice(-5).map((item, index) => (
                <div
                  key={`voice-runtime-policy-shift-${String(item.bucket_start ?? index)}`}
                  className="rounded border border-primary/10 bg-background/20 p-2"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span>{formatCompactDateTime(item.bucket_start)}</span>
                    <Badge variant="outline">
                      {String(item.from_profile ?? 'unknown')} -&gt; {String(item.to_profile ?? item.recommended_profile ?? 'unknown')}
                    </Badge>
                  </div>
                  <p>action: {String(item.profile_action ?? 'n/a')} | drift: {String(item.drift_score ?? 'n/a')}</p>
                </div>
              ))}
              {restartPolicyShiftTimeline.length === 0 ? (
                <p>No cross-session profile shifts recorded yet.</p>
              ) : null}
            </div>
          </div>
        </div>
        <div className="mt-2 rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
          <p className="mb-2 font-medium text-primary/90">Cross-Session Drift Trend</p>
          <div className="space-y-1">
            {restartPolicyTimeline.slice(-5).map((item, index) => (
              <div
                key={`voice-runtime-policy-trend-${String(item.bucket_start ?? index)}`}
                className="rounded border border-primary/10 bg-background/20 p-2"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span>{formatCompactDateTime(item.bucket_start)}</span>
                  <Badge variant="outline">{String(item.recommended_profile ?? 'unknown')}</Badge>
                </div>
                <p>action: {String(item.profile_action ?? 'n/a')} | drift: {String(item.drift_score ?? 'n/a')}</p>
                <p>exhausted: {String(item.exhausted_count ?? 0)} | recovered: {String(item.recovered_count ?? 0)}</p>
              </div>
            ))}
            {restartPolicyTimeline.length === 0 ? (
              <p>No cross-session wakeword drift samples recorded yet.</p>
            ) : null}
          </div>
        </div>
      </div>

      <div className="rounded-md border border-primary/20 bg-black/30 p-3">
        <div className="mb-2 flex flex-wrap items-center gap-2">
          <p className="text-xs uppercase tracking-wider text-muted-foreground">Voice Route Gate Timeline</p>
          <Badge
            variant={
              String(voiceRoutePolicyCurrentSummary.status ?? '').trim().toLowerCase() === 'blocked'
                ? 'secondary'
                : 'outline'
            }
          >
            {formatVoiceRouteStatus(voiceRoutePolicyCurrentSummary.status ?? voiceSessionState?.route_policy_status)}
          </Badge>
          <Badge variant="outline">blocks:{String(voiceSessionState?.route_policy_block_count ?? 0)}</Badge>
          <Badge variant="outline">reroutes:{String(voiceSessionState?.route_policy_reroute_count ?? 0)}</Badge>
          <Badge variant="outline">recovery:{String(voiceSessionState?.route_policy_recovery_count ?? 0)}</Badge>
        </div>
        <div className="grid grid-cols-1 gap-2 text-[11px] text-muted-foreground md:grid-cols-3">
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">STT Gate</p>
            <p>status: {formatVoiceRouteStatus(voiceRoutePolicyCurrentStt.status)}</p>
            <p>selected: {String(voiceRoutePolicyCurrentStt.selected_provider ?? 'n/a')}</p>
            <p>recommended: {String(voiceRoutePolicyCurrentStt.recommended_provider ?? 'n/a')}</p>
            <p>retry: {voiceRoutePolicyCurrentStt.next_retry_at ? formatCompactDateTime(voiceRoutePolicyCurrentStt.next_retry_at) : 'n/a'}</p>
          </div>
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">Wakeword Gate</p>
            <p>status: {formatVoiceRouteStatus(voiceRoutePolicyCurrentWakeword.status)}</p>
            <p>selected: {String(voiceRoutePolicyCurrentWakeword.selected_provider ?? 'local')}</p>
            <p>reason: {formatRoutePolicyReason(String(voiceRoutePolicyCurrentWakeword.reason_code ?? '')) || 'none'}</p>
            <p>retry: {voiceRoutePolicyCurrentWakeword.next_retry_at ? formatCompactDateTime(voiceRoutePolicyCurrentWakeword.next_retry_at) : 'n/a'}</p>
          </div>
          <div className="rounded border border-primary/20 bg-background/20 p-2">
            <p className="font-medium text-primary/90">TTS Route</p>
            <p>status: {formatVoiceRouteStatus(voiceRoutePolicyCurrentTts.status)}</p>
            <p>selected: {String(voiceRoutePolicyCurrentTts.selected_provider ?? 'n/a')}</p>
            <p>recommended: {String(voiceRoutePolicyCurrentTts.recommended_provider ?? 'n/a')}</p>
            <p>retry: {voiceRoutePolicyCurrentTts.next_retry_at ? formatCompactDateTime(voiceRoutePolicyCurrentTts.next_retry_at) : 'n/a'}</p>
          </div>
        </div>
        {voiceRoutePolicyCurrentSummary.reason || voiceRoutePolicyCurrentSummary.reason_code ? (
          <div className="mt-2 rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground">
            <p>
              current reason:{' '}
              {String(voiceRoutePolicyCurrentSummary.reason ?? '') ||
                formatRoutePolicyReason(String(voiceRoutePolicyCurrentSummary.reason_code ?? ''))}
            </p>
            {voiceRoutePolicyCurrentSummary.next_retry_at ? (
              <p>next retry: {formatCompactDateTime(voiceRoutePolicyCurrentSummary.next_retry_at)}</p>
            ) : null}
          </div>
        ) : null}
        <div className="mt-2 space-y-2">
          {voiceRoutePolicyItems.slice(-8).reverse().map((item, index) => (
            <div
              key={`voice-route-timeline-${String(item.event_id ?? index)}`}
              className="rounded border border-primary/20 bg-background/20 p-2 text-[11px] text-muted-foreground"
            >
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">{String(item.task ?? 'voice')}</Badge>
                <Badge variant={String(item.status ?? '').trim().toLowerCase() === 'blocked' ? 'secondary' : 'outline'}>
                  {formatVoiceRouteStatus(item.status)}
                </Badge>
                <p>{formatCompactDateTime(item.occurred_at)}</p>
              </div>
              <p className="mt-1">
                {String(item.previous_status ?? 'unknown')} {' -> '} {String(item.status ?? 'unknown')} • selected:{' '}
                {String(item.selected_provider ?? 'n/a')} • recommended: {String(item.recommended_provider ?? 'n/a')}
              </p>
              {item.reason_code || item.reason ? (
                <p>reason: {String(item.reason ?? '') || formatRoutePolicyReason(String(item.reason_code ?? ''))}</p>
              ) : null}
              {item.next_retry_at ? <p>retry at: {formatCompactDateTime(item.next_retry_at)}</p> : null}
            </div>
          ))}
          {voiceRoutePolicyItems.length === 0 ? (
            <p className="text-[11px] text-muted-foreground">No voice route-policy transitions recorded yet.</p>
          ) : null}
        </div>
      </div>

      <VoiceRecoveryHistoryPanel
        diagnostics={{
          ...voiceRoutePolicyHistoryDiagnostics,
          count: voiceRoutePolicyHistoryState.count ?? voiceRoutePolicyHistoryItems.length ?? 0,
        }}
        items={voiceRoutePolicyHistoryItems}
        buckets={voiceRoutePolicyHistoryBuckets}
        missionReliability={voiceMissionReliabilityState}
        routeRecoveryRecommendation={voiceRouteRecoveryRecommendation}
        formatDateTime={formatCompactDateTime}
        formatStatus={formatVoiceRouteStatus}
      />
      <VoiceWakewordSupervisionPanel
        history={voiceWakewordSupervisionHistoryState}
        restartHistory={voiceWakewordRestartHistoryState}
        restartPolicyHistory={voiceWakewordRestartPolicyHistoryState}
        activeRun={voiceContinuousActiveRun}
        formatDateTime={formatCompactDateTime}
        formatStatus={formatVoiceRouteStatus}
        restartEventTypeFilter={voiceWakewordRestartEventTypeFilter}
        restartHistoryBusy={voiceWakewordRestartBusy}
        restartPolicyHistoryBusy={voiceWakewordRestartPolicyBusy}
        onRestartEventTypeFilterChange={onVoiceWakewordRestartEventTypeFilterChange}
        onRefreshRestartHistory={onRefreshVoiceWakewordRestartHistory}
        onRefreshRestartPolicyHistory={onRefreshVoiceWakewordRestartPolicyHistory}
      />
      <VoiceContinuousRecoveryCard
        activeRun={voiceContinuousActiveRun}
        items={voiceContinuousRuns}
        formatDateTime={formatCompactDateTime}
        formatStatus={formatVoiceRouteStatus}
      />
    </div>
  );
}

export default VoiceRuntimeOverviewPanel;
