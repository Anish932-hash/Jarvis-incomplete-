'use client';

import React from 'react';
import { BrainCircuit, Loader2, RefreshCw, ShieldAlert, Sparkles, TerminalSquare } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import useDesktopRecovery from '@/hooks/use-desktop-recovery';
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';

interface DesktopRecoveryBannerProps {
  className?: string;
  compact?: boolean;
}

function formatMissionTimestamp(value: string | undefined): string {
  const normalized = String(value ?? '').trim();
  if (!normalized) return 'n/a';
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return normalized;
  return parsed.toLocaleString();
}

function missionStatusVariant(status: string): 'outline' | 'secondary' | 'destructive' {
  const normalized = status.trim().toLowerCase();
  if (normalized === 'paused') return 'outline';
  if (normalized === 'completed') return 'secondary';
  return 'destructive';
}

export default function DesktopRecoveryBanner({ className, compact = false }: DesktopRecoveryBannerProps) {
  const { toast } = useToast();
  const {
    latestMission,
    latestWatchdogRun,
    watchdogHistory,
    supervisorStatus,
    backendSupervisorEnabled,
    additionalMissionCount,
    resumeReadyCount,
    explorationMissionCount,
    explorationResumeReadyCount,
    manualAttentionCount,
    loading,
    resuming,
    configuringDaemon,
    triggeringDaemon,
    refreshSnapshot,
    resumeLatestMission,
    configureRecoveryDaemon,
    triggerRecoveryDaemon,
    resetWatchdogHistory,
  } = useDesktopRecovery();

  if (!latestMission && !loading && !supervisorStatus) {
    return null;
  }

  const summaryText =
    String(latestMission?.stop_reason ?? '').trim() ||
    String(latestMission?.recovery_hint ?? '').trim() ||
    String(supervisorStatus?.last_result_message ?? '').trim() ||
    String(latestMission?.latest_result_message ?? '').trim() ||
    (backendSupervisorEnabled
      ? 'The desktop recovery daemon is monitoring paused Windows tasks in the background.'
      : 'A paused desktop mission is waiting for operator-approved recovery.');
  const missionId = String(latestMission?.mission_id ?? '').trim() || 'desktop-mission';
  const missionKind = String(latestMission?.mission_kind ?? 'mission').trim() || 'mission';
  const missionKindLabel =
    missionKind === 'exploration'
      ? 'surface recon'
      : missionKind;
  const isExplorationMission = missionKind === 'exploration';
  const missionApp = String(latestMission?.app_name ?? latestMission?.anchor_window_title ?? 'desktop').trim() || 'desktop';
  const approvalKind = String(latestMission?.approval_kind ?? '').trim();
  const recoveryProfile = String(latestMission?.recovery_profile ?? '').trim();
  const explorationStepCount = Number(latestMission?.step_count ?? 0);
  const explorationMaxSteps = Number(latestMission?.max_steps ?? 0);
  const explorationAttemptedCount = Number(latestMission?.attempted_target_count ?? 0);
  const explorationAlternativeCount = Number(latestMission?.alternative_target_count ?? 0);
  const explorationBranchCount = Number(latestMission?.branch_transition_count ?? 0);
  const explorationLastBranchKind = String(latestMission?.last_branch_kind ?? '').trim();
  const explorationBranchRepeatCount = Number(latestMission?.branch_repeat_count ?? 0);
  const explorationSurfaceDepth = Number(latestMission?.surface_path_depth ?? 0);
  const explorationRustRouterHint = String(latestMission?.rust_router_hint ?? '').trim();
  const explorationTopologyVisibleWindowCount = Number(latestMission?.topology_visible_window_count ?? 0);
  const explorationTopologyDialogLikeCount = Number(latestMission?.topology_dialog_like_count ?? 0);
  const explorationTopologySameProcessWindowCount = Number(latestMission?.topology_same_process_window_count ?? 0);
  const explorationTopologySignature = String(latestMission?.surface_topology_signature ?? '').trim();
  const updatedAt = formatMissionTimestamp(String(latestMission?.updated_at ?? latestMission?.created_at ?? ''));
  const daemonLastTick = formatMissionTimestamp(String(supervisorStatus?.last_tick_at ?? ''));
  const daemonIntervalS = Number(supervisorStatus?.interval_s ?? 0);
  const watchdogUpdatedAt = formatMissionTimestamp(
    String(latestWatchdogRun?.updated_at ?? latestWatchdogRun?.created_at ?? '')
  );

  return (
    <div
      className={cn(
        'rounded-xl border border-amber-400/25 bg-[linear-gradient(135deg,rgba(255,184,77,0.14),rgba(0,0,0,0.25))] p-3 text-primary/90 shadow-[0_0_24px_rgba(255,184,77,0.08)] backdrop-blur-sm',
        className
      )}
    >
      <div className={cn('flex gap-3', compact ? 'flex-col' : 'items-start justify-between')}>
        <div className="min-w-0 flex-1 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <ShieldAlert className="h-4 w-4 text-amber-300/90" />
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-amber-100/90">
              Desktop Recovery Waiting
            </p>
            <Badge variant={missionStatusVariant(String(latestMission?.status ?? 'paused'))}>
              {String(latestMission?.status ?? 'paused')}
            </Badge>
            <Badge variant="secondary">{missionKindLabel}</Badge>
            <Badge variant={backendSupervisorEnabled ? 'secondary' : 'outline'}>
              daemon:{backendSupervisorEnabled ? 'on' : 'off'}
            </Badge>
            {recoveryProfile ? <Badge variant="outline">{recoveryProfile}</Badge> : null}
            {approvalKind ? <Badge variant="outline">approval:{approvalKind}</Badge> : null}
            {resumeReadyCount > 0 ? <Badge variant="secondary">ready:{resumeReadyCount}</Badge> : null}
            {explorationMissionCount > 0 ? <Badge variant="outline">recon:{explorationMissionCount}</Badge> : null}
            {explorationResumeReadyCount > 0 ? <Badge variant="secondary">recon-ready:{explorationResumeReadyCount}</Badge> : null}
            {isExplorationMission && explorationAlternativeCount > 0 ? (
              <Badge variant="outline">alts:{explorationAlternativeCount}</Badge>
            ) : null}
            {isExplorationMission && explorationBranchCount > 0 ? (
              <Badge variant="outline">
                branches:{explorationBranchCount}
                {explorationLastBranchKind ? `:${explorationLastBranchKind}` : ''}
                {explorationBranchRepeatCount > 1 ? `:${explorationBranchRepeatCount}x` : ''}
              </Badge>
            ) : null}
            {isExplorationMission && explorationRustRouterHint ? (
              <Badge variant="outline">rust:{explorationRustRouterHint}</Badge>
            ) : null}
            {isExplorationMission &&
            (explorationTopologyVisibleWindowCount > 0 ||
              explorationTopologyDialogLikeCount > 0 ||
              explorationTopologySameProcessWindowCount > 0) ? (
              <Badge variant="outline">
                topo:{explorationTopologyVisibleWindowCount}/{explorationTopologyDialogLikeCount}/{explorationTopologySameProcessWindowCount}
              </Badge>
            ) : null}
            {manualAttentionCount > 0 ? <Badge variant="outline">review:{manualAttentionCount}</Badge> : null}
            {additionalMissionCount > 0 ? <Badge variant="outline">+{additionalMissionCount} more</Badge> : null}
          </div>
          <p className="text-sm text-primary/90">
            {missionApp} :: {summaryText}
          </p>
          <p className="text-[11px] text-muted-foreground">
            mission: {missionId} {' • '}updated: {updatedAt}
            {missionKind === 'exploration' && explorationStepCount > 0
              ? ` • steps:${explorationStepCount}${explorationMaxSteps > 0 ? `/${explorationMaxSteps}` : ''}`
              : ''}
            {missionKind === 'exploration' && explorationAttemptedCount > 0
              ? ` • attempted:${explorationAttemptedCount}`
              : ''}
            {missionKind === 'exploration' && explorationAlternativeCount > 0
              ? ` • alts:${explorationAlternativeCount}`
              : ''}
            {missionKind === 'exploration' && explorationBranchCount > 0
              ? ` • branches:${explorationBranchCount}${explorationLastBranchKind ? `(${explorationLastBranchKind})` : ''}${explorationBranchRepeatCount > 1 ? `:${explorationBranchRepeatCount}x` : ''}`
              : ''}
            {missionKind === 'exploration' && explorationSurfaceDepth > 0
              ? ` • depth:${explorationSurfaceDepth}`
              : ''}
            {missionKind === 'exploration' && explorationRustRouterHint
              ? ` • rust:${explorationRustRouterHint}`
              : ''}
            {missionKind === 'exploration' &&
            (explorationTopologyVisibleWindowCount > 0 ||
              explorationTopologyDialogLikeCount > 0 ||
              explorationTopologySameProcessWindowCount > 0)
              ? ` • topo:${explorationTopologyVisibleWindowCount}/${explorationTopologyDialogLikeCount}/${explorationTopologySameProcessWindowCount}${explorationTopologySignature ? `(${explorationTopologySignature})` : ''}`
              : ''}
            {latestMission?.resume_action ? ` • resume:${String(latestMission.resume_action)}` : ''}
            {supervisorStatus ? ` • daemon tick:${daemonLastTick}` : ''}
            {backendSupervisorEnabled && daemonIntervalS > 0 ? ` • interval:${Math.round(daemonIntervalS)}s` : ''}
            {latestWatchdogRun ? ` • watchdog:${watchdogUpdatedAt}` : ''}
          </p>
          {latestWatchdogRun ? (
            <p className="text-[11px] text-muted-foreground">
              watchdog: {String(latestWatchdogRun.status ?? 'idle').trim() || 'idle'}
              {` • triggered:${Number(latestWatchdogRun.auto_resume_triggered_count ?? 0)}`}
              {` • blocked:${Number(latestWatchdogRun.blocked_count ?? 0)}`}
              {` • errors:${Number(latestWatchdogRun.error_count ?? 0)}`}
              {String(latestWatchdogRun.message ?? '').trim()
                ? ` • ${String(latestWatchdogRun.message ?? '').trim()}`
                : ''}
            </p>
          ) : null}
        </div>
        <div className="flex shrink-0 flex-wrap items-center gap-2">
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const payload = await refreshSnapshot();
                const refreshedSnapshot =
                  payload?.snapshot && typeof payload.snapshot === 'object' && !Array.isArray(payload.snapshot)
                    ? payload.snapshot
                    : null;
                if (refreshedSnapshot && Number(refreshedSnapshot.count ?? 0) > 0) {
                  toast({
                    title: 'Desktop Recoveries Refreshed',
                    description: `${Number(refreshedSnapshot.count ?? 0)} paused desktop mission record(s) are waiting for recovery.`,
                  });
                }
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Desktop Recovery Refresh Failed',
                  description: error instanceof Error ? error.message : 'JARVIS could not refresh paused desktop missions.',
                });
              }
            }}
            disabled={loading}
          >
            {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
            Refresh
          </Button>
          <Button
            type="button"
            className="h-8 gap-2 px-2 text-xs"
            onClick={async () => {
              if (!latestMission) {
                toast({
                  variant: 'destructive',
                  title: 'No Paused Desktop Mission',
                  description: 'There is no paused desktop mission available to resume right now.',
                });
                return;
              }
              try {
                const response = await resumeLatestMission();
                if (!response) {
                  toast({
                    variant: 'destructive',
                    title: 'Desktop Recovery Blocked',
                    description: 'JARVIS could not resume the latest desktop mission.',
                  });
                  return;
                }
                if (String(response.status ?? '').trim().toLowerCase() === 'success') {
                  toast({
                    title: 'Desktop Recovery Resumed',
                    description:
                      String(response.final_action ?? '').trim() ||
                      `JARVIS resumed ${String(latestMission.mission_id ?? 'the latest desktop mission')}.`,
                  });
                  return;
                }
                toast({
                  variant: 'destructive',
                  title: 'Desktop Recovery Blocked',
                  description:
                    String(response.message ?? '').trim() ||
                    `JARVIS could not resume ${String(latestMission.mission_id ?? 'the selected desktop mission')}.`,
                });
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Desktop Recovery Error',
                  description:
                    error instanceof Error
                      ? error.message
                      : 'JARVIS hit an error while resuming the latest desktop mission.',
                });
              }
            }}
            disabled={resuming || !latestMission}
          >
            {resuming ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <TerminalSquare className="h-3.5 w-3.5" />}
            {isExplorationMission ? 'Resume Recon' : 'Resume Latest'}
          </Button>
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const payload = await configureRecoveryDaemon({
                  enabled: !backendSupervisorEnabled,
                  intervalS: daemonIntervalS > 0 ? daemonIntervalS : 45,
                  missionStatus: 'paused',
                });
                toast({
                  title: backendSupervisorEnabled ? 'Desktop Daemon Disabled' : 'Desktop Daemon Enabled',
                  description:
                    String(payload?.last_result_message ?? '').trim() ||
                    (backendSupervisorEnabled
                      ? 'JARVIS stopped background desktop recovery ticks.'
                      : 'JARVIS will now run bounded desktop recovery ticks in the background.'),
                });
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Desktop Daemon Update Failed',
                  description:
                    error instanceof Error ? error.message : 'JARVIS could not update the desktop recovery daemon.',
                });
              }
            }}
            disabled={loading || configuringDaemon}
          >
            <BrainCircuit className="h-3.5 w-3.5" />
            {backendSupervisorEnabled ? 'Disable Daemon' : 'Enable Daemon'}
          </Button>
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const payload = await triggerRecoveryDaemon({
                  missionStatus: 'paused',
                  missionKind: isExplorationMission ? 'exploration' : undefined,
                  maxAutoResumes: isExplorationMission ? 1 : undefined,
                });
                toast({
                  title: isExplorationMission ? 'Recon Sweep Triggered' : 'Desktop Daemon Triggered',
                  description:
                    String(payload?.message ?? '').trim() ||
                    (isExplorationMission
                      ? 'JARVIS ran a bounded recovery sweep for paused surface recon missions.'
                      : 'JARVIS ran a bounded desktop recovery tick.'),
                });
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: isExplorationMission ? 'Recon Sweep Failed' : 'Desktop Daemon Trigger Failed',
                  description:
                    error instanceof Error ? error.message : 'JARVIS could not trigger the desktop recovery daemon.',
                });
              }
            }}
            disabled={loading || triggeringDaemon}
          >
            {triggeringDaemon ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Sparkles className="h-3.5 w-3.5" />}
            {isExplorationMission ? 'Sweep Recon' : 'Trigger Daemon'}
          </Button>
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const response = await resetWatchdogHistory();
                toast({
                  title: 'Desktop Watchdog History Cleared',
                  description: `${Number(response?.removed ?? 0)} daemon run record(s) were removed.`,
                });
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Desktop Watchdog Reset Failed',
                  description:
                    error instanceof Error ? error.message : 'JARVIS could not clear desktop daemon history.',
                });
              }
            }}
            disabled={Number(watchdogHistory?.count ?? 0) === 0}
          >
            <RefreshCw className="h-3.5 w-3.5" />
            Clear Watchdog
          </Button>
        </div>
      </div>
    </div>
  );
}
