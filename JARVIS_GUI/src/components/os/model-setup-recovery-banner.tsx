'use client';

import React from 'react';
import { BrainCircuit, Loader2, PlayCircle, RefreshCw, Sparkles, Trash2, Workflow } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import useModelSetupRecovery from '@/hooks/use-model-setup-recovery';
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';

interface ModelSetupRecoveryBannerProps {
  className?: string;
  compact?: boolean;
}

function formatTimestamp(value: string | undefined): string {
  const normalized = String(value ?? '').trim();
  if (!normalized) return 'n/a';
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return normalized;
  return parsed.toLocaleString();
}

function formatMissionScopeLabel(mission: { manifest_path?: string; workspace_root?: string } | null | undefined): string {
  const manifestPath = String(mission?.manifest_path ?? '').trim().replace(/\\/g, '/');
  const workspaceRoot = String(mission?.workspace_root ?? '').trim().replace(/\\/g, '/');
  if (!manifestPath && !workspaceRoot) return '';
  const manifestName = manifestPath.split('/').filter(Boolean).pop() ?? manifestPath;
  const workspaceName = workspaceRoot.split('/').filter(Boolean).pop() ?? workspaceRoot;
  if (workspaceName && manifestName) {
    return `${workspaceName} :: ${manifestName}`;
  }
  return manifestName || workspaceName;
}

function statusVariant(status: string): 'outline' | 'secondary' | 'destructive' {
  const normalized = status.trim().toLowerCase();
  if (normalized === 'resume_ready' || normalized === 'ready') return 'secondary';
  if (normalized === 'running') return 'outline';
  return 'destructive';
}

export default function ModelSetupRecoveryBanner({
  className,
  compact = false,
}: ModelSetupRecoveryBannerProps) {
  const { toast } = useToast();
  const {
    latestMission,
    resumeAdvice,
    watchdogHistory,
    latestWatchdogRun,
    autoResumeCandidateCount,
    resumeReadyCount,
    manualAttentionCount,
    runningCount,
    stalledCount,
    watchActiveRuns,
    dynamicPollMs,
    loading,
    resuming,
    sweeping,
    watchdogging,
    refreshSnapshot,
    autoResumeLatestMission,
    resumeLatestMission,
    sweepRecovery,
    watchdogRecovery,
    resetWatchdogHistory,
  } = useModelSetupRecovery();

  if (!latestMission && !loading) {
    return null;
  }

  const summaryText =
    String(resumeAdvice?.message ?? '').trim() ||
    String(latestMission?.auto_resume_reason ?? '').trim() ||
    String(latestMission?.recovery_hint ?? '').trim() ||
    String(latestMission?.latest_result_message ?? '').trim() ||
    'A local model setup mission is waiting for recovery.';
  const missionId = String(latestMission?.mission_id ?? '').trim() || 'model-setup-mission';
  const recoveryProfile = String(latestMission?.recovery_profile ?? '').trim();
  const resumeTrigger = String(
    resumeAdvice?.resume_trigger ?? latestMission?.resume_trigger ?? ''
  ).trim();
  const updatedAt = formatTimestamp(
    String(latestMission?.updated_at ?? latestMission?.created_at ?? '')
  );
  const scopeLabel = formatMissionScopeLabel(latestMission);
  const canAutoResume = Boolean(resumeAdvice?.can_auto_resume_now);
  const canResume = Boolean(resumeAdvice?.can_resume_now);
  const activeRunHealth = String(
    resumeAdvice?.active_run_health ?? latestMission?.active_run_health ?? ''
  ).trim();
  const watchdogCount = Number(watchdogHistory?.count ?? 0);
  const latestWatchdogSummary = String(latestWatchdogRun?.message ?? '').trim();
  const latestWatchdogUpdatedAt = formatTimestamp(
    String(latestWatchdogRun?.updated_at ?? latestWatchdogRun?.created_at ?? '')
  );

  return (
    <div
      className={cn(
        'rounded-xl border border-cyan-400/25 bg-[linear-gradient(135deg,rgba(34,211,238,0.14),rgba(0,0,0,0.24))] p-3 text-primary/90 shadow-[0_0_24px_rgba(34,211,238,0.08)] backdrop-blur-sm',
        className
      )}
    >
      <div className={cn('flex gap-3', compact ? 'flex-col' : 'items-start justify-between')}>
        <div className="min-w-0 flex-1 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <BrainCircuit className="h-4 w-4 text-cyan-200/90" />
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-cyan-100/90">
              Local Model Recovery
            </p>
            <Badge variant={statusVariant(String(latestMission?.status ?? 'blocked'))}>
              {String(latestMission?.status ?? 'blocked')}
            </Badge>
            {recoveryProfile ? <Badge variant="outline">{recoveryProfile}</Badge> : null}
            {resumeTrigger ? <Badge variant="outline">trigger:{resumeTrigger}</Badge> : null}
            {autoResumeCandidateCount > 0 ? (
              <Badge variant="secondary">auto:{autoResumeCandidateCount}</Badge>
            ) : null}
            {resumeReadyCount > 0 ? <Badge variant="secondary">ready:{resumeReadyCount}</Badge> : null}
            {manualAttentionCount > 0 ? <Badge variant="outline">review:{manualAttentionCount}</Badge> : null}
            {runningCount > 0 ? <Badge variant="outline">running:{runningCount}</Badge> : null}
            {stalledCount > 0 ? <Badge variant="destructive">stalled:{stalledCount}</Badge> : null}
            {watchActiveRuns ? <Badge variant="outline">watch</Badge> : null}
          </div>
          <p className="text-sm text-primary/90">{summaryText}</p>
          <p className="text-[11px] text-muted-foreground">
            mission: {missionId}
            {scopeLabel ? ` • scope: ${scopeLabel}` : ''}
            {' • '}updated: {updatedAt}
            {activeRunHealth ? ` • health: ${activeRunHealth}` : ''}
            {dynamicPollMs > 0 ? ` • poll: ${Math.round(dynamicPollMs / 1000)}s` : ''}
            {watchdogCount > 0 ? ` • watchdog runs: ${watchdogCount}` : ''}
          </p>
          {watchdogCount > 0 ? (
            <p className="text-[11px] text-muted-foreground">
              latest watchdog: {String(latestWatchdogRun?.status ?? 'unknown')}
              {' • '}updated: {latestWatchdogUpdatedAt}
              {latestWatchdogSummary ? ` • ${latestWatchdogSummary}` : ''}
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
                const count = Number(payload?.snapshot?.count ?? 0);
                toast({
                  title: 'Model Recovery Refreshed',
                  description:
                    count > 0
                      ? `${count} local-model recovery record(s) are being tracked.`
                      : 'No stored local-model recovery missions are active right now.',
                });
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Model Recovery Refresh Failed',
                  description:
                    error instanceof Error
                      ? error.message
                      : 'JARVIS could not refresh local-model recovery state.',
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
                  title: 'No Stored Setup Mission',
                  description: 'There is no stored local-model setup mission available right now.',
                });
                return;
              }
              try {
                const response = canAutoResume
                  ? await autoResumeLatestMission()
                  : canResume
                    ? await resumeLatestMission()
                    : null;
                if (!response) {
                  toast({
                    variant: 'destructive',
                    title: 'Model Recovery Waiting',
                    description:
                      String(resumeAdvice?.message ?? '').trim() ||
                      'The local-model setup mission is not ready to resume yet.',
                  });
                  return;
                }
                if (String(response.status ?? '').trim().toLowerCase() === 'success') {
                  toast({
                    title: canAutoResume ? 'Model Setup Auto-Resumed' : 'Model Setup Resumed',
                    description:
                      String(response.message ?? '').trim() ||
                      `JARVIS continued ${String(latestMission.mission_id ?? 'the stored model setup mission')}.`,
                  });
                  return;
                }
                toast({
                  variant: 'destructive',
                  title: 'Model Recovery Blocked',
                  description:
                    String(response.message ?? '').trim() ||
                    'JARVIS could not continue the stored local-model setup mission.',
                });
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Model Recovery Error',
                  description:
                    error instanceof Error
                      ? error.message
                      : 'JARVIS hit an error while continuing the local-model setup mission.',
                });
              }
            }}
            disabled={resuming || (!canAutoResume && !canResume)}
          >
            {resuming ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : canAutoResume ? (
              <Sparkles className="h-3.5 w-3.5" />
            ) : (
              <PlayCircle className="h-3.5 w-3.5" />
            )}
            {canAutoResume ? 'Auto Resume' : 'Resume'}
          </Button>
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const payload = await watchdogRecovery({
                  missionId: String(latestMission?.mission_id ?? '').trim() || undefined,
                  currentScope: !String(latestMission?.mission_id ?? '').trim(),
                  maxMissions: 6,
                  maxAutoResumes: 2,
                  continueFollowupActions: true,
                  maxFollowupWaves: 3,
                });
                if (!payload) {
                  toast({
                    variant: 'destructive',
                    title: 'Recovery Watchdog Unavailable',
                    description: 'JARVIS could not start the local-model recovery watchdog.',
                  });
                  return;
                }
                const triggered = Number(payload.auto_resume_triggered_count ?? 0);
                const watched = Number(payload.watch_count ?? 0);
                const stalled = Number(payload.stalled_count ?? 0);
                toast(
                  triggered > 0
                    ? {
                        title: 'Recovery Watchdog Continued Work',
                        description:
                          String(payload.message ?? '').trim() ||
                          `JARVIS auto-resumed ${triggered} stored setup mission${triggered === 1 ? '' : 's'}.`,
                      }
                    : watched > 0 || stalled > 0
                      ? {
                          title: 'Recovery Watchdog Watching',
                          description:
                            String(payload.message ?? '').trim() ||
                            `Watch:${watched} • stalled:${stalled}`,
                        }
                      : {
                          title: 'Recovery Watchdog Idle',
                          description:
                            String(payload.message ?? '').trim() ||
                            'No stored local-model setup missions needed automatic continuation.',
                        }
                );
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Recovery Watchdog Failed',
                  description:
                    error instanceof Error
                      ? error.message
                      : 'JARVIS hit an error while running the local-model recovery watchdog.',
                });
              }
            }}
            disabled={watchdogging || loading}
          >
            {watchdogging ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <BrainCircuit className="h-3.5 w-3.5" />}
            Watchdog
          </Button>
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const payload = await resetWatchdogHistory();
                toast({
                  title: 'Watchdog History Cleared',
                  description:
                    Number(payload?.removed ?? 0) > 0
                      ? `${Number(payload?.removed ?? 0)} watchdog run${Number(payload?.removed ?? 0) === 1 ? '' : 's'} cleared for this scope.`
                      : 'There were no stored watchdog runs to clear for this scope.',
                });
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Watchdog History Reset Failed',
                  description:
                    error instanceof Error
                      ? error.message
                      : 'JARVIS could not clear the stored watchdog history.',
                });
              }
            }}
            disabled={loading || watchdogging || watchdogCount <= 0}
          >
            <Trash2 className="h-3.5 w-3.5" />
            Clear Watchdog
          </Button>
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const payload = await sweepRecovery({
                  missionId: String(latestMission?.mission_id ?? '').trim() || undefined,
                  currentScope: !String(latestMission?.mission_id ?? '').trim(),
                  maxAutoResumePasses: 3,
                  continueFollowupActions: true,
                  maxFollowupWaves: 3,
                });
                if (!payload) {
                  toast({
                    variant: 'destructive',
                    title: 'Recovery Sweep Unavailable',
                    description: 'JARVIS could not start the local-model recovery sweep.',
                  });
                  return;
                }
                const triggered = Number(payload.auto_resume_triggered_count ?? 0);
                toast(
                  triggered > 0
                    ? {
                        title: 'Recovery Sweep Completed',
                        description:
                          String(payload.message ?? '').trim() ||
                          `JARVIS auto-resumed ${triggered} setup recovery pass${triggered === 1 ? '' : 'es'}.`,
                      }
                    : {
                        title: 'Recovery Sweep Idle',
                        description:
                          String(payload.message ?? '').trim() ||
                          'No auto-resumable local-model setup work was ready during the sweep.',
                      }
                );
              } catch (error) {
                toast({
                  variant: 'destructive',
                  title: 'Recovery Sweep Failed',
                  description:
                    error instanceof Error
                      ? error.message
                      : 'JARVIS hit an error while sweeping local-model recovery.',
                });
              }
            }}
            disabled={sweeping || loading || watchdogging}
          >
            {sweeping ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Workflow className="h-3.5 w-3.5" />}
            Sweep
          </Button>
        </div>
      </div>
    </div>
  );
}
