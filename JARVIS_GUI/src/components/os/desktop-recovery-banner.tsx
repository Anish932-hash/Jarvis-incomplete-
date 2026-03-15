'use client';

import React from 'react';
import { RefreshCw, ShieldAlert, TerminalSquare, Loader2 } from 'lucide-react';

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
    additionalMissionCount,
    resumeReadyCount,
    manualAttentionCount,
    loading,
    resuming,
    refreshSnapshot,
    resumeLatestMission,
  } = useDesktopRecovery();

  if (!latestMission && !loading) {
    return null;
  }

  const summaryText =
    String(latestMission?.stop_reason ?? '').trim() ||
    String(latestMission?.recovery_hint ?? '').trim() ||
    String(latestMission?.latest_result_message ?? '').trim() ||
    'A paused desktop mission is waiting for operator-approved recovery.';
  const missionId = String(latestMission?.mission_id ?? '').trim() || 'desktop-mission';
  const missionKind = String(latestMission?.mission_kind ?? 'mission').trim() || 'mission';
  const missionApp = String(latestMission?.app_name ?? latestMission?.anchor_window_title ?? 'desktop').trim() || 'desktop';
  const approvalKind = String(latestMission?.approval_kind ?? '').trim();
  const recoveryProfile = String(latestMission?.recovery_profile ?? '').trim();
  const updatedAt = formatMissionTimestamp(String(latestMission?.updated_at ?? latestMission?.created_at ?? ''));

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
            <Badge variant="secondary">{missionKind}</Badge>
            {recoveryProfile ? <Badge variant="outline">{recoveryProfile}</Badge> : null}
            {approvalKind ? <Badge variant="outline">approval:{approvalKind}</Badge> : null}
            {resumeReadyCount > 0 ? <Badge variant="secondary">ready:{resumeReadyCount}</Badge> : null}
            {manualAttentionCount > 0 ? <Badge variant="outline">review:{manualAttentionCount}</Badge> : null}
            {additionalMissionCount > 0 ? <Badge variant="outline">+{additionalMissionCount} more</Badge> : null}
          </div>
          <p className="text-sm text-primary/90">
            {missionApp} :: {summaryText}
          </p>
          <p className="text-[11px] text-muted-foreground">
            mission: {missionId} {' • '}updated: {updatedAt}
            {latestMission?.resume_action ? ` • resume:${String(latestMission.resume_action)}` : ''}
          </p>
        </div>
        <div className="flex shrink-0 flex-wrap items-center gap-2">
          <Button
            type="button"
            variant="outline"
            className="h-8 gap-2 border-primary/30 bg-transparent px-2 text-xs"
            onClick={async () => {
              try {
                const payload = await refreshSnapshot();
                if (payload && Number(payload.count ?? 0) > 0) {
                  toast({
                    title: 'Desktop Recoveries Refreshed',
                    description: `${Number(payload.count ?? 0)} paused desktop mission record(s) are waiting for recovery.`,
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
            Resume Latest
          </Button>
        </div>
      </div>
    </div>
  );
}
