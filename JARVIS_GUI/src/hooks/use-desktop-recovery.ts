'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';

import {
  backendClient,
  type DesktopRecoveryDaemonStatusResponse,
  type DesktopRecoveryDaemonTriggerResponse,
  type DesktopInteractResponse,
  type DesktopMissionRecord,
  type DesktopMissionSnapshotResponse,
  type DesktopRecoveryWatchdogHistoryResponse,
  type DesktopRecoveryWatchdogRunRecord,
} from '@/lib/backend-client';

interface UseDesktopRecoveryOptions {
  enabled?: boolean;
  status?: string;
  limit?: number;
  pollMs?: number;
}

function normalizeMissionTimestamp(mission: DesktopMissionRecord | null): string {
  return String(mission?.updated_at ?? mission?.created_at ?? '').trim();
}

function compareMissionFreshness(left: DesktopMissionRecord, right: DesktopMissionRecord): number {
  return normalizeMissionTimestamp(right).localeCompare(normalizeMissionTimestamp(left));
}

function normalizeWatchdogRunRecord(value: unknown): DesktopRecoveryWatchdogRunRecord | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as DesktopRecoveryWatchdogRunRecord;
}

export function useDesktopRecovery({
  enabled = true,
  status = 'paused',
  limit = 6,
  pollMs = 30000,
}: UseDesktopRecoveryOptions = {}) {
  const [snapshot, setSnapshot] = useState<DesktopMissionSnapshotResponse | null>(null);
  const [supervisorStatus, setSupervisorStatus] = useState<DesktopRecoveryDaemonStatusResponse | null>(null);
  const [watchdogHistory, setWatchdogHistory] = useState<DesktopRecoveryWatchdogHistoryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [configuringDaemon, setConfiguringDaemon] = useState(false);
  const [triggeringDaemon, setTriggeringDaemon] = useState(false);

  const refreshSnapshot = useCallback(async () => {
    if (!enabled) {
      setSnapshot(null);
      setSupervisorStatus(null);
      setWatchdogHistory(null);
      return null;
    }
    setLoading(true);
    try {
      const [payload, daemonPayload] = await Promise.all([
        backendClient.desktopMissions({
          status: status.trim() || undefined,
          limit,
        }),
        backendClient.desktopRecoveryDaemon({ history_limit: limit }),
      ]);
      setSnapshot(payload);
      setSupervisorStatus(daemonPayload);
      const daemonHistory =
        daemonPayload.watchdog_history &&
        typeof daemonPayload.watchdog_history === 'object' &&
        !Array.isArray(daemonPayload.watchdog_history)
          ? (daemonPayload.watchdog_history as DesktopRecoveryWatchdogHistoryResponse)
          : null;
      setWatchdogHistory(daemonHistory);
      return { snapshot: payload, supervisorStatus: daemonPayload, watchdogHistory: daemonHistory };
    } finally {
      setLoading(false);
    }
  }, [enabled, limit, status]);

  const missionRows = useMemo(() => {
    const items = Array.isArray(snapshot?.items) ? snapshot.items : [];
    return [...items].sort((left, right) => {
      const priorityGap = Number(right.recovery_priority ?? 0) - Number(left.recovery_priority ?? 0);
      if (priorityGap !== 0) return priorityGap;
      return compareMissionFreshness(left, right);
    });
  }, [snapshot]);

  const latestMission = useMemo(() => {
    const latestPaused =
      snapshot?.latest_paused && typeof snapshot.latest_paused === 'object' && !Array.isArray(snapshot.latest_paused)
        ? (snapshot.latest_paused as DesktopMissionRecord)
        : null;
    return latestPaused ?? missionRows[0] ?? null;
  }, [missionRows, snapshot]);
  const latestWatchdogRun = useMemo(
    () =>
      normalizeWatchdogRunRecord(watchdogHistory?.latest_run ?? null) ??
      normalizeWatchdogRunRecord(watchdogHistory?.latest_triggered_run ?? null) ??
      normalizeWatchdogRunRecord(watchdogHistory?.latest_blocked_run ?? null),
    [watchdogHistory]
  );

  const additionalMissionCount = Math.max(0, missionRows.length - 1);
  const resumeReadyCount = Number(
    snapshot?.resume_ready_count ??
      missionRows.filter((item) => Boolean(item.resume_ready)).length ??
      0
  );
  const explorationMissionCount = Number(
    snapshot?.mission_kind_counts?.exploration ??
      missionRows.filter((item) => String(item.mission_kind ?? '').trim().toLowerCase() === 'exploration').length ??
      0
  );
  const explorationResumeReadyCount = missionRows.filter(
    (item) =>
      String(item.mission_kind ?? '').trim().toLowerCase() === 'exploration' &&
      Boolean(item.resume_ready)
  ).length;
  const manualAttentionCount = Number(
    snapshot?.manual_attention_count ??
      missionRows.filter((item) => Boolean(item.manual_attention_required)).length ??
      0
  );
  const adminApprovalCount = Number(
    snapshot?.admin_approval_count ??
      missionRows.filter((item) => Boolean(item.admin_clearance_required)).length ??
      0
  );
  const destructiveApprovalCount = Number(
    snapshot?.destructive_approval_count ??
      missionRows.filter((item) => Boolean(item.destructive_confirmation)).length ??
      0
  );
  const criticalRiskCount = Number(
    snapshot?.critical_risk_count ??
      missionRows.filter((item) => Boolean(item.critical_risk)).length ??
      0
  );
  const backendSupervisorEnabled = Boolean(supervisorStatus?.enabled);

  const resumeLatestMission = useCallback(async (): Promise<DesktopInteractResponse | null> => {
    const mission = latestMission;
    if (!mission) return null;
    setResuming(true);
    try {
      const response = await backendClient.desktopResumeMission({
        mission_id: String(mission.mission_id ?? '').trim() || undefined,
        mission_kind: String(mission.mission_kind ?? '').trim() || undefined,
        app_name: String(mission.app_name ?? '').trim() || undefined,
        window_title:
          String(mission.blocking_window_title ?? mission.anchor_window_title ?? '').trim() || undefined,
      });
      await refreshSnapshot();
      return response;
    } finally {
      setResuming(false);
    }
  }, [latestMission, refreshSnapshot]);

  const configureRecoveryDaemon = useCallback(
    async (input?: {
      enabled?: boolean;
      intervalS?: number;
      limit?: number;
      maxAutoResumes?: number;
      policyProfile?: string;
      allowHighRisk?: boolean;
      allowCriticalRisk?: boolean;
      allowAdminClearance?: boolean;
      allowDestructive?: boolean;
      missionStatus?: string;
      missionKind?: string;
      appName?: string;
      stopReasonCode?: string;
      resumeForce?: boolean;
    }): Promise<DesktopRecoveryDaemonStatusResponse | null> => {
      setConfiguringDaemon(true);
      try {
        const response = await backendClient.updateDesktopRecoveryDaemon({
          enabled: input?.enabled,
          interval_s: input?.intervalS,
          limit: input?.limit,
          max_auto_resumes: input?.maxAutoResumes,
          policy_profile: input?.policyProfile,
          allow_high_risk: input?.allowHighRisk,
          allow_critical_risk: input?.allowCriticalRisk,
          allow_admin_clearance: input?.allowAdminClearance,
          allow_destructive: input?.allowDestructive,
          mission_status: input?.missionStatus,
          mission_kind: input?.missionKind,
          app_name: input?.appName,
          stop_reason_code: input?.stopReasonCode,
          resume_force: input?.resumeForce,
          history_limit: limit,
        });
        setSupervisorStatus(response);
        const snapshotPayload =
          response.snapshot && typeof response.snapshot === 'object' && !Array.isArray(response.snapshot)
            ? (response.snapshot as DesktopMissionSnapshotResponse)
            : null;
        const daemonHistory =
          response.watchdog_history &&
          typeof response.watchdog_history === 'object' &&
          !Array.isArray(response.watchdog_history)
            ? (response.watchdog_history as DesktopRecoveryWatchdogHistoryResponse)
            : null;
        if (snapshotPayload) {
          setSnapshot(snapshotPayload);
        }
        setWatchdogHistory(daemonHistory);
        return response;
      } finally {
        setConfiguringDaemon(false);
      }
    },
    []
  );

  const triggerRecoveryDaemon = useCallback(
    async (input?: {
      limit?: number;
      maxAutoResumes?: number;
      policyProfile?: string;
      allowHighRisk?: boolean;
      allowCriticalRisk?: boolean;
      allowAdminClearance?: boolean;
      allowDestructive?: boolean;
      missionStatus?: string;
      missionKind?: string;
      appName?: string;
      stopReasonCode?: string;
      resumeForce?: boolean;
    }): Promise<DesktopRecoveryDaemonTriggerResponse | null> => {
      setTriggeringDaemon(true);
      try {
        const response = await backendClient.triggerDesktopRecoveryDaemon({
          limit: input?.limit,
          max_auto_resumes: input?.maxAutoResumes,
          policy_profile: input?.policyProfile,
          allow_high_risk: input?.allowHighRisk,
          allow_critical_risk: input?.allowCriticalRisk,
          allow_admin_clearance: input?.allowAdminClearance,
          allow_destructive: input?.allowDestructive,
          mission_status: input?.missionStatus,
          mission_kind: input?.missionKind,
          app_name: input?.appName,
          stop_reason_code: input?.stopReasonCode,
          resume_force: input?.resumeForce,
          history_limit: limit,
        });
        const responseSupervisor =
          response.supervisor && typeof response.supervisor === 'object' && !Array.isArray(response.supervisor)
            ? (response.supervisor as DesktopRecoveryDaemonStatusResponse)
            : null;
        if (responseSupervisor) {
          setSupervisorStatus(responseSupervisor);
          const snapshotPayload =
            responseSupervisor.snapshot &&
            typeof responseSupervisor.snapshot === 'object' &&
            !Array.isArray(responseSupervisor.snapshot)
              ? (responseSupervisor.snapshot as DesktopMissionSnapshotResponse)
              : null;
          const daemonHistory =
            responseSupervisor.watchdog_history &&
            typeof responseSupervisor.watchdog_history === 'object' &&
            !Array.isArray(responseSupervisor.watchdog_history)
              ? (responseSupervisor.watchdog_history as DesktopRecoveryWatchdogHistoryResponse)
              : null;
          if (snapshotPayload) {
            setSnapshot(snapshotPayload);
          }
          setWatchdogHistory(daemonHistory);
        } else {
          await refreshSnapshot();
        }
        return response;
      } finally {
        setTriggeringDaemon(false);
      }
    },
    [limit, refreshSnapshot]
  );

  const resetWatchdogHistory = useCallback(
    async (input?: {
      runId?: string;
      status?: string;
      source?: string;
      appName?: string;
      missionKind?: string;
    }) => {
      const response = await backendClient.resetDesktopRecoveryWatchdogHistory({
        run_id: String(input?.runId ?? '').trim() || undefined,
        status: String(input?.status ?? '').trim() || undefined,
        source: String(input?.source ?? '').trim() || undefined,
        app_name: String(input?.appName ?? '').trim() || undefined,
        mission_kind: String(input?.missionKind ?? '').trim() || undefined,
      });
      await refreshSnapshot();
      return response;
    },
    [refreshSnapshot]
  );

  useEffect(() => {
    if (!enabled) {
      setSnapshot(null);
      setSupervisorStatus(null);
      return;
    }
    void refreshSnapshot();
    if (pollMs <= 0) return;
    const timer = window.setInterval(() => {
      void refreshSnapshot();
    }, pollMs);
    return () => window.clearInterval(timer);
  }, [enabled, pollMs, refreshSnapshot]);

  return {
    snapshot,
    missionRows,
    latestMission,
    watchdogHistory,
    latestWatchdogRun,
    supervisorStatus,
    backendSupervisorEnabled,
    additionalMissionCount,
    resumeReadyCount,
    explorationMissionCount,
    explorationResumeReadyCount,
    manualAttentionCount,
    adminApprovalCount,
    destructiveApprovalCount,
    criticalRiskCount,
    loading,
    resuming,
    configuringDaemon,
    triggeringDaemon,
    refreshSnapshot,
    resumeLatestMission,
    configureRecoveryDaemon,
    triggerRecoveryDaemon,
    resetWatchdogHistory,
  };
}

export default useDesktopRecovery;
