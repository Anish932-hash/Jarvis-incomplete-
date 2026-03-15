'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  backendClient,
  type ModelSetupRecoveryWatchdogHistoryResponse,
  type ModelSetupRecoveryWatchdogRunRecord,
  type ModelSetupMissionHistoryResponse,
  type ModelSetupMissionLaunchResponse,
  type ModelSetupMissionRecord,
  type ModelSetupMissionRecoverySweepResponse,
  type ModelSetupMissionRecoveryWatchdogResponse,
  type ModelSetupResumeAdviceResponse,
} from '@/lib/backend-client';

interface UseModelSetupRecoveryOptions {
  enabled?: boolean;
  limit?: number;
  pollMs?: number;
  currentScope?: boolean;
}

function normalizeMissionTimestamp(mission: ModelSetupMissionRecord | null): string {
  return String(mission?.updated_at ?? mission?.created_at ?? '').trim();
}

function compareMissionFreshness(left: ModelSetupMissionRecord, right: ModelSetupMissionRecord): number {
  return normalizeMissionTimestamp(right).localeCompare(normalizeMissionTimestamp(left));
}

function normalizeMissionRecord(value: unknown): ModelSetupMissionRecord | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as ModelSetupMissionRecord;
}

function normalizeWatchdogRunRecord(value: unknown): ModelSetupRecoveryWatchdogRunRecord | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as ModelSetupRecoveryWatchdogRunRecord;
}

function normalizeTimestamp(value: string | undefined): number {
  const normalized = String(value ?? '').trim();
  if (!normalized) return 0;
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? 0 : parsed.getTime();
}

function missionScopeInput(
  mission: ModelSetupMissionRecord | null
): { manifest_path?: string; workspace_root?: string } {
  const manifestPath = String(mission?.manifest_path ?? '').trim();
  const workspaceRoot = String(mission?.workspace_root ?? '').trim();
  return {
    manifest_path: manifestPath || undefined,
    workspace_root: workspaceRoot || undefined,
  };
}

function rankMissionPriority(mission: ModelSetupMissionRecord): number {
  return Number(mission.recovery_priority ?? 0);
}

function selectPreferredMission(
  snapshot: ModelSetupMissionHistoryResponse | null,
  missionRows: ModelSetupMissionRecord[]
): ModelSetupMissionRecord | null {
  const latestAuto = normalizeMissionRecord(snapshot?.latest_auto_resume_candidate ?? null);
  if (latestAuto) return latestAuto;
  const latestReady = normalizeMissionRecord(snapshot?.latest_resume_ready ?? null);
  if (latestReady) return latestReady;
  const latestAttention = normalizeMissionRecord(snapshot?.latest_attention_required ?? null);
  if (latestAttention) return latestAttention;
  const latestRunning = normalizeMissionRecord(snapshot?.latest_running ?? null);
  if (latestRunning) return latestRunning;
  return missionRows[0] ?? null;
}

export function useModelSetupRecovery({
  enabled = true,
  limit = 8,
  pollMs = 30000,
  currentScope = true,
}: UseModelSetupRecoveryOptions = {}) {
  const [snapshot, setSnapshot] = useState<ModelSetupMissionHistoryResponse | null>(null);
  const [resumeAdvice, setResumeAdvice] = useState<ModelSetupResumeAdviceResponse | null>(null);
  const [watchdogHistory, setWatchdogHistory] = useState<ModelSetupRecoveryWatchdogHistoryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [sweeping, setSweeping] = useState(false);
  const [watchdogging, setWatchdogging] = useState(false);
  const autoWatchdogLockRef = useRef(false);
  const lastAutoWatchdogAtRef = useRef(0);

  const missionRows = useMemo(() => {
    const items = Array.isArray(snapshot?.items) ? snapshot.items : [];
    return [...items].sort((left, right) => {
      const priorityGap = rankMissionPriority(right) - rankMissionPriority(left);
      if (priorityGap !== 0) return priorityGap;
      return compareMissionFreshness(left, right);
    });
  }, [snapshot]);

  const latestMission = useMemo(
    () => selectPreferredMission(snapshot, missionRows),
    [missionRows, snapshot]
  );
  const latestWatchdogRun = useMemo(
    () =>
      normalizeWatchdogRunRecord(watchdogHistory?.latest_run ?? null) ??
      normalizeWatchdogRunRecord(watchdogHistory?.latest_triggered_run ?? null),
    [watchdogHistory]
  );
  const latestWatchdogRunTs = useMemo(
    () => normalizeTimestamp(String(latestWatchdogRun?.updated_at ?? latestWatchdogRun?.created_at ?? '')),
    [latestWatchdogRun]
  );
  const autoResumeCandidateCount = useMemo(
    () =>
      Number(
        snapshot?.auto_resume_candidate_count ??
          missionRows.filter((item) => Boolean(item.auto_resume_candidate)).length ??
          0
      ),
    [missionRows, snapshot]
  );
  const resumeReadyCount = useMemo(
    () =>
      Number(
        snapshot?.resume_ready_count ??
          missionRows.filter((item) => Boolean(item.resume_ready)).length ??
          0
      ),
    [missionRows, snapshot]
  );
  const manualAttentionCount = useMemo(
    () =>
      Number(
        snapshot?.manual_attention_count ??
          missionRows.filter((item) => Boolean(item.manual_attention_required)).length ??
          0
      ),
    [missionRows, snapshot]
  );
  const runningCount = useMemo(
    () =>
      Number(
        snapshot?.running_count ??
          missionRows.filter((item) => String(item.status ?? '').trim().toLowerCase() === 'running').length ??
          0
      ),
    [missionRows, snapshot]
  );
  const stalledCount = useMemo(
    () =>
      Number(
        resumeAdvice?.stalled_run_count ??
          latestMission?.stalled_run_count ??
          missionRows.reduce((count, item) => count + Number(item.stalled_run_count ?? 0), 0) ??
          0
      ),
    [latestMission, missionRows, resumeAdvice]
  );
  const watchActiveRuns = useMemo(
    () => Boolean(resumeAdvice?.watch_active_runs ?? latestMission?.watch_active_runs ?? false),
    [latestMission, resumeAdvice]
  );
  const dynamicPollMs = useMemo(() => {
    const suggestedPollS = Number(
      resumeAdvice?.next_poll_s ?? latestMission?.next_poll_s ?? 0
    );
    const stalledCount = Number(
      resumeAdvice?.stalled_run_count ?? latestMission?.stalled_run_count ?? 0
    );
    const activeRunCount = Number(
      resumeAdvice?.active_run_count ?? latestMission?.active_run_count ?? 0
    );
    if (suggestedPollS > 0) {
      return Math.max(5000, Math.min(pollMs, suggestedPollS * 1000));
    }
    if (stalledCount > 0) {
      return Math.max(5000, Math.min(pollMs, 8000));
    }
    if (activeRunCount > 0) {
      return Math.max(8000, Math.min(pollMs, 12000));
    }
    return pollMs;
  }, [latestMission, pollMs, resumeAdvice]);

  const refreshSnapshot = useCallback(async () => {
    if (!enabled) {
      setSnapshot(null);
      setResumeAdvice(null);
      setWatchdogHistory(null);
      return null;
    }
    setLoading(true);
    try {
      const payload = await backendClient.modelSetupMissionHistory({
        limit,
        current_scope: currentScope,
      });
      const items = Array.isArray(payload.items) ? [...payload.items] : [];
      items.sort((left, right) => {
        const priorityGap = rankMissionPriority(right) - rankMissionPriority(left);
        if (priorityGap !== 0) return priorityGap;
        return compareMissionFreshness(left, right);
      });
      const preferred =
        normalizeMissionRecord(payload.latest_auto_resume_candidate ?? null) ??
        normalizeMissionRecord(payload.latest_resume_ready ?? null) ??
        normalizeMissionRecord(payload.latest_attention_required ?? null) ??
        normalizeMissionRecord(payload.latest_running ?? null) ??
        items[0] ??
        null;
      setSnapshot(payload);
      const watchdogPayload = await backendClient.modelSetupMissionRecoveryWatchdogHistory({
        limit,
        current_scope: currentScope,
      });
      setWatchdogHistory(watchdogPayload);

      if (preferred) {
        const advice = await backendClient.modelSetupMissionResumeAdvice({
          mission_id: String(preferred.mission_id ?? '').trim() || undefined,
          limit,
          current_scope: false,
          ...missionScopeInput(preferred),
        });
        setResumeAdvice(advice);
        return { snapshot: payload, resumeAdvice: advice, watchdogHistory: watchdogPayload };
      }

      const advice = await backendClient.modelSetupMissionResumeAdvice({
        limit,
        current_scope: currentScope,
      });
      setResumeAdvice(advice);
      return { snapshot: payload, resumeAdvice: advice, watchdogHistory: watchdogPayload };
    } finally {
      setLoading(false);
    }
  }, [currentScope, enabled, limit]);

  const autoResumeLatestMission = useCallback(async (): Promise<ModelSetupMissionLaunchResponse | null> => {
    const mission = latestMission;
    if (!mission) return null;
    setResuming(true);
    try {
      const response = await backendClient.autoResumeModelSetupMission({
        mission_id: String(mission.mission_id ?? '').trim() || undefined,
        continue_on_error: true,
        limit,
        current_scope: false,
        ...missionScopeInput(mission),
      });
      await refreshSnapshot();
      return response;
    } finally {
      setResuming(false);
    }
  }, [latestMission, limit, refreshSnapshot]);

  const resumeLatestMission = useCallback(async (): Promise<ModelSetupMissionLaunchResponse | null> => {
    const mission = latestMission;
    if (!mission) return null;
    setResuming(true);
    try {
      const response = await backendClient.modelSetupMissionResume({
        mission_id: String(mission.mission_id ?? '').trim() || undefined,
        continue_on_error: true,
        limit,
        ...missionScopeInput(mission),
      });
      await refreshSnapshot();
      return response;
    } finally {
      setResuming(false);
    }
  }, [latestMission, limit, refreshSnapshot]);

  const sweepRecovery = useCallback(
    async (input?: {
      missionId?: string;
      currentScope?: boolean;
      maxAutoResumePasses?: number;
      continueFollowupActions?: boolean;
      maxFollowupWaves?: number;
      dryRun?: boolean;
    }): Promise<ModelSetupMissionRecoverySweepResponse | null> => {
      setSweeping(true);
      try {
        const response = await backendClient.modelSetupMissionRecoverySweep({
          mission_id: String(input?.missionId ?? latestMission?.mission_id ?? '').trim() || undefined,
          current_scope: input?.currentScope ?? !String(input?.missionId ?? latestMission?.mission_id ?? '').trim(),
          max_auto_resume_passes: input?.maxAutoResumePasses ?? 3,
          continue_followup_actions: input?.continueFollowupActions ?? true,
          max_followup_waves: input?.maxFollowupWaves ?? 3,
          dry_run: input?.dryRun ?? false,
          continue_on_error: true,
          limit,
          ...missionScopeInput(latestMission),
        });
        await refreshSnapshot();
        return response;
      } finally {
        setSweeping(false);
      }
    },
    [latestMission, limit, refreshSnapshot]
  );

  const watchdogRecovery = useCallback(
    async (input?: {
      missionId?: string;
      currentScope?: boolean;
      maxMissions?: number;
      maxAutoResumes?: number;
      continueFollowupActions?: boolean;
      maxFollowupWaves?: number;
      dryRun?: boolean;
    }): Promise<ModelSetupMissionRecoveryWatchdogResponse | null> => {
      setWatchdogging(true);
      try {
        const missionId =
          String(input?.missionId ?? latestMission?.mission_id ?? '').trim() || undefined;
        const currentScopeFlag =
          input?.currentScope ?? !String(input?.missionId ?? latestMission?.mission_id ?? '').trim();
        const response = await backendClient.modelSetupMissionRecoveryWatchdog({
          mission_id: missionId,
          current_scope: currentScopeFlag,
          max_missions: input?.maxMissions ?? 6,
          max_auto_resumes: input?.maxAutoResumes ?? 2,
          continue_followup_actions: input?.continueFollowupActions ?? true,
          max_followup_waves: input?.maxFollowupWaves ?? 3,
          dry_run: input?.dryRun ?? false,
          continue_on_error: true,
          limit,
          ...missionScopeInput(latestMission),
        });
        lastAutoWatchdogAtRef.current = Date.now();
        await refreshSnapshot();
        return response;
      } finally {
        setWatchdogging(false);
      }
    },
    [latestMission, limit, refreshSnapshot]
  );

  const resetWatchdogHistory = useCallback(
    async (input?: {
      runId?: string;
      status?: string;
      scope?: { manifest_path?: string; workspace_root?: string } | null;
    }): Promise<{ status: string; removed?: number; filters?: Record<string, unknown> } | null> => {
      const missionScope = missionScopeInput(latestMission);
      const scope = {
        manifest_path: String(input?.scope?.manifest_path ?? missionScope.manifest_path ?? '').trim() || undefined,
        workspace_root: String(input?.scope?.workspace_root ?? missionScope.workspace_root ?? '').trim() || undefined,
      };
      const response = await backendClient.resetModelSetupMissionRecoveryWatchdog({
        run_id: String(input?.runId ?? '').trim() || undefined,
        status: String(input?.status ?? '').trim() || undefined,
        ...scope,
      });
      await refreshSnapshot();
      return response;
    },
    [latestMission, refreshSnapshot]
  );

  const autoWatchdogEligible = useMemo(
    () =>
      enabled &&
      !loading &&
      !resuming &&
      !sweeping &&
      !watchdogging &&
      Boolean(latestMission) &&
      (autoResumeCandidateCount > 0 || watchActiveRuns || stalledCount > 0),
    [enabled, latestMission, loading, resuming, snapshot, stalledCount, sweeping, watchdogging, watchActiveRuns]
  );

  useEffect(() => {
    if (!enabled) {
      setSnapshot(null);
      setResumeAdvice(null);
      setWatchdogHistory(null);
      return;
    }
    void refreshSnapshot();
    if (dynamicPollMs <= 0) return;
    const timer = window.setInterval(() => {
      void refreshSnapshot();
    }, dynamicPollMs);
    return () => window.clearInterval(timer);
  }, [dynamicPollMs, enabled, refreshSnapshot]);

  useEffect(() => {
    if (!autoWatchdogEligible || autoWatchdogLockRef.current) return;
    const cooldownMs = Math.max(8000, dynamicPollMs);
    const lastObservedAt = Math.max(latestWatchdogRunTs, lastAutoWatchdogAtRef.current);
    const now = Date.now();
    const waitMs = lastObservedAt > 0 ? Math.max(1500, cooldownMs - (now - lastObservedAt)) : cooldownMs;
    const timer = window.setTimeout(() => {
      if (autoWatchdogLockRef.current) return;
      autoWatchdogLockRef.current = true;
      void watchdogRecovery({
        missionId:
          autoResumeCandidateCount > 0
            ? undefined
            : String(latestMission?.mission_id ?? '').trim() || undefined,
        currentScope:
          autoResumeCandidateCount > 0
            ? currentScope
            : !String(latestMission?.mission_id ?? '').trim(),
        maxMissions: autoResumeCandidateCount > 0 ? 4 : 2,
        maxAutoResumes: 1,
        continueFollowupActions: true,
        maxFollowupWaves: 2,
      }).finally(() => {
        lastAutoWatchdogAtRef.current = Date.now();
        autoWatchdogLockRef.current = false;
      });
    }, waitMs);
    return () => window.clearTimeout(timer);
  }, [
    autoWatchdogEligible,
    currentScope,
    dynamicPollMs,
    latestMission,
    latestWatchdogRunTs,
    autoResumeCandidateCount,
    watchdogRecovery,
  ]);

  return {
    snapshot,
    resumeAdvice,
    watchdogHistory,
    latestWatchdogRun,
    missionRows,
    latestMission,
    dynamicPollMs,
    autoResumeCandidateCount,
    resumeReadyCount,
    manualAttentionCount,
    runningCount,
    stalledCount,
    watchActiveRuns,
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
  };
}

export default useModelSetupRecovery;
