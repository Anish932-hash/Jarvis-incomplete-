'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';

import {
  backendClient,
  type DesktopInteractResponse,
  type DesktopMissionRecord,
  type DesktopMissionSnapshotResponse,
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

export function useDesktopRecovery({
  enabled = true,
  status = 'paused',
  limit = 6,
  pollMs = 30000,
}: UseDesktopRecoveryOptions = {}) {
  const [snapshot, setSnapshot] = useState<DesktopMissionSnapshotResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [resuming, setResuming] = useState(false);

  const refreshSnapshot = useCallback(async () => {
    if (!enabled) {
      setSnapshot(null);
      return null;
    }
    setLoading(true);
    try {
      const payload = await backendClient.desktopMissions({
        status: status.trim() || undefined,
        limit,
      });
      setSnapshot(payload);
      return payload;
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

  const additionalMissionCount = Math.max(0, missionRows.length - 1);
  const resumeReadyCount = Number(
    snapshot?.resume_ready_count ??
      missionRows.filter((item) => Boolean(item.resume_ready)).length ??
      0
  );
  const manualAttentionCount = Number(
    snapshot?.manual_attention_count ??
      missionRows.filter((item) => Boolean(item.manual_attention_required)).length ??
      0
  );

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

  useEffect(() => {
    if (!enabled) {
      setSnapshot(null);
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
    additionalMissionCount,
    resumeReadyCount,
    manualAttentionCount,
    loading,
    resuming,
    refreshSnapshot,
    resumeLatestMission,
  };
}

export default useDesktopRecovery;
