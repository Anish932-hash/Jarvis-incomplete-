'use client';

import { useState, useEffect, useRef, useCallback } from 'react';
import {
  backendClient,
  type BackendMetrics,
  type SystemPerformanceOutput,
} from '@/lib/backend-client';

export interface SystemSpecs {
  cpu: string;
  gpu: string;
  ram: string;
  os: string;
}

export type BoostStatus = 'idle' | 'boosting' | 'cooling';

export type FullSystemMetrics = BackendMetrics;

const initialMetrics: FullSystemMetrics = {
  cpuUsage: 0,
  gpuUsage: 0,
  ramUsage: 0,
  diskUsage: 0,
  networkInMbps: 0,
  networkOutMbps: 0,
  batteryLevel: 100,
  batteryCharging: true,
  specs: {
    cpu: 'Detecting CPU...',
    gpu: 'Detecting GPU...',
    ram: 'Detecting RAM...',
    os: 'Detecting OS...',
  },
  openedApps: [],
  boostStatus: 'idle',
};

const useSystemMetrics = () => {
  const [metrics, setMetrics] = useState<FullSystemMetrics>(initialMetrics);
  const [analysis, setAnalysis] = useState<SystemPerformanceOutput | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);

  const metricsRef = useRef(metrics);
  const isAnalyzingRef = useRef(false);
  const metricsInFlightRef = useRef(false);
  const lastMetricsFetchRef = useRef(0);
  const boostTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    metricsRef.current = metrics;
  }, [metrics]);

  const refreshMetrics = useCallback(async (force = false) => {
    if (metricsRef.current.boostStatus !== 'idle') {
      return;
    }
    if (!force && typeof document !== 'undefined' && document.hidden) {
      return;
    }
    const now = Date.now();
    if (!force && now - lastMetricsFetchRef.current < 1500) {
      return;
    }
    if (metricsInFlightRef.current) {
      return;
    }
    metricsInFlightRef.current = true;
    lastMetricsFetchRef.current = now;
    try {
      const incoming = await backendClient.metrics();
      setMetrics((prev) => ({
        ...incoming,
        boostStatus: prev.boostStatus,
      }));
      if (incoming.analysis) {
        setAnalysis(incoming.analysis);
      }
    } catch (error) {
      console.error('Failed to fetch backend metrics:', error);
    } finally {
      metricsInFlightRef.current = false;
    }
  }, []);

  const boostPerformance = useCallback(() => {
    const current = metricsRef.current;
    if (current.boostStatus !== 'idle') {
      return;
    }

    const originalValues = {
      cpuUsage: current.cpuUsage,
      gpuUsage: current.gpuUsage,
      ramUsage: current.ramUsage,
    };

    setMetrics((prev) => ({
      ...prev,
      boostStatus: 'boosting',
      cpuUsage: Math.min(100, originalValues.cpuUsage + 35),
      gpuUsage: Math.min(100, originalValues.gpuUsage + 45),
      ramUsage: Math.min(100, originalValues.ramUsage + 25),
    }));

    if (boostTimeoutRef.current) {
      clearTimeout(boostTimeoutRef.current);
    }

    boostTimeoutRef.current = setTimeout(() => {
      setMetrics((prev) => ({ ...prev, boostStatus: 'cooling' }));
      boostTimeoutRef.current = setTimeout(() => {
        setMetrics((prev) => ({
          ...prev,
          boostStatus: 'idle',
          cpuUsage: originalValues.cpuUsage,
          gpuUsage: originalValues.gpuUsage,
          ramUsage: originalValues.ramUsage,
        }));
      }, 2600);
    }, 1500);
  }, []);

  useEffect(() => {
    refreshMetrics(true);
    const metricsIntervalId = setInterval(refreshMetrics, 2000);
    const onVisible = () => {
      if (typeof document !== 'undefined' && !document.hidden) {
        refreshMetrics(true);
      }
    };
    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', onVisible);
    }

    return () => {
      clearInterval(metricsIntervalId);
      if (typeof document !== 'undefined') {
        document.removeEventListener('visibilitychange', onVisible);
      }
      if (boostTimeoutRef.current) {
        clearTimeout(boostTimeoutRef.current);
      }
    };
  }, [refreshMetrics]);

  useEffect(() => {
    const performAnalysis = async () => {
      if (isAnalyzingRef.current) {
        return;
      }
      isAnalyzingRef.current = true;
      setIsAnalyzing(true);
      try {
        const current = metricsRef.current;
        const result = await backendClient.analysis({
          cpuUsage: current.cpuUsage,
          gpuUsage: current.gpuUsage,
          ramUsage: current.ramUsage,
          diskUsage: current.diskUsage,
          networkInMbps: current.networkInMbps,
          networkOutMbps: current.networkOutMbps,
          batteryLevel: current.batteryLevel,
          batteryCharging: current.batteryCharging,
        });
        setAnalysis(result);
      } catch (error) {
        console.error('Failed to analyze system performance:', error);
      } finally {
        isAnalyzingRef.current = false;
        setIsAnalyzing(false);
      }
    };

    performAnalysis();
    const analysisIntervalId = setInterval(performAnalysis, 60000);
    return () => clearInterval(analysisIntervalId);
  }, []);

  return { metrics, analysis, isAnalyzing, boostPerformance };
};

export default useSystemMetrics;
