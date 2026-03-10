'use client';

import { Battery, Cpu, Disc, MemoryStick, Network, Zap } from 'lucide-react';
import type { FullSystemMetrics } from '@/hooks/use-system-metrics';
import JarvisPanel from './JarvisPanel';
import MetricGauge from './metric-gauge';

const PowerIndicator = ({ level, charging }: { level: number; charging: boolean }) => (
    <div className="space-y-1 text-xs">
        <div className="flex justify-between items-center">
            <div className="flex items-center gap-1.5 text-primary/70">
                <Battery size={14} />
                <span>Power Status</span>
            </div>
            <div className="flex items-center gap-1 font-bold text-foreground">
                 {charging && <Zap size={12} className="text-green-400 fill-green-400" />}
                <span>{level.toFixed(0)}%</span>
            </div>
        </div>
        <div className="w-full bg-primary/10 h-1.5 rounded-full overflow-hidden">
            <div 
                className="h-full rounded-full transition-all duration-300" 
                style={{
                    width: `${level}%`,
                    backgroundColor: level < 20 ? 'hsl(var(--destructive))' : 'hsl(var(--primary))'
                }}
            />
        </div>
        {charging && <p className="text-green-400 text-right font-bold text-[10px] tracking-wider -mt-0.5">CHARGING</p>}
    </div>
);

const formatBytes = (bytes: number) => {
  if (!Number.isFinite(bytes) || bytes <= 0) return 'N/A';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  const decimals = value >= 100 || idx === 0 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(decimals)} ${units[idx]}`;
};

const StorageIndicator = ({
  diskUsage,
  totalBytes,
  usedBytes,
  freeBytes,
}: {
  diskUsage: number;
  totalBytes: number;
  usedBytes: number;
  freeBytes: number;
}) => (
  <div className="space-y-1 text-xs">
    <div className="flex justify-between items-center">
      <span>Storage Used:</span>
      <span className="font-bold">{formatBytes(usedBytes)} / {formatBytes(totalBytes)}</span>
    </div>
    <div className="w-full bg-primary/10 h-1.5 rounded-full">
        <div className="bg-primary h-full rounded-full" style={{width: `${diskUsage}%`}}/>
    </div>
    <div className="flex justify-between text-[10px] text-primary/70">
      <span>Free: {formatBytes(freeBytes)}</span>
      <span>{diskUsage.toFixed(1)}%</span>
    </div>
  </div>
);


const SystemMetricsDisplay = ({ metrics }: { metrics: FullSystemMetrics }) => {
  const totalBytes = metrics.diskTotalBytes ?? 0;
  const usedBytes =
    metrics.diskUsedBytes ?? (totalBytes > 0 ? (totalBytes * metrics.diskUsage) / 100 : 0);
  const freeBytes =
    metrics.diskFreeBytes ?? (totalBytes > 0 ? Math.max(0, totalBytes - usedBytes) : 0);

  return (
    <JarvisPanel title="System Diagnostics" className="w-72">
        <div className="space-y-4">
            {/* Core Metrics */}
            <div>
                <h4 className="text-xs text-primary/70 tracking-widest mb-1">CORE METRICS</h4>
                <div className="grid grid-cols-3 gap-1">
                    <MetricGauge name="CPU" value={metrics.cpuUsage} color="hsl(var(--chart-1))" icon={<Cpu size={24} />} compact />
                    <MetricGauge name="GPU" value={metrics.gpuUsage} color="hsl(var(--chart-2))" icon={<Disc size={24} />} compact />
                    <MetricGauge name="RAM" value={metrics.ramUsage} color="hsl(var(--chart-3))" icon={<MemoryStick size={24} />} compact />
                </div>
            </div>

            <div className="h-px w-full bg-primary/10" />

            {/* Sub-Systems */}
             <div className="space-y-3">
                <div className="flex items-center gap-3 text-xs">
                    <Network size={16} className="text-primary flex-shrink-0"/>
                    <div>
                        <p className="text-primary/70">Network Status</p>
                        <p className="font-bold">{metrics.networkInMbps.toFixed(1)} Mbps ↓ / {metrics.networkOutMbps.toFixed(1)} Mbps ↑</p>
                    </div>
                </div>
                <StorageIndicator
                  diskUsage={metrics.diskUsage}
                  totalBytes={totalBytes}
                  usedBytes={usedBytes}
                  freeBytes={freeBytes}
                />
            </div>

            <div className="h-px w-full bg-primary/10" />
            
            {/* Power */}
            <PowerIndicator level={metrics.batteryLevel} charging={metrics.batteryCharging} />
        </div>
    </JarvisPanel>
  );
};

export default SystemMetricsDisplay;
