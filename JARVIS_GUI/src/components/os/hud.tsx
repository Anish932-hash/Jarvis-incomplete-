'use client';

import useSystemMetrics from '@/hooks/use-system-metrics';
import CentralCore from './central-core';
import SystemMetricsDisplay from './system-metrics-panel';
import SystemAnalysisPanel from './system-analysis-panel';
import InfoListPanel from './InfoListPanel';
import Clock from './Clock';
import BoostButton from './BoostButton';
import ActionControlPanel from './action-control-panel';
import DesktopRecoveryBanner from './desktop-recovery-banner';
import ModelSetupRecoveryBanner from './model-setup-recovery-banner';
import { MessageSquare, SlidersHorizontal, Wrench } from 'lucide-react';
import { type AppMode } from '@/app/page';
import { cn } from '@/lib/utils';
import { useToast } from '@/hooks/use-toast';

const HUD = ({ setMode }: { setMode: (mode: AppMode) => void }) => {
  const { metrics, analysis, isAnalyzing, boostPerformance } = useSystemMetrics();
  const { toast } = useToast();

  const boostOverlayClass = cn(
    "fixed inset-0 z-50 pointer-events-none transition-opacity duration-1000",
    {
      'opacity-100 animate-boost-flash': metrics.boostStatus === 'boosting',
      'opacity-0': metrics.boostStatus !== 'boosting',
    }
  );

  return (
    <div className="relative h-screen w-screen p-4 animate-fade-in font-headline text-primary/90">
        <div className={boostOverlayClass} style={{
            backgroundImage: 'radial-gradient(circle, hsl(var(--primary) / 0.3) 0%, transparent 60%)',
        }}/>
        <div className={cn(
            "fixed inset-0 z-[60] pointer-events-none bg-[radial-gradient(ellipse_at_center,_transparent_60%,_black_100%)] opacity-0",
            {
              'animate-vignette-pulse': metrics.boostStatus === 'boosting',
            }
        )} />

        <div className="h-full w-full grid grid-cols-3 grid-rows-3 gap-4">
            {/* Top Row */}
            <div className="col-span-1 row-span-1 flex items-start justify-start">
                <Clock />
            </div>
            <div className="col-span-1 row-span-1 flex items-start justify-center pt-2">
                <div className="flex w-full max-w-md flex-col gap-2">
                    <DesktopRecoveryBanner className="w-full" compact />
                    <ModelSetupRecoveryBanner className="w-full" compact />
                </div>
            </div>
            <div className="col-span-1 row-span-1 flex items-start justify-end">
                <div className="flex items-center gap-4 text-primary tracking-widest text-sm">
                    <button 
                        className="flex items-center gap-2 rounded-md p-1 transition-all hover:bg-primary/10 hover:text-accent hover:shadow-[0_0_15px_hsl(var(--primary)/0.3)]"
                        onClick={() => {
                            toast({
                                title: "System Configuration",
                                description: "Access to system settings is restricted at this time.",
                            })
                        }}
                    >
                        <SlidersHorizontal size={16} />
                        <span>NEON OS // J.A.R.V.I.S. v1.0</span>
                    </button>
                    <button onClick={() => setMode('chat')} className="rounded-md p-1 transition-all hover:bg-primary/10 hover:text-accent hover:shadow-[0_0_15px_hsl(var(--primary)/0.3)]" title="Switch to Chat Mode">
                        <MessageSquare size={16} />
                    </button>
                    <ActionControlPanel
                        trigger={
                            <button className="rounded-md p-1 transition-all hover:bg-primary/10 hover:text-accent hover:shadow-[0_0_15px_hsl(var(--primary)/0.3)]" title="Open Action Panel">
                                <Wrench size={16} />
                            </button>
                        }
                    />
                </div>
            </div>

            {/* Middle Row */}
            <div className="col-span-1 row-span-1 flex items-center justify-start">
                <SystemMetricsDisplay metrics={metrics} />
            </div>
            <div className="col-span-1 row-span-1 flex items-center justify-center">
                <CentralCore />
            </div>
            <div className="col-span-1 row-span-1 flex items-center justify-end">
                <div className="flex flex-col gap-4">
                    <InfoListPanel title="User PC Specs" items={Object.entries(metrics.specs).map(([key, value]) => `${key.toUpperCase()}: ${value}`)} />
                    <InfoListPanel title="Opened Apps" items={metrics.openedApps} />
                </div>
            </div>

            {/* Bottom Row */}
            <div className="col-span-1 row-span-1 flex items-end justify-start">
                <BoostButton boostStatus={metrics.boostStatus} onClick={boostPerformance} />
            </div>
            <div className="col-span-1 row-span-1" />
            <div className="col-span-1 row-span-1 flex items-end justify-end">
                <SystemAnalysisPanel analysis={analysis} isAnalyzing={isAnalyzing} />
            </div>
        </div>
    </div>
  );
};

export default HUD;
