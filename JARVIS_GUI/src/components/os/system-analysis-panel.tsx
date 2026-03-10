import { type SystemPerformanceOutput } from '@/lib/backend-client';
import { AlertCircle, Loader, ShieldCheck } from 'lucide-react';
import JarvisPanel from './JarvisPanel';
import { ScrollArea } from '@/components/ui/scroll-area';

interface SystemAnalysisPanelProps {
  analysis: SystemPerformanceOutput | null;
  isAnalyzing: boolean;
}

const SystemAnalysisPanel = ({ analysis, isAnalyzing }: SystemAnalysisPanelProps) => {
  const Icon = analysis?.hasCriticalIssues ? AlertCircle : ShieldCheck;
  const iconColor = analysis?.hasCriticalIssues ? 'text-destructive' : 'text-green-400';
  const title = analysis?.hasCriticalIssues ? 'CRITICAL ALERTS' : 'SYSTEM STATUS: NOMINAL';

  return (
    <JarvisPanel title="J.A.R.V.I.S. AI Analysis" className="w-full max-w-xl">
        <ScrollArea className="max-h-32 w-full pr-4">
            {isAnalyzing && !analysis && (
            <div className="flex items-center gap-2 text-accent">
                <Loader className="animate-spin" size={16} />
                <span>ANALYZING METRICS...</span>
            </div>
            )}

            {analysis && (
            <div className={`flex items-start gap-3`}>
                <Icon size={20} className={`${iconColor} mt-0.5 flex-shrink-0 [filter:drop-shadow(0_0_3px_currentColor)]`} />
                <div>
                    <h3 className={`font-bold tracking-wider ${iconColor}`}>{title}</h3>
                    <p className="text-sm text-foreground/80">{analysis.overallSummary}</p>
                    {analysis.hasCriticalIssues && analysis.issues.length > 0 && (
                        <div className="mt-2 space-y-1 border-l-2 border-destructive/50 pl-3">
                            {analysis.issues.map((issue, index) => (
                            <div key={index} className="text-xs">
                                <p><span className="font-bold text-destructive/80">[{issue.category.toUpperCase()}]:</span> {issue.recommendation}</p>
                            </div>
                            ))}
                        </div>
                    )}
                </div>
                </div>
            )}
        </ScrollArea>
    </JarvisPanel>
  );
};

export default SystemAnalysisPanel;
