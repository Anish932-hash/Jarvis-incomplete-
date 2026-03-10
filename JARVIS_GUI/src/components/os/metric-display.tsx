import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import React from 'react';

interface MetricDisplayProps {
  name: string;
  value: string;
  unit?: string;
  status?: string;
  icon: React.ReactNode;
}

const MetricDisplay = ({ name, value, unit, status, icon }: MetricDisplayProps) => {
  return (
    <Card className="w-64 border-primary/20 bg-card/70 backdrop-blur-sm shadow-[0_0_20px_hsl(var(--primary)/0.15),inset_0_0_5px_hsl(var(--primary)/0.1)]">
      <CardHeader className="flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium text-primary/80">{name}</CardTitle>
        <div className="text-primary [filter:drop-shadow(0_0_2px_hsl(var(--primary)))]">{icon}</div>
      </CardHeader>
      <CardContent>
        <div className="flex items-baseline gap-2">
          <span className="text-2xl font-bold text-foreground">{value}</span>
          {unit && <span className="text-sm text-primary/80">{unit}</span>}
        </div>
        <p className="text-xs text-accent">{status}</p>
      </CardContent>
    </Card>
  );
};

export default MetricDisplay;
