'use client';

import { ResponsiveContainer, RadialBarChart, RadialBar, PolarAngleAxis } from 'recharts';
import React from 'react';
import { cn } from '@/lib/utils';

interface MetricGaugeProps {
  name: string;
  value: number;
  color: string;
  icon: React.ReactNode;
  compact?: boolean;
}

const MetricGauge = ({ name, value, color, icon, compact = false }: MetricGaugeProps) => {
  const data = [{ name, value: value, fill: color }];

  const wrapperClass = compact ? "w-full" : "w-64";
  const containerSize = compact ? "h-[70px] w-[70px]" : "h-32 w-32";
  const titleSize = compact ? "text-[10px]" : "text-sm";
  const valueSize = compact ? "text-base" : "text-2xl";
  const iconContainerSize = compact ? "h-6 w-6" : "h-8 w-8";

  return (
    <div className={cn("flex flex-col items-center", wrapperClass)} style={{ '--glow-color': color } as React.CSSProperties}>
      <div
        className={cn(
          "mb-1 flex items-center justify-center rounded-full border border-primary/40 bg-background/50 text-primary",
          iconContainerSize,
          "[&_svg]:h-4 [&_svg]:w-4",
        )}
      >
        {icon}
      </div>
       <p className={cn("font-medium text-primary/80 -mb-1", titleSize)}>{name}</p>
        <div className={cn("relative", containerSize)}>
          <ResponsiveContainer width="100%" height="100%">
            <RadialBarChart
              innerRadius="70%"
              outerRadius="90%"
              data={data}
              startAngle={90}
              endAngle={-270}
              barSize={compact ? 6 : 12}
            >
              <PolarAngleAxis type="number" domain={[0, 100]} angleAxisId={0} tick={false} />
              <RadialBar
                background={{ fill: 'hsl(var(--primary) / 0.1)' }}
                dataKey="value"
                angleAxisId={0}
                cornerRadius={compact ? 3: 6}
              />
            </RadialBarChart>
          </ResponsiveContainer>
          <div className="absolute inset-0 flex flex-col items-center justify-center">
            <span className={cn("font-bold text-foreground", valueSize)}>
              {value.toFixed(0)}
              <span className={cn("font-normal text-primary/80", compact ? 'text-[10px]' : 'text-base')}>%</span>
            </span>
          </div>
        </div>
    </div>
  );
};

export default MetricGauge;
