import { cn } from '@/lib/utils';
import React from 'react';

interface JarvisPanelProps extends React.HTMLAttributes<HTMLDivElement> {
  title?: string;
}

const Corner = ({ className }: { className?: string }) => (
  <div className={cn('absolute h-2.5 w-2.5 border-primary', className)} />
);

const JarvisPanel = ({ title, children, className, ...props }: JarvisPanelProps) => {
  return (
    <div
      className={cn(
        'relative border border-primary/20 bg-background/50 p-4 pt-5 backdrop-blur-sm shadow-[0_0_20px_hsl(var(--primary)/0.1)]',
        className
      )}
      {...props}
    >
      <Corner className="top-0 left-0 border-t-2 border-l-2" />
      <Corner className="top-0 right-0 border-t-2 border-r-2" />
      <Corner className="bottom-0 left-0 border-b-2 border-l-2" />
      <Corner className="bottom-0 right-0 border-b-2 border-r-2" />
      
      {title && (
        <h3 className="absolute -top-2.5 left-4 bg-background px-2 text-sm uppercase tracking-widest text-primary">
          {title}
        </h3>
      )}
      
      {children}
    </div>
  );
};

export default JarvisPanel;
