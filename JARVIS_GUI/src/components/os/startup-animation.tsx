'use client';

import { cn } from '@/lib/utils';
import { Orbitron } from 'next/font/google';
import { useState, useEffect } from 'react';

const orbitron = Orbitron({ subsets: ['latin'] });

const StartupAnimation = () => {
  const [particles, setParticles] = useState<React.CSSProperties[]>([]);
  const particleCount = 150; // Adjusted particle count

  useEffect(() => {
    // This logic should remain on the client to avoid hydration errors.
    if (typeof window !== 'undefined') {
      const newParticles = Array.from({ length: particleCount }).map((_, i) => {
        const angle = (i / particleCount) * Math.PI * 2;
        const radius = 65 + Math.random() * 15; // End near the core rings
        return {
          '--angle': `${angle}rad`,
          '--radius': `${radius}px`,
          '--delay': `${1.5 + Math.random() * 1.5}s`,
          '--duration': `${1.5 + Math.random() * 1}s`, // Faster animation
        } as React.CSSProperties;
      });
      setParticles(newParticles);
    }
  }, []);

  const jarvisName = 'JARVIS';

  return (
    <div className="flex flex-col items-center justify-center gap-8 text-primary overflow-hidden">
      <div className="relative h-64 w-64">
        {/* Phase 1: Background Grid */}
        <div className="absolute inset-[-100%] animate-fade-in [animation-duration:1.5s] [background-image:radial-gradient(hsl(var(--primary)/0.1)_1px,transparent_1px)] [background-size:1.5rem_1.5rem]" />

        {/* Phase 2: Core Assembly */}
        <div className="absolute inset-0 animate-fade-in [animation-delay:1s]">
          <div className="h-full w-full animate-[spin_20s_linear_infinite] rounded-full border-2 border-dashed border-primary/10" />
        </div>
        <div className="absolute inset-[10%] h-[80%] w-[80%] animate-[spin_15s_linear_infinite_reverse] rounded-full border border-dotted border-accent/20" />
        
        <svg
          viewBox="0 0 200 200"
          className="absolute inset-0 h-full w-full -rotate-90"
          style={{ '--glow-color': 'hsl(var(--primary))' } as React.CSSProperties}
        >
          <defs>
            <filter id="glow">
              <feGaussianBlur stdDeviation="3.5" result="coloredBlur" />
              <feMerge>
                <feMergeNode in="coloredBlur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>
          <g filter="url(#glow)" className="origin-center">
            {/* Core Pulse */}
            <circle cx="100" cy="100" r="5" className="fill-current text-primary animate-pulse-slow" style={{animationDelay: '0.5s'}} />
            
            {/* Incoming Electric Streaks */}
            {particles.map((style, i) => (
              <line
                key={i}
                x1="-4"
                y1="0"
                x2="4"
                y2="0"
                strokeWidth="1.5"
                className="stroke-current text-accent opacity-0 animate-particle-line-in"
                style={style}
              />
            ))}
            
            {/* Circuitry paths */}
            <g strokeWidth="0.5" className="fill-none stroke-primary/50 opacity-70">
                <path
                    className="animate-trace-in"
                    style={{ '--delay': '1.5s', strokeDasharray: 80, strokeDashoffset: 80 } as React.CSSProperties}
                    d="M 0 100 h 40 v -20 h 20"
                />
                <path
                    className="animate-trace-in"
                    style={{ '--delay': '1.6s', strokeDasharray: 80, strokeDashoffset: 80 } as React.CSSProperties}
                    d="M 200 100 h -40 v 20 h -20"
                />
                <path
                    className="animate-trace-in"
                    style={{ '--delay': '1.7s', strokeDasharray: 80, strokeDashoffset: 80 } as React.CSSProperties}
                    d="M 100 0 v 40 h -20 v 20"
                />
                <path
                    className="animate-trace-in"
                    style={{ '--delay': '1.8s', strokeDasharray: 80, strokeDashoffset: 80 } as React.CSSProperties}
                    d="M 100 200 v -40 h 20 v -20"
                />
                <path
                    className="animate-trace-in"
                    style={{ '--delay': '1.9s', strokeDasharray: 85, strokeDashoffset: 85 } as React.CSSProperties}
                    d="M 20 20 L 60 60"
                />
                 <path
                    className="animate-trace-in"
                    style={{ '--delay': '2.0s', strokeDasharray: 85, strokeDashoffset: 85 } as React.CSSProperties}
                    d="M 180 180 L 140 140"
                />
                 <path
                    className="animate-trace-in"
                    style={{ '--delay': '2.1s', strokeDasharray: 85, strokeDashoffset: 85 } as React.CSSProperties}
                    d="M 20 180 L 60 140"
                />
                 <path
                    className="animate-trace-in"
                    style={{ '--delay': '2.2s', strokeDasharray: 85, strokeDashoffset: 85 } as React.CSSProperties}
                    d="M 180 20 L 140 60"
                />
            </g>

            {/* Logo Rings */}
            <g className="fill-none stroke-current">
                <circle
                  className="animate-trace-in"
                  style={{ '--delay': '1s' } as React.CSSProperties}
                  cx="100"
                  cy="100"
                  r="60"
                  strokeWidth="2"
                  strokeDasharray="377"
                  strokeDashoffset="377"
                />
                <circle
                  className="animate-trace-in"
                  style={{ '--delay': '1.2s' } as React.CSSProperties}
                  cx="100"
                  cy="100"
                  r="70"
                  strokeWidth="1"
                  strokeDasharray="440"
                  strokeDashoffset="440"
                />
                <circle
                  className="animate-trace-in"
                  style={{ '--delay': '1.4s' } as React.CSSProperties}
                  cx="100"
                  cy="100"
                  r="80"
                  strokeWidth="2"
                  strokeDasharray="503"
                  strokeDashoffset="503"
                />
                {/* New animated arcs */}
                 <path
                  className="animate-trace-in"
                  style={{ '--delay': '2.2s', strokeDasharray: 90, strokeDashoffset: 90 } as React.CSSProperties}
                  d="M 20 100 A 80 80 0 0 1 100 20"
                  strokeWidth="1"
                />
                <path
                  className="animate-trace-in"
                  style={{ '--delay': '2.3s', strokeDasharray: 90, strokeDashoffset: 90 } as React.CSSProperties}
                  d="M 100 180 A 80 80 0 0 1 20 100"
                  strokeWidth="1"
                  transform="rotate(180 100 100)"
                />

                {/* Inner details */}
                <path
                  className="animate-trace-in"
                  style={{ '--delay': '1.8s' } as React.CSSProperties}
                  d="M100 50 V 150 M50 100 H 150"
                  strokeWidth="1"
                  strokeDasharray="200"
                  strokeDashoffset="200"
                />
                 <path
                  className="animate-trace-in"
                  style={{ '--delay': '2s' } as React.CSSProperties}
                  d="M 71.7 71.7 L 128.3 128.3 M 71.7 128.3 L 128.3 71.7"
                  strokeWidth="1"
                  strokeDasharray="160"
                  strokeDashoffset="160"
                />
            </g>
          </g>
        </svg>
      </div>
      {/* Phase 3: Text Boot */}
      <div className="relative flex h-12 items-center justify-center overflow-hidden">
        <div className="flex">
          {jarvisName.split('').map((letter, i) => (
            <span
              key={i}
              className={cn(
                'relative text-4xl font-bold tracking-[0.2em] opacity-0 animate-jarvis-letter-in',
                orbitron.className
              )}
              style={{ animationDelay: `${2.5 + i * 0.1}s` }}
            >
              {letter}
            </span>
          ))}
        </div>
        <div className="absolute top-0 left-0 h-full w-full animate-scanline bg-gradient-to-b from-transparent via-primary/20 to-transparent" style={{ animationDelay: '3.5s'}}/>
      </div>
      {/* Phase 4: System Online */}
      <p className="tracking-widest text-accent opacity-0 animate-fade-in [animation-delay:3.5s]">
        J.A.R.V.I.S. INITIALIZING...
      </p>
    </div>
  );
};

export default StartupAnimation;
