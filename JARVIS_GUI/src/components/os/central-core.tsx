'use client';

import { useState, useEffect, useRef } from 'react';
import { Mic } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import VoiceHUD from './voice-hud';
import { cn } from '@/lib/utils';
import { Orbitron } from 'next/font/google';

const orbitron = Orbitron({ subsets: ['latin'] });

type AiStatus = 'listening' | 'thinking' | 'speaking';

const Hexagon = ({ className, ...props }: React.SVGProps<SVGSVGElement>) => (
  <svg
    viewBox="0 0 24 24"
    fill="currentColor"
    className={cn('h-full w-full', className)}
    {...props}
  >
    <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4a2 2 0 0 0 1-1.73z" />
  </svg>
);

const CentralCore = () => {
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [status, setStatus] = useState<AiStatus>('listening');
  const [particles, setParticles] = useState<React.CSSProperties[]>([]);
  const containerRef = useRef<HTMLDivElement>(null);
  const coreRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const newParticles = Array.from({ length: 100 }).map(() => {
      const angle = Math.random() * 2 * Math.PI;
      const radius = 30 + Math.random() * 120;
      return {
        '--size': `${Math.random() * 2 + 1}px`,
        '--x-start': `${Math.cos(angle) * radius}px`,
        '--y-start': `${Math.sin(angle) * radius}px`,
        '--x-end': `${
          Math.cos(angle + (Math.random() - 0.5) * Math.PI) * radius
        }px`,
        '--y-end': `${
          Math.sin(angle + (Math.random() - 0.5) * Math.PI) * radius
        }px`,
        '--delay': `${Math.random() * 15}s`,
        '--duration': `${Math.random() * 10 + 10}s`,
      } as React.CSSProperties;
    });
    setParticles(newParticles);
  }, []);

  useEffect(() => {
    if (isDialogOpen) return; // Pause animation when dialog is open
    let isMounted = true;
    const sequence = async () => {
      while (isMounted) {
        if (isMounted) setStatus('listening');
        await new Promise((resolve) => setTimeout(resolve, 5000));
        if (!isMounted || isDialogOpen) break;

        if (isMounted) setStatus('thinking');
        await new Promise((resolve) => setTimeout(resolve, 6000));
        if (!isMounted || isDialogOpen) break;

        if (isMounted) setStatus('speaking');
        await new Promise((resolve) => setTimeout(resolve, 7000));
        if (!isMounted || isDialogOpen) break;
      }
    };
    sequence();
    return () => {
      isMounted = false;
    };
  }, [isDialogOpen]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const handleMouseMove = (e: MouseEvent) => {
      if (!coreRef.current) return;
      const { left, top, width, height } = el.getBoundingClientRect();
      const x = (e.clientX - left) / width - 0.5;
      const y = (e.clientY - top) / height - 0.5;
      coreRef.current.style.transform = `rotateY(${
        x * 10
      }deg) rotateX(${-y * 10}deg)`;
    };

    const handleMouseLeave = () => {
      if (coreRef.current) {
        coreRef.current.style.transform = 'rotateY(0deg) rotateX(0deg)';
      }
    };

    el.addEventListener('mousemove', handleMouseMove);
    el.addEventListener('mouseleave', handleMouseLeave);

    return () => {
      el.removeEventListener('mousemove', handleMouseMove);
      el.removeEventListener('mouseleave', handleMouseLeave);
    };
  }, []);

  const statusText: Record<AiStatus, string> = {
    listening: 'Listening...',
    thinking: 'Processing...',
    speaking: 'Responding...',
  };

  const statusDurations: Record<AiStatus, number> = {
    listening: 5,
    thinking: 6,
    speaking: 7,
  };

  return (
    <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
      <div
        ref={containerRef}
        className="relative flex h-[300px] w-[300px] flex-col items-center justify-center"
        style={{ perspective: '1000px' }}
      >
        <div
          ref={coreRef}
          className="relative h-full w-full transition-transform duration-300 ease-out"
          style={{ transformStyle: 'preserve-3d' }}
        >
          {/* BG Layers */}
          <div
            className="animate-pulse-slow absolute inset-0 rounded-full bg-primary/5 blur-3xl"
            style={{ transform: 'translateZ(-150px)' }}
          />

          {/* Drifting Particles */}
          <div
            className={cn(
              'absolute inset-0 transition-transform duration-1000',
              status === 'thinking' ? 'animate-particle-swirl' : ''
            )}
            style={{ transform: 'translateZ(-80px)', transformStyle: 'preserve-3d' }}
          >
            {particles.slice(0, 40).map((style, i) => (
              <div
                key={i}
                className="absolute left-1/2 top-1/2 h-[var(--size)] w-[var(--size)] animate-particle-drift rounded-full bg-accent/70"
                style={style}
              />
            ))}
          </div>

          {/* Main Rings */}
          <div
            className={cn(
              'absolute inset-0 h-full w-full transition-transform duration-1000',
              status === 'thinking' ? 'scale-105' : 'scale-100'
            )}
            style={{ transformStyle: 'preserve-3d' }}
          >
            <div
              className={cn(
                'absolute inset-[2%] rounded-full border-2 border-dashed border-primary/10',
                status === 'thinking'
                  ? 'animate-[spin_10s_linear_infinite]'
                  : 'animate-[spin_40s_linear_infinite]'
              )}
              style={{ transform: 'translateZ(-40px)' }}
            />
            <div
              className="absolute inset-[8%] animate-[spin_30s_linear_infinite_reverse] rounded-full border border-dotted border-accent/20"
              style={{ transform: 'translateZ(-30px)' }}
            />
            <div
              className="absolute inset-[15%] animate-flicker rounded-full border border-primary/30"
              style={{ transform: 'translateZ(0px)' }}
            />
            <div
              className={cn(
                'absolute inset-[22%] rounded-full border-2 border-accent/70',
                status === 'thinking'
                  ? 'animate-[spin_8s_linear_infinite_reverse]'
                  : 'animate-[spin_12s_linear_infinite_reverse]'
              )}
              style={{ transform: 'translateZ(20px)' }}
            />
            <div
              className="absolute inset-[28%] animate-[spin_20s_linear_infinite] rounded-full border border-dashed border-primary/40"
              style={{ transform: 'translateZ(40px)' }}
            />
          </div>

          {/* State-specific Visuals */}
          <div className="absolute inset-0 h-full w-full" style={{ transformStyle: 'preserve-3d' }}>
            {/* LISTENING */}
            <div
              className={cn(
                'absolute inset-0 transition-opacity duration-500',
                !isDialogOpen && status === 'listening' ? 'opacity-100' : 'opacity-0'
              )}
            >
              <svg
                viewBox="-100 -100 200 200"
                className="h-full w-full animate-pulse-slow"
              >
                {Array.from({ length: 120 }).map((_, i) => (
                  <rect
                    key={i}
                    x="-1"
                    y="-90"
                    width="2"
                    height="4"
                    rx="1"
                    fill="hsl(var(--accent))"
                    transform={`rotate(${i * 3})`}
                    className="animate-waveform-bar"
                    style={{
                      animationDelay: `${i * 0.025}s`,
                      animationDuration: '1.5s',
                    }}
                  />
                ))}
              </svg>
              <div className="absolute inset-0">
                {particles.slice(40, 60).map((style, i) => (
                  <div
                    key={i}
                    className="absolute left-1/2 top-1/2 h-[var(--size)] w-[var(--size)] animate-particle-suck-in rounded-full bg-accent"
                    style={{
                      ...style,
                      animationDuration: `${Math.random() * 2 + 1.5}s`,
                      animationDelay: `${Math.random() * 1.5}s`,
                    }}
                  />
                ))}
              </div>
            </div>

            {/* THINKING */}
            <div
              className={cn(
                'absolute inset-0 transition-opacity duration-500',
                !isDialogOpen && status === 'thinking' ? 'opacity-100' : 'opacity-0'
              )}
            >
              <div
                className="absolute inset-[25%] animate-scanner-line-rotate"
                style={{ '--speed': '4s' } as React.CSSProperties}
              >
                <div className="absolute top-0 left-1/2 h-1/2 w-px animate-scanner-line-fade bg-gradient-to-b from-accent/0 to-accent" />
              </div>
              <div
                className="absolute inset-[25%] animate-scanner-line-rotate"
                style={
                  {
                    '--speed': '5s',
                    animationDirection: 'reverse',
                  } as React.CSSProperties
                }
              >
                <div className="absolute bottom-0 left-1/2 h-1/2 w-px animate-scanner-line-fade bg-gradient-to-t from-accent/0 to-accent" />
              </div>

              {Array.from({ length: 7 }).map((_, i) => (
                <div
                  key={i}
                  className="absolute animate-glyph-float text-primary/30"
                  style={{
                    left: `${10 + i * 13}%`,
                    height: `${15 + (i % 3) * 5}px`,
                    animationDelay: `${i * 0.5}s`,
                    animationDuration: `${5 + Math.random() * 4}s`,
                    transform: `translateZ(${20 + Math.random() * 40}px)`,
                  }}
                >
                  <Hexagon />
                </div>
              ))}
            </div>

            {/* SPEAKING */}
            <div
              className={cn(
                'absolute inset-0 transition-opacity duration-500',
                !isDialogOpen && status === 'speaking' ? 'opacity-100' : 'opacity-0'
              )}
            >
              {Array.from({ length: 4 }).map((_, i) => (
                <div
                  key={i}
                  className="absolute inset-0 animate-speaking-ripple rounded-full border-2 border-primary/70"
                  style={{ animationDelay: `${i * 0.5}s` }}
                />
              ))}
              <div className="absolute inset-0 animate-energy-pulse rounded-full border-2 border-accent" />
            </div>
          </div>

          {/* Orbiting Particles */}
          <div
            className="absolute inset-0"
            style={{ transform: 'translateZ(80px)', transformStyle: 'preserve-3d' }}
          >
            {particles.slice(60, 70).map((style, i) => (
              <div
                key={i}
                className="absolute inset-0 animate-orbit"
                style={{
                  ...style,
                  '--radius': `${130 + i * 2}px`,
                  '--duration': `${10 + i * 2}s`,
                } as React.CSSProperties}
              >
                <div
                  className="absolute left-1/2 top-1/2 h-1.5 w-1.5 rounded-full bg-accent"
                  style={{ transform: 'translateX(var(--radius))' }}
                />
              </div>
            ))}
          </div>

          {/* Center Button */}
          <DialogTrigger asChild>
            <button
              className="group absolute inset-[32%] rounded-full transition-all duration-300 ease-in-out hover:scale-105"
              style={{ transform: 'translateZ(60px)', transformStyle: 'preserve-3d' }}
            >
              <div className="absolute inset-0 animate-breathing-glow rounded-full bg-primary/20 group-hover:animate-none group-hover:bg-primary/30" />
              <div className="absolute inset-0 animate-holographic-flicker rounded-full border-2 border-primary group-hover:border-accent" />
              <div
                className={cn(
                  'absolute inset-0 flex items-center justify-center text-primary/90 opacity-100 transition-opacity duration-300 group-hover:opacity-0',
                  orbitron.className
                )}
              >
                <div className="text-4xl font-bold tracking-[0.2em] [text-shadow:0_0_15px_hsl(var(--primary))]">
                  JARVIS
                </div>
              </div>
              <div className="absolute inset-0 flex items-center justify-center text-primary opacity-0 transition-opacity duration-300 group-hover:opacity-100">
                <Mic className="h-12 w-12 text-accent [filter:drop-shadow(0_0_10px_hsl(var(--accent)))]" />
              </div>
            </button>
          </DialogTrigger>
        </div>
        {!isDialogOpen && (
          <div
            key={status}
            className={cn(
              'absolute -bottom-4 animate-fade-in-out text-center text-sm tracking-widest text-accent',
              orbitron.className
            )}
            style={{ animationDuration: `${statusDurations[status]}s` }}
          >
            {statusText[status]}
          </div>
        )}
      </div>
      <DialogContent className="flex h-auto w-auto max-w-none items-center justify-center border-none bg-transparent p-0 shadow-none focus-visible:ring-0">
        <DialogTitle className="sr-only">Voice Mode</DialogTitle>
        <DialogDescription className="sr-only">
          Voice interaction mode for J.A.R.V.I.S. has been activated.
        </DialogDescription>
        {isDialogOpen && (
          <VoiceHUD closeDialog={() => setIsDialogOpen(false)} />
        )}
      </DialogContent>
    </Dialog>
  );
};

export default CentralCore;
