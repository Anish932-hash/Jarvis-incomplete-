'use client';

import { useState, useEffect, useRef } from 'react';
import { cn } from '@/lib/utils';
import { Orbitron } from 'next/font/google';

const orbitron = Orbitron({ subsets: ['latin'] });

type AiStatus = 'listening' | 'thinking' | 'speaking';

const LoopingVoiceHUD = () => {
  const [status, setStatus] = useState<AiStatus>('listening');
  const [particles, setParticles] = useState<React.CSSProperties[]>([]);
  const containerRef = useRef<HTMLDivElement>(null);
  const coreRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Generate particles only once on mount
    const newParticles = Array.from({ length: 150 }).map(() => { // Increased particle count
      const angle = Math.random() * 2 * Math.PI;
      const radius = 50 + Math.random() * 150;
      return {
        '--size': `${Math.random() * 2 + 1}px`,
        '--x-start': `${Math.cos(angle) * radius}px`,
        '--y-start': `${Math.sin(angle) * radius}px`,
        '--x-end': `${Math.cos(angle + (Math.random() - 0.5) * Math.PI) * radius}px`,
        '--y-end': `${Math.sin(angle + (Math.random() - 0.5) * Math.PI) * radius}px`,
        '--delay': `${Math.random() * 15}s`,
        '--duration': `${Math.random() * 10 + 10}s`,
      } as React.CSSProperties
    });
    setParticles(newParticles);
  }, []);
  
  useEffect(() => {
    let isMounted = true;
    const sequence = async () => {
      while (isMounted) {
        if (isMounted) setStatus('listening');
        await new Promise(resolve => setTimeout(resolve, 5000));
        if (!isMounted) break;
        
        if (isMounted) setStatus('thinking');
        await new Promise(resolve => setTimeout(resolve, 6000));
        if (!isMounted) break;

        if (isMounted) setStatus('speaking');
        await new Promise(resolve => setTimeout(resolve, 7000));
        if (!isMounted) break;
      }
    };
    sequence();
    return () => { isMounted = false; };
  }, []);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const handleMouseMove = (e: MouseEvent) => {
      if (!coreRef.current) return;
      const { left, top, width, height } = el.getBoundingClientRect();
      const x = (e.clientX - left) / width - 0.5;
      const y = (e.clientY - top) / height - 0.5;
      coreRef.current.style.transform = `rotateY(${x * 15}deg) rotateX(${-y * 15}deg)`; // Increased rotation
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

  const Hexagon = ({ className, ...props }: React.SVGProps<SVGSVGElement>) => (
    <svg viewBox="0 0 24 24" fill="currentColor" className={cn("h-full w-full", className)} {...props}>
      <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4a2 2 0 0 0 1-1.73z" />
    </svg>
  );

  return (
    <div className="relative flex flex-col items-center justify-center">
      <div ref={containerRef} className="h-[400px] w-[400px]" style={{ perspective: '1500px' }}>
        <div ref={coreRef} className="relative h-full w-full transition-transform duration-300 ease-out" style={{ transformStyle: 'preserve-3d' }}>
          
          {/* BG Layers */}
          <div className="absolute inset-0 rounded-full bg-primary/5 blur-3xl animate-pulse-slow" style={{ transform: 'translateZ(-150px)' }} />
          <div
            className="absolute inset-0"
            style={{
                transform: 'translateZ(-100px)',
                maskImage: 'radial-gradient(circle at center, white 20%, transparent 70%)',
            }}
          >
            <div className="absolute inset-0 animate-circuit-pulse [background-image:radial-gradient(hsl(var(--primary)/0.15)_1px,transparent_1px)] [background-size:1.5rem_1.5rem]" />
          </div>
          
          {/* Drifting Particles */}
          <div className={cn("absolute inset-0 transition-transform duration-1000", status === 'thinking' ? 'animate-particle-swirl' : '')} style={{ transform: 'translateZ(-80px)', transformStyle: 'preserve-3d' }}>
            {particles.slice(0, 50).map((style, i) => ( // Increased count
              <div key={i} className="absolute left-1/2 top-1/2 h-[var(--size)] w-[var(--size)] rounded-full bg-accent/70 animate-particle-drift" style={style} />
            ))}
          </div>

          {/* Main Rings (Gyroscope) */}
          <div className={cn("absolute inset-0 h-full w-full transition-transform duration-1000", status === 'thinking' ? 'scale-105' : 'scale-100')} style={{ transformStyle: 'preserve-3d' }}>
            <div className={cn("absolute inset-[2%] rounded-full border-2 border-dashed border-primary/10", status === 'thinking' ? 'animate-[spin_10s_linear_infinite]' : 'animate-[spin_40s_linear_infinite]')} style={{ transform: 'translateZ(-40px) rotateX(70deg)' }} />
            <div className="absolute inset-[8%] rounded-full border border-dotted border-accent/20 animate-[spin_30s_linear_infinite_reverse]" style={{ transform: 'translateZ(-30px) rotateX(70deg)' }} />
            <div className="absolute inset-[15%] rounded-full border border-primary/30 animate-flicker" style={{ transform: 'translateZ(0px) rotateX(70deg)' }} />
            <div className={cn("absolute inset-[22%] rounded-full border-2 border-accent/70", status === 'thinking' ? 'animate-[spin_8s_linear_infinite_reverse]' : 'animate-[spin_12s_linear_infinite_reverse]')} style={{ transform: 'translateZ(20px) rotateX(70deg)' }} />
            <div className="absolute inset-[28%] rounded-full border border-dashed border-primary/40 animate-[spin_20s_linear_infinite]" style={{ transform: 'translateZ(40px) rotateX(70deg)' }} />
            <div className="absolute inset-[18%] rounded-full border border-primary/20 animate-[spin_25s_linear_infinite]" style={{ transform: 'translateZ(10px) rotateX(-50deg) rotateY(20deg)' }} />
          </div>

          {/* State-specific Visuals */}
          <div className="absolute inset-0 h-full w-full" style={{ transformStyle: 'preserve-3d' }}>
            {/* LISTENING */}
            <div className={cn("absolute inset-0 transition-opacity duration-500", status === 'listening' ? 'opacity-100' : 'opacity-0')}>
              <svg viewBox="-100 -100 200 200" className="h-full w-full animate-pulse-slow">
                {Array.from({ length: 120 }).map((_, i) => (
                  <rect key={i} x="-1" y="-90" width="2" height="4" rx="1" fill="hsl(var(--accent))" transform={`rotate(${i * 3})`}
                    className="animate-waveform-bar" style={{ animationDelay: `${i * 0.025}s`, animationDuration: '1.5s' }} />
                ))}
              </svg>
              <div className="absolute inset-0">
                {particles.slice(50, 70).map((style, i) => ( // Using different particles
                    <div key={i}
                        className="absolute left-1/2 top-1/2 h-[var(--size)] w-[var(--size)] rounded-full bg-accent animate-particle-suck-in"
                        style={{ ...style, animationDuration: `${Math.random() * 2 + 1.5}s`, animationDelay: `${Math.random() * 1.5}s` }} />
                ))}
              </div>
            </div>
            
            {/* THINKING */}
            <div className={cn("absolute inset-0 transition-opacity duration-500", status === 'thinking' ? 'opacity-100' : 'opacity-0')}>
              <div className="absolute inset-[25%] animate-scanner-line-rotate" style={{'--speed': '2.5s'} as React.CSSProperties}>
                  <div className="absolute top-0 left-1/2 h-1/2 w-px animate-scanner-line-fade bg-gradient-to-b from-accent/0 to-accent" />
              </div>
              <div className="absolute inset-[25%] animate-scanner-line-rotate" style={{'--speed': '3.5s', animationDirection: 'reverse'} as React.CSSProperties}>
                  <div className="absolute bottom-0 left-1/2 h-1/2 w-px animate-scanner-line-fade bg-gradient-to-t from-accent/0 to-accent" />
              </div>
              <div className="absolute inset-[15%] animate-scanner-line-rotate" style={{'--speed': '6s'} as React.CSSProperties}>
                  <div className="absolute top-0 right-1/2 h-1/2 w-px animate-scanner-line-fade bg-gradient-to-b from-primary/0 to-primary/70" />
              </div>
               
              {Array.from({ length: 10 }).map((_, i) => ( // Increased glyph count
                <div key={i} className="absolute text-primary/30 animate-glyph-float" style={{
                  left: `${5 + i * 9}%`,
                  height: `${12 + (i%4)*5}px`,
                  animationDelay: `${i * 0.3}s`,
                  animationDuration: `${4 + Math.random() * 3}s`,
                  transform: `translateZ(${10 + Math.random()*50}px)`
                }}>
                  <Hexagon />
                </div>
              ))}
            </div>

            {/* SPEAKING */}
            <div className={cn("absolute inset-0 transition-opacity duration-500", status === 'speaking' ? 'opacity-100' : 'opacity-0')}>
              {Array.from({ length: 5 }).map((_, i) => ( // Increased ripple count
                <div key={i} className="absolute inset-0 rounded-full border-2 border-primary/70 animate-speaking-ripple" style={{ animationDelay: `${i * 0.4}s` }}/>
              ))}
              <div className="absolute inset-0 rounded-full animate-energy-pulse border-2 border-accent" />
            </div>
          </div>
          
          {/* Core Disc */}
          <div className="absolute inset-[32%] rounded-full" style={{ transform: 'translateZ(60px)', transformStyle: 'preserve-3d' }}>
            <div className="absolute inset-0 rounded-full bg-primary/20 animate-breathing-glow [animation-duration:3s]" />
            <div className="absolute inset-0 rounded-full border-2 border-primary animate-holographic-flicker" />
            <div className={cn("absolute inset-0 flex items-center justify-center text-4xl font-bold tracking-[0.2em] text-primary/90 [text-shadow:0_0_15px_hsl(var(--primary))]", orbitron.className)}>
              JARVIS
            </div>
          </div>

          {/* Multi-layered Orbiting Particles */}
          <div className="absolute inset-0" style={{ transformStyle: 'preserve-3d' }}>
            {particles.slice(70, 85).map((style, i) => (
              <div key={`orbit-a-${i}`} className="absolute inset-0 animate-orbit" style={{ 
                  ...style, 
                  '--radius': `${180 + i * 4}px`, 
                  '--duration': `${10 + i * 2}s`,
                  transform: 'translateZ(80px)' 
              } as React.CSSProperties}>
                  <div className="absolute left-1/2 top-1/2 h-1.5 w-1.5 rounded-full bg-accent" style={{ transform: 'translateX(var(--radius))' }} />
              </div>
            ))}
            {particles.slice(85, 100).map((style, i) => (
              <div key={`orbit-b-${i}`} className="absolute inset-0 animate-orbit" style={{ 
                  ...style, 
                  '--radius': `${190 + i * 3}px`, 
                  '--duration': `${12 + i * 2.5}s`,
                  animationDirection: 'reverse',
                  transform: 'rotateX(90deg) translateZ(10px)' 
              } as React.CSSProperties}>
                  <div className="absolute left-1/2 top-1/2 h-1 w-1 rounded-full bg-primary/80" style={{ transform: 'translateX(var(--radius))' }} />
              </div>
            ))}
            {particles.slice(100, 115).map((style, i) => (
              <div key={`orbit-c-${i}`} className="absolute inset-0 animate-orbit" style={{ 
                  ...style, 
                  '--radius': `${200 + i * 2}px`, 
                  '--duration': `${15 + i * 2}s`,
                  transform: 'rotateY(60deg) rotateX(-20deg) translateZ(-20px)' 
              } as React.CSSProperties}>
                  <div className="absolute left-1/2 top-1/2 h-1 w-1 rounded-full bg-accent/70 blur-[1px]" style={{ transform: 'translateX(var(--radius))' }} />
              </div>
            ))}
          </div>

        </div>
      </div>
      <div 
        key={status} 
        className={cn("absolute -bottom-4 text-center text-accent tracking-widest text-sm animate-fade-in-out", orbitron.className)}
        style={{ animationDuration: `${statusDurations[status]}s` }}
      >
        {statusText[status]}
      </div>
    </div>
  );
};

export default LoopingVoiceHUD;
