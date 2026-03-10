'use client';

import { useState, useEffect } from 'react';
import StartupAnimation from '@/components/os/startup-animation';
import HUD from '@/components/os/hud';
import ChatMode from '@/components/os/chat-mode';
import { cn } from '@/lib/utils';

export type AppMode = 'hud' | 'chat';

export default function Home() {
  const [booting, setBooting] = useState(true);
  const [mode, setMode] = useState<AppMode>('hud');

  useEffect(() => {
    const bootTimer = setTimeout(() => {
      setBooting(false);
    }, 4500); // Corresponds to the length of the startup animation

    return () => clearTimeout(bootTimer);
  }, []);

  const renderContent = () => {
    switch (mode) {
      case 'hud':
        return <HUD setMode={setMode} />;
      case 'chat':
        return <ChatMode setMode={setMode} />;
      default:
        return <HUD setMode={setMode} />;
    }
  };

  return (
    <div className={cn(
      "relative flex min-h-screen w-full items-center justify-center overflow-hidden",
      !booting && 'bg-background'
    )}>
      {booting ? <StartupAnimation /> : renderContent()}
    </div>
  );
}
