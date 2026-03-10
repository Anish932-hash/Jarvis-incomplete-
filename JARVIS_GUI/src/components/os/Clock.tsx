'use client';

import { useState, useEffect } from 'react';

const Clock = () => {
  const [date, setDate] = useState(new Date());

  useEffect(() => {
    const timerId = setInterval(() => setDate(new Date()), 1000);
    return () => clearInterval(timerId);
  }, []);

  const time = date.toLocaleTimeString('en-US', { hour12: false });
  const day = date.toLocaleDateString('en-US', { weekday: 'long' });
  const month = date.toLocaleDateString('en-US', { month: 'long' });
  const dayOfMonth = date.getDate();

  return (
    <div className="flex items-baseline gap-4 text-primary">
        <div className="text-5xl font-bold tabular-nums leading-none">
            {time}
        </div>
        <div className="flex flex-col text-left leading-tight border-l-2 border-primary/50 pl-4">
            <div className="text-2xl font-bold">{dayOfMonth} {month.substring(0,3).toUpperCase()}</div>
            <div className="text-xs tracking-widest">{day.toUpperCase()}</div>
        </div>
    </div>
  );
};

export default Clock;
