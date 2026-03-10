'use client';

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { Zap, Loader, ShieldCheck } from "lucide-react";
import type { BoostStatus } from "@/hooks/use-system-metrics";

interface BoostButtonProps {
    boostStatus: BoostStatus;
    onClick: () => void;
}

const BoostButton = ({ boostStatus, onClick }: BoostButtonProps) => {
    const isIdle = boostStatus === 'idle';
    const isBoosting = boostStatus === 'boosting';
    const isCooling = boostStatus === 'cooling';
    
    const Icon = isBoosting ? Loader : isCooling ? ShieldCheck : Zap;
    const text = isBoosting ? "Boosting..." : isCooling ? "Cooldown" : "Boost Performance";

    return (
        <Button 
            onClick={onClick} 
            disabled={!isIdle} 
            className={cn(
                "w-64 text-lg tracking-wider transition-all duration-300",
                "bg-primary/10 text-primary border-2 border-primary/50 hover:bg-primary/20 hover:border-primary hover:shadow-[0_0_15px_hsl(var(--primary)/0.5)]",
                "disabled:opacity-70",
                {
                    "animate-pulse border-primary shadow-[0_0_25px_3px_hsl(var(--primary))]": isBoosting,
                    "border-green-400 text-green-400": isCooling,
                }
            )}
        >
            <Icon size={20} className={cn({"animate-spin": isBoosting})} />
            <span>{text}</span>
        </Button>
    )
};

export default BoostButton;
