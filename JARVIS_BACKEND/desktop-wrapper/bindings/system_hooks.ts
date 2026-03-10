// JARVIS/desktop-wrapper/bindings/system_hooks.ts

import { EventEmitter } from "events";
import os from "os";

export class SystemHooks extends EventEmitter {
    private interval: NodeJS.Timeout | null = null;

    constructor() {
        super();
    }

    public start() {
        if (this.interval) return;

        this.interval = setInterval(() => {
            const cpu = this.getCpuUsage();
            const ram = this.getRamUsage();
            const timestamp = Date.now();

            this.emit("system.stats", { cpu, ram, timestamp });
        }, 1500);

        this.setupWindowHooks();
    }

    public stop() {
        if (this.interval) clearInterval(this.interval);
    }

    private getCpuUsage() {
        const cpus = os.cpus();
        let idle = 0, total = 0;

        cpus.forEach((core) => {
            idle += core.times.idle;
            total += core.times.user + core.times.nice + core.times.sys + core.times.irq + core.times.idle;
        });

        return Number(((1 - idle / total) * 100).toFixed(2));
    }

    private getRamUsage() {
        const total = os.totalmem();
        const free = os.freemem();
        const used = total - free;

        return {
            used_mb: Math.round(used / 1024 / 1024),
            total_mb: Math.round(total / 1024 / 1024),
            percent: Number(((used / total) * 100).toFixed(2))
        };
    }

    private setupWindowHooks() {
        if (typeof window === "undefined") return;

        window.addEventListener("focus", () => {
            this.emit("window.focus");
        });

        window.addEventListener("blur", () => {
            this.emit("window.blur");
        });

        document.addEventListener("visibilitychange", () => {
            this.emit("window.visibility", {
                visible: !document.hidden,
                timestamp: Date.now()
            });
        });
    }
}

export const systemHooks = new SystemHooks();
systemHooks.start();