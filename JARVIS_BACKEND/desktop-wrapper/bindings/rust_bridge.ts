// JARVIS/desktop-wrapper/bindings/rust_bridge.ts

import { spawn, ChildProcessWithoutNullStreams } from "child_process";
import { EventEmitter } from "events";

type PendingRequest = {
    resolve: (value: any) => void;
    reject: (reason?: unknown) => void;
    timeout: NodeJS.Timeout;
};

export class RustBridge extends EventEmitter {
    private process: ChildProcessWithoutNullStreams | null = null;
    private buffer = "";
    private pending = new Map<string, PendingRequest>();

    constructor() {
        super();
        this.launch();
    }

    private launch() {
        const binaryPath = "backend/rust/target/release/jarvis_backend_bin";

        this.process = spawn(binaryPath, [], {
            windowsHide: true,
            stdio: ["pipe", "pipe", "pipe"]
        });

        this.process.stdout.on("data", (data) => {
            this.buffer += data.toString("utf8");
            let boundary;

            while ((boundary = this.buffer.indexOf("\n")) >= 0) {
                const packet = this.buffer.slice(0, boundary);
                this.buffer = this.buffer.slice(boundary + 1);

                try {
                    const msg = JSON.parse(packet);
                    this.resolvePending(msg);
                    this.emit("message", msg);
                } catch (err) {
                    this.emit("error", new Error("Invalid JSON from Rust"));
                }
            }
        });

        this.process.stderr.on("data", (data) => {
            this.emit("error", new Error("Rust STDERR: " + data.toString()));
        });

        this.process.on("exit", () => {
            this.rejectPending(new Error("Rust bridge process exited."));
            this.emit("crashed");
            setTimeout(() => this.launch(), 2000); // restart
        });
    }

    private resolvePending(msg: any) {
        const replyTo = String(msg?.reply_to || "").trim();
        if (!replyTo) return;
        const pending = this.pending.get(replyTo);
        if (!pending) return;

        this.pending.delete(replyTo);
        clearTimeout(pending.timeout);
        if (String(msg?.status || "success").toLowerCase() === "error") {
            pending.reject(new Error(String(msg?.message || "Rust request failed")));
            return;
        }
        pending.resolve(msg?.data);
    }

    private rejectPending(reason: Error) {
        for (const [id, pending] of this.pending.entries()) {
            clearTimeout(pending.timeout);
            pending.reject(reason);
            this.pending.delete(id);
        }
    }

    public send(event: string, payload: any = {}) {
        if (!this.process) return;
        const packet = JSON.stringify({ event, payload }) + "\n";
        this.process.stdin.write(packet, "utf8", (err) => {
            if (err) {
                this.emit("error", new Error("Failed to write packet to Rust process."));
            }
        });
    }

    public async request(event: string, payload: any = {}, timeoutMs = 10000) {
        return new Promise((resolve, reject) => {
            const id = "rust_req_" + Math.random().toString(36).slice(2);
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error(`Rust request timed out for event '${event}'.`));
            }, Math.max(500, timeoutMs));
            this.pending.set(id, { resolve, reject, timeout });
            this.send(event, { ...payload, request_id: id });
        });
    }

    public async healthCheck() {
        return this.request("health_check", {});
    }
}

export const rustBridge = new RustBridge();
