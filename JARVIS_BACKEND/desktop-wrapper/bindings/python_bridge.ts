// JARVIS/desktop-wrapper/bindings/python_bridge.ts

import net from "net";
import { EventEmitter } from "events";

export class PythonBridge extends EventEmitter {
    private client: net.Socket | null = null;
    private readonly host = "127.0.0.1";
    private readonly port = 7654;
    private buffer = "";

    constructor() {
        super();
        this.connect();
    }

    private connect() {
        this.client = new net.Socket();

        this.client.connect(this.port, this.host, () => {
            this.emit("connected");
        });

        this.client.on("data", (data) => {
            const chunk = data.toString("utf8");
            this.buffer += chunk;

            // Split messages by newline for streaming packets
            let boundary: number;
            while ((boundary = this.buffer.indexOf("\n")) >= 0) {
                const packet = this.buffer.slice(0, boundary);
                this.buffer = this.buffer.slice(boundary + 1);

                try {
                    const msg = JSON.parse(packet);
                    this.emit("message", msg);
                } catch (err) {
                    this.emit("error", new Error("Invalid JSON from Python"));
                }
            }
        });

        this.client.on("close", () => {
            this.emit("disconnected");
            setTimeout(() => this.connect(), 1500); // Auto-reconnect
        });

        this.client.on("error", (err) => {
            this.emit("error", err);
        });
    }

    public send(event: string, payload: any = {}) {
        if (!this.client) return;
        const packet = JSON.stringify({ event, payload }) + "\n";
        this.client.write(packet, "utf8");
    }

    public request(event: string, payload: any = {}): Promise<any> {
        return new Promise((resolve) => {
            const id = "req_" + Math.random().toString(36).slice(2);

            const handler = (msg: any) => {
                if (msg?.reply_to === id) {
                    this.off("message", handler);
                    resolve(msg.data);
                }
            };

            this.on("message", handler);

            this.send(event, { ...payload, request_id: id });
        });
    }
}

export const pythonBridge = new PythonBridge();