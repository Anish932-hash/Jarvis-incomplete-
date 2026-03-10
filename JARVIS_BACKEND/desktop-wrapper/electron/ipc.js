// JARVIS/desktop-wrapper/electron/ipc.js

export class DesktopIPC {
    static request(channel, payload = {}) {
        return window.JARVIS.invoke(channel, payload);
    }

    static send(channel, payload = {}) {
        window.JARVIS.send(channel, payload);
    }

    static on(channel, callback) {
        window.JARVIS.on(channel, callback);
    }

    // App diagnostics
    static getAppInfo() {
        return this.request("app.info");
    }

    // External browser links
    static openExternal(url) {
        this.send("open.external", url);
    }
}