// JARVIS/desktop-wrapper/electron/preload.js

const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("JARVIS", {
    invoke: (channel, data) => ipcRenderer.invoke(channel, data),
    send: (channel, data) => ipcRenderer.send(channel, data),

    on: (channel, cb) => {
        ipcRenderer.on(channel, (event, ...args) => cb(...args));
    },
});