// JARVIS/desktop-wrapper/electron/main.js

const { app, BrowserWindow, ipcMain, shell, nativeTheme } = require("electron");
const path = require("path");
const os = require("os");

let win = null;

function createWindow() {
    win = new BrowserWindow({
        width: 1450,
        height: 900,
        backgroundColor: "#000000",
        autoHideMenuBar: true,
        frame: true,
        webPreferences: {
            preload: path.join(__dirname, "preload.js"),
            contextIsolation: true,
            nodeIntegration: false,
            devTools: true,
        },
    });

    win.loadFile(path.join(__dirname, "index.html"));

    win.on("closed", () => {
        win = null;
    });
}

app.whenReady().then(() => {
    createWindow();

    app.on("activate", () => {
        if (BrowserWindow.getAllWindows().length === 0) createWindow();
    });
});

app.on("window-all-closed", () => {
    if (process.platform !== "darwin") app.quit();
});

// --------------------------------------------------
// Application Diagnostics Events
// --------------------------------------------------

ipcMain.handle("app.info", () => {
    return {
        version: app.getVersion(),
        platform: os.platform(),
        arch: os.arch(),
        cpus: os.cpus().length,
        memory: os.totalmem(),
        home: os.homedir(),
        gpu: process.getGPUFeatureStatus(),
        theme: nativeTheme.shouldUseDarkColors ? "dark" : "light",
    };
});

// Open external links safely
ipcMain.on("open.external", (_, url) => {
    shell.openExternal(url);
});