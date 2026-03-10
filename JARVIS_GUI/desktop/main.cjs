const { app, BrowserWindow, dialog, ipcMain, shell } = require('electron');
const fs = require('fs');
const http = require('http');
const net = require('net');
const path = require('path');
const { spawn, spawnSync } = require('child_process');
const os = require('os');

let FRONTEND_PORT = Number(process.env.JARVIS_FRONTEND_PORT || 3210);
let BACKEND_PORT = Number(process.env.JARVIS_BACKEND_PORT || 8765);
const BACKEND_HOST = process.env.JARVIS_BACKEND_HOST || '127.0.0.1';

let mainWindow = null;
let backendProcess = null;
let frontendProcess = null;
let staticServer = null;
let ownsBackendProcess = false;
let ownsFrontendProcess = false;
let runtimeWatchdogTimer = null;
let runtimeWatchdogInFlight = false;
let runtimeWatchdogBackendRestarting = false;
let runtimeWatchdogFrontendRestarting = false;
let appIsShuttingDown = false;
const FALLBACK_LOG_FILE = path.join(process.cwd(), 'jarvis-desktop-main.log');
const hasSingleInstanceLock = app.requestSingleInstanceLock();
const WATCHDOG_INTERVAL_MS = Math.max(
  5000,
  Math.min(120000, Number(process.env.JARVIS_RUNTIME_WATCHDOG_INTERVAL_MS || 15000))
);

if (!hasSingleInstanceLock) {
  app.quit();
}

function isWindows() {
  return process.platform === 'win32';
}

function isBrokenPipeError(error) {
  if (!error || typeof error !== 'object') return false;
  const code = String(error.code || '');
  return code === 'EPIPE' || code === 'ERR_STREAM_DESTROYED';
}

function appendFallbackLog(line) {
  try {
    fs.appendFileSync(FALLBACK_LOG_FILE, `${new Date().toISOString()} ${line}\n`, 'utf8');
  } catch (_) {
    // Ignore fallback logging errors.
  }
}

function safeConsoleWrite(method, line) {
  try {
    if (method === 'error') {
      console.error(line);
    } else {
      console.log(line);
    }
  } catch (error) {
    if (isBrokenPipeError(error)) {
      appendFallbackLog(line);
      return;
    }
    appendFallbackLog(`${line} | console_write_error=${String(error)}`);
  }
}

function log(prefix, message) {
  const line = `[${prefix}] ${message}`;
  safeConsoleWrite('log', line);
  appendFallbackLog(line);
}

function logError(prefix, message) {
  const line = `[${prefix}] ${message}`;
  safeConsoleWrite('error', line);
  appendFallbackLog(line);
}

function cleanEnv(input) {
  const output = {};
  for (const [key, value] of Object.entries(input || {})) {
    if (typeof value === 'string') {
      output[key] = value;
    }
  }
  return output;
}

function backendRootPath() {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, 'JARVIS_BACKEND');
  }
  return path.resolve(__dirname, '..', '..', 'JARVIS_BACKEND');
}

function guiRootPath() {
  return path.resolve(__dirname, '..');
}

function frontendBaseUrl() {
  return `http://127.0.0.1:${FRONTEND_PORT}`;
}

function backendBaseUrl() {
  return `http://${BACKEND_HOST}:${BACKEND_PORT}`;
}

function resolvePythonExecutable(backendRoot) {
  const explicit = process.env.JARVIS_PYTHON;
  const candidates = [
    explicit,
    path.join(backendRoot, '.venv', 'Scripts', 'python.exe'),
    path.join(path.dirname(backendRoot), '.venv', 'Scripts', 'python.exe'),
    'python',
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (candidate === 'python') {
      return candidate;
    }
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return 'python';
}

function killProcessTree(child) {
  if (!child || !child.pid) return;
  try {
    if (isWindows()) {
      spawnSync('taskkill', ['/pid', String(child.pid), '/t', '/f'], {
        windowsHide: true,
        stdio: 'ignore',
      });
    } else {
      child.kill('SIGTERM');
    }
  } catch (error) {
    log('PROC', `Failed to kill process ${child.pid}: ${String(error)}`);
  }
}

async function waitForHttp(url, timeoutMs = 60000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok || response.status < 500) {
        return;
      }
    } catch (_) {
      // retry
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

async function isHttpReachable(url, timeoutMs = 2500) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal });
    return response.ok || response.status < 500;
  } catch (_) {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

async function canBindPort(port, host = '127.0.0.1') {
  return new Promise((resolve) => {
    const tester = net.createServer();
    tester.once('error', () => resolve(false));
    tester.once('listening', () => {
      tester.close(() => resolve(true));
    });
    tester.listen(port, host);
  });
}

async function pickAvailablePort(preferred, host = '127.0.0.1', span = 30) {
  if (await canBindPort(preferred, host)) {
    return preferred;
  }
  for (let next = preferred + 1; next < preferred + span; next += 1) {
    if (await canBindPort(next, host)) {
      return next;
    }
  }
  return preferred;
}

function startBackend() {
  if (backendProcess) return;

  const backendRoot = backendRootPath();
  const python = resolvePythonExecutable(backendRoot);
  const args = ['-m', 'backend.python.desktop_api', '--host', BACKEND_HOST, '--port', String(BACKEND_PORT)];

  backendProcess = spawn(python, args, {
    cwd: backendRoot,
    env: {
      ...cleanEnv(process.env),
      PYTHONUNBUFFERED: '1',
    },
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  backendProcess.stdout.on('data', (chunk) => log('BACKEND', chunk.toString().trim()));
  backendProcess.stderr.on('data', (chunk) => log('BACKEND', chunk.toString().trim()));
  backendProcess.on('exit', (code) => {
    log('BACKEND', `Exited with code ${code}`);
    backendProcess = null;
    ownsBackendProcess = false;
  });
  ownsBackendProcess = true;
}

function startFrontendDevServer() {
  if (frontendProcess) return;

  const command = isWindows()
    ? `npm run dev -- --hostname 127.0.0.1 --port ${FRONTEND_PORT}`
    : `npm run dev -- --hostname 127.0.0.1 --port ${FRONTEND_PORT}`;

  frontendProcess = spawn(command, [], {
    cwd: guiRootPath(),
    env: cleanEnv(process.env),
    windowsHide: true,
    shell: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  frontendProcess.stdout.on('data', (chunk) => log('FRONTEND', chunk.toString().trim()));
  frontendProcess.stderr.on('data', (chunk) => log('FRONTEND', chunk.toString().trim()));
  frontendProcess.on('exit', (code) => {
    log('FRONTEND', `Exited with code ${code}`);
    frontendProcess = null;
    ownsFrontendProcess = false;
  });
  ownsFrontendProcess = true;
}

function serveStaticFile(filePath, res) {
  const ext = path.extname(filePath).toLowerCase();
  const typeMap = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
  };

  const contentType = typeMap[ext] || 'application/octet-stream';
  fs.readFile(filePath, (error, content) => {
    if (error) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(content);
  });
}

function startFrontendStaticServer() {
  if (staticServer) return;

  const outDir = path.join(guiRootPath(), 'out');
  if (!fs.existsSync(outDir)) {
    throw new Error(`Missing static export at ${outDir}. Run "npm run build" first.`);
  }

  staticServer = http.createServer((req, res) => {
    const incoming = decodeURIComponent(req.url || '/');
    const clean = incoming.split('?')[0];

    let relativePath = clean;
    if (relativePath === '/') {
      relativePath = '/index.html';
    } else if (!path.extname(relativePath)) {
      relativePath = relativePath.endsWith('/') ? `${relativePath}index.html` : `${relativePath}/index.html`;
    }

    const safePath = path.normalize(relativePath).replace(/^(\.\.[/\\])+/, '');
    const target = path.join(outDir, safePath);

    if (!target.startsWith(outDir)) {
      res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Forbidden');
      return;
    }

    if (fs.existsSync(target) && fs.statSync(target).isFile()) {
      serveStaticFile(target, res);
      return;
    }

    const fallback = path.join(outDir, 'index.html');
    serveStaticFile(fallback, res);
  });

  staticServer.listen(FRONTEND_PORT, '127.0.0.1');
  log('FRONTEND', `Serving static UI from ${outDir} on ${frontendBaseUrl()}`);
}

function stopAllRuntimeProcesses() {
  if (staticServer) {
    staticServer.close();
    staticServer = null;
  }
  if (ownsFrontendProcess) {
    killProcessTree(frontendProcess);
  }
  if (ownsBackendProcess) {
    killProcessTree(backendProcess);
  }
  frontendProcess = null;
  backendProcess = null;
  ownsFrontendProcess = false;
  ownsBackendProcess = false;
}

function stopRuntimeWatchdog() {
  if (runtimeWatchdogTimer) {
    clearInterval(runtimeWatchdogTimer);
    runtimeWatchdogTimer = null;
  }
  runtimeWatchdogInFlight = false;
  runtimeWatchdogBackendRestarting = false;
  runtimeWatchdogFrontendRestarting = false;
}

async function restartBackendFromWatchdog(reason) {
  if (appIsShuttingDown || runtimeWatchdogBackendRestarting) return false;
  runtimeWatchdogBackendRestarting = true;
  try {
    log('WATCHDOG', `Backend restart requested: ${reason}`);
    if (ownsBackendProcess && backendProcess) {
      killProcessTree(backendProcess);
      backendProcess = null;
      ownsBackendProcess = false;
      await new Promise((resolve) => setTimeout(resolve, 700));
    } else if (!(await canBindPort(BACKEND_PORT, BACKEND_HOST))) {
      const fallbackPort = await pickAvailablePort(BACKEND_PORT, BACKEND_HOST, 40);
      if (fallbackPort !== BACKEND_PORT) {
        BACKEND_PORT = fallbackPort;
        log('WATCHDOG', `Backend port remapped to ${BACKEND_PORT}`);
      }
    }

    startBackend();
    await waitForHttp(`${backendBaseUrl()}/health`, 60000);
    log('WATCHDOG', `Backend healthy on ${backendBaseUrl()}`);
    return true;
  } catch (error) {
    logError('WATCHDOG', `Backend restart failed: ${String(error)}`);
    return false;
  } finally {
    runtimeWatchdogBackendRestarting = false;
  }
}

async function restartFrontendFromWatchdog(reason) {
  if (appIsShuttingDown || runtimeWatchdogFrontendRestarting) return false;
  runtimeWatchdogFrontendRestarting = true;
  try {
    log('WATCHDOG', `Frontend restart requested: ${reason}`);
    if (app.isPackaged) {
      if (staticServer) {
        staticServer.close();
        staticServer = null;
      }
      if (!(await canBindPort(FRONTEND_PORT, '127.0.0.1'))) {
        const fallbackPort = await pickAvailablePort(FRONTEND_PORT, '127.0.0.1', 60);
        if (fallbackPort !== FRONTEND_PORT) {
          FRONTEND_PORT = fallbackPort;
          log('WATCHDOG', `Frontend port remapped to ${FRONTEND_PORT}`);
        }
      }
      startFrontendStaticServer();
    } else {
      if (ownsFrontendProcess && frontendProcess) {
        killProcessTree(frontendProcess);
        frontendProcess = null;
        ownsFrontendProcess = false;
        await new Promise((resolve) => setTimeout(resolve, 700));
      } else if (!(await canBindPort(FRONTEND_PORT, '127.0.0.1'))) {
        const existingFrontendReachable = await isHttpReachable(`${frontendBaseUrl()}/`, 1500);
        if (!existingFrontendReachable) {
          const fallbackPort = await pickAvailablePort(FRONTEND_PORT, '127.0.0.1', 60);
          if (fallbackPort !== FRONTEND_PORT) {
            FRONTEND_PORT = fallbackPort;
            log('WATCHDOG', `Frontend port remapped to ${FRONTEND_PORT}`);
          }
        }
      }
      startFrontendDevServer();
    }

    const frontendUrl = `${frontendBaseUrl()}/`;
    await waitForHttp(frontendUrl, 90000);
    if (mainWindow && !mainWindow.isDestroyed()) {
      await mainWindow.loadURL(frontendBaseUrl());
    }
    log('WATCHDOG', `Frontend healthy on ${frontendBaseUrl()}`);
    return true;
  } catch (error) {
    logError('WATCHDOG', `Frontend restart failed: ${String(error)}`);
    return false;
  } finally {
    runtimeWatchdogFrontendRestarting = false;
  }
}

async function runtimeWatchdogTick() {
  if (appIsShuttingDown || runtimeWatchdogInFlight) return;
  runtimeWatchdogInFlight = true;
  try {
    const backendHealthy = await isHttpReachable(`${backendBaseUrl()}/health`, 2500);
    if (!backendHealthy) {
      await restartBackendFromWatchdog('health endpoint unavailable');
    }

    const frontendHealthy = await isHttpReachable(`${frontendBaseUrl()}/`, 2500);
    if (!frontendHealthy) {
      await restartFrontendFromWatchdog('frontend endpoint unavailable');
      return;
    }

    if (mainWindow && !mainWindow.isDestroyed() && mainWindow.webContents.isCrashed()) {
      logError('WATCHDOG', 'Renderer process crashed; reloading window');
      await mainWindow.loadURL(frontendBaseUrl());
    }
  } catch (error) {
    logError('WATCHDOG', `Watchdog tick failed: ${String(error)}`);
  } finally {
    runtimeWatchdogInFlight = false;
  }
}

function startRuntimeWatchdog() {
  if (appIsShuttingDown || runtimeWatchdogTimer) return;
  log('WATCHDOG', `Starting runtime watchdog every ${WATCHDOG_INTERVAL_MS}ms`);
  runtimeWatchdogTimer = setInterval(() => {
    void runtimeWatchdogTick();
  }, WATCHDOG_INTERVAL_MS);
  setTimeout(() => {
    void runtimeWatchdogTick();
  }, Math.min(5000, WATCHDOG_INTERVAL_MS));
}

async function startRuntime() {
  if (!(await canBindPort(BACKEND_PORT, BACKEND_HOST))) {
    const existingBackendReachable = await isHttpReachable(`${backendBaseUrl()}/health`, 2000);
    if (!existingBackendReachable) {
      const fallbackBackendPort = await pickAvailablePort(BACKEND_PORT, BACKEND_HOST, 40);
      if (fallbackBackendPort !== BACKEND_PORT) {
        BACKEND_PORT = fallbackBackendPort;
        log('BACKEND', `Primary port busy; switching to ${BACKEND_PORT}`);
      }
    }
  }

  let backendHealthUrl = `${backendBaseUrl()}/health`;
  let frontendUrl = `${frontendBaseUrl()}/`;

  const backendAlreadyRunning = await isHttpReachable(backendHealthUrl, 2000);
  if (backendAlreadyRunning) {
    log('BACKEND', `Using existing backend at ${backendBaseUrl()}`);
  } else {
    startBackend();
    backendHealthUrl = `${backendBaseUrl()}/health`;
    await waitForHttp(backendHealthUrl, 60000);
  }

  if (app.isPackaged) {
    startFrontendStaticServer();
  } else {
    if (!(await canBindPort(FRONTEND_PORT, '127.0.0.1'))) {
      const existingFrontendReachable = await isHttpReachable(frontendUrl, 1500);
      if (!existingFrontendReachable) {
        const fallbackFrontendPort = await pickAvailablePort(FRONTEND_PORT, '127.0.0.1', 40);
        if (fallbackFrontendPort !== FRONTEND_PORT) {
          FRONTEND_PORT = fallbackFrontendPort;
          log('FRONTEND', `Primary port busy; switching to ${FRONTEND_PORT}`);
        }
      }
    }
    frontendUrl = `${frontendBaseUrl()}/`;
    const frontendAlreadyRunning = await isHttpReachable(frontendUrl, 1500);
    if (frontendAlreadyRunning) {
      log('FRONTEND', `Using existing frontend at ${frontendBaseUrl()}`);
    } else {
      startFrontendDevServer();
    }
  }

  try {
    await waitForHttp(frontendUrl, 90000);
  } catch (error) {
    if (!app.isPackaged) {
      logError('FRONTEND', `Initial startup failed on ${frontendUrl}: ${String(error)}`);
      if (ownsFrontendProcess) {
        killProcessTree(frontendProcess);
        frontendProcess = null;
        ownsFrontendProcess = false;
      }

      const retryPort = await pickAvailablePort(FRONTEND_PORT + 1, '127.0.0.1', 60);
      FRONTEND_PORT = retryPort;
      log('FRONTEND', `Retrying startup on ${FRONTEND_PORT}`);
      startFrontendDevServer();
      frontendUrl = `${frontendBaseUrl()}/`;
      await waitForHttp(frontendUrl, 90000);
      return;
    }
    throw error;
  }
}

function registerIpcHandlers() {
  ipcMain.handle('jarvis:api:request', async (_event, input) => {
    try {
      const pathInput = typeof input?.path === 'string' ? input.path : '/health';
      const method = typeof input?.method === 'string' ? input.method.toUpperCase() : 'GET';
      const payload = typeof input?.payload === 'object' && input.payload ? input.payload : {};
      const normalizedPath = pathInput.startsWith('/') ? pathInput : `/${pathInput}`;

      const response = await fetch(`${backendBaseUrl()}${normalizedPath}`, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: method === 'POST' ? JSON.stringify(payload) : undefined,
      });

      const bodyText = await response.text();
      let parsed = {};
      try {
        parsed = bodyText ? JSON.parse(bodyText) : {};
      } catch (_) {
        parsed = { raw: bodyText };
      }

      if (!response.ok) {
        const detail = typeof parsed?.message === 'string' ? parsed.message : `Backend error (${response.status})`;
        return {
          __error: true,
          status: response.status,
          message: detail,
          data: parsed,
        };
      }

      return parsed;
    } catch (error) {
      return {
        __error: true,
        status: 500,
        message: error instanceof Error ? error.message : String(error),
      };
    }
  });

  ipcMain.handle('jarvis:app-info', async () => ({
    appVersion: app.getVersion(),
    electronVersion: process.versions.electron,
    chromiumVersion: process.versions.chrome,
    nodeVersion: process.versions.node,
    platform: process.platform,
    arch: process.arch,
    cpus: os.cpus().length,
    memoryTotal: os.totalmem(),
    backendUrl: backendBaseUrl(),
    frontendUrl: frontendBaseUrl(),
  }));

  ipcMain.handle('jarvis:open-external', async (_event, url) => {
    await shell.openExternal(String(url));
  });
}

function createWindow() {
  let retryCount = 0;
  const maxRetry = 4;

  mainWindow = new BrowserWindow({
    width: 1500,
    height: 950,
    autoHideMenuBar: true,
    backgroundColor: '#080808',
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
      preload: path.join(__dirname, 'preload.cjs'),
    },
  });

  mainWindow.loadURL(frontendBaseUrl());
  if (!app.isPackaged) {
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  }

  mainWindow.webContents.on('did-fail-load', (_event, code, description, validatedURL) => {
    logError('WINDOW', `did-fail-load code=${code} url=${validatedURL} reason=${description}`);
    if (retryCount >= maxRetry || !mainWindow || mainWindow.isDestroyed()) {
      return;
    }
    retryCount += 1;
    setTimeout(() => {
      if (mainWindow && !mainWindow.isDestroyed()) {
        mainWindow.loadURL(frontendBaseUrl());
      }
    }, 1000 * retryCount);
  });

  mainWindow.webContents.on('did-finish-load', () => {
    retryCount = 0;
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

async function boot() {
  registerIpcHandlers();
  await startRuntime();
  createWindow();
  startRuntimeWatchdog();
}

app.whenReady().then(async () => {
  try {
    await boot();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logError('BOOT', message);
    dialog.showErrorBox(
      'JARVIS Desktop failed to start',
      `${message}\n\nCheck Python runtime, backend dependencies, and build output.`
    );
    app.quit();
  }
});

app.on('second-instance', () => {
  if (mainWindow) {
    if (mainWindow.isMinimized()) {
      mainWindow.restore();
    }
    mainWindow.focus();
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    void (async () => {
      try {
        await startRuntime();
        createWindow();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logError('ACTIVATE', message);
        dialog.showErrorBox('JARVIS Desktop failed to reactivate', message);
      }
    })();
  }
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('before-quit', () => {
  appIsShuttingDown = true;
  stopRuntimeWatchdog();
  stopAllRuntimeProcesses();
});

if (process.stdout && typeof process.stdout.on === 'function') {
  process.stdout.on('error', (error) => {
    if (!isBrokenPipeError(error)) {
      appendFallbackLog(`[STDOUT] ${String(error)}`);
    }
  });
}

if (process.stderr && typeof process.stderr.on === 'function') {
  process.stderr.on('error', (error) => {
    if (!isBrokenPipeError(error)) {
      appendFallbackLog(`[STDERR] ${String(error)}`);
    }
  });
}

process.on('uncaughtException', (error) => {
  if (isBrokenPipeError(error)) {
    appendFallbackLog('[MAIN] Ignored broken pipe uncaught exception.');
    return;
  }
  appendFallbackLog(`[MAIN] uncaughtException: ${error?.stack || String(error)}`);
});

process.on('unhandledRejection', (reason) => {
  appendFallbackLog(`[MAIN] unhandledRejection: ${String(reason)}`);
});
