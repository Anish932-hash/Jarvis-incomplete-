const fs = require('fs');
const path = require('path');

const projectRoot = path.resolve(__dirname, '..');
const backendRoot = path.resolve(projectRoot, '..', 'JARVIS_BACKEND');
const electronDist = path.join(projectRoot, 'node_modules', 'electron', 'dist');

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function removeDir(dirPath) {
  if (fs.existsSync(dirPath)) {
    fs.rmSync(dirPath, { recursive: true, force: true });
  }
}

function copyFile(source, target) {
  ensureDir(path.dirname(target));
  fs.copyFileSync(source, target);
}

function copyDirectory(source, target) {
  if (!fs.existsSync(source)) return;
  ensureDir(target);
  const entries = fs.readdirSync(source, { withFileTypes: true });
  for (const entry of entries) {
    const from = path.join(source, entry.name);
    const to = path.join(target, entry.name);
    if (entry.isDirectory()) {
      copyDirectory(from, to);
    } else if (entry.isFile()) {
      copyFile(from, to);
    }
  }
}

function writeJson(target, value) {
  ensureDir(path.dirname(target));
  fs.writeFileSync(target, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function copyIfExists(source, target) {
  if (!fs.existsSync(source)) return;
  const stat = fs.statSync(source);
  if (stat.isDirectory()) {
    copyDirectory(source, target);
  } else if (stat.isFile()) {
    copyFile(source, target);
  }
}

async function main() {
  if (!fs.existsSync(electronDist)) {
    throw new Error(`Electron runtime not found at ${electronDist}. Run npm install first.`);
  }

  const outDir = path.join(projectRoot, 'dist');
  ensureDir(outDir);

  const appPath = path.join(outDir, 'JARVIS-Desktop-win32-x64');
  removeDir(appPath);

  // Base Electron runtime.
  copyDirectory(electronDist, appPath);

  // Rename executable for user-friendly launch.
  const electronExe = path.join(appPath, 'electron.exe');
  const jarvisExe = path.join(appPath, 'JARVIS Desktop.exe');
  if (fs.existsSync(electronExe)) {
    try {
      fs.renameSync(electronExe, jarvisExe);
    } catch (_) {
      // Keep electron.exe if rename fails.
    }
  }

  const resourcesDir = path.join(appPath, 'resources');
  const appDir = path.join(resourcesDir, 'app');
  const backendTarget = path.join(resourcesDir, 'JARVIS_BACKEND');

  removeDir(appDir);
  ensureDir(appDir);

  // App runtime payload for Electron main process.
  copyDirectory(path.join(projectRoot, 'desktop'), path.join(appDir, 'desktop'));
  copyDirectory(path.join(projectRoot, 'out'), path.join(appDir, 'out'));

  writeJson(path.join(appDir, 'package.json'), {
    name: 'jarvis-desktop-runtime',
    private: true,
    version: '1.0.0',
    main: 'desktop/main.cjs',
  });

  // Backend payload consumed by desktop main.
  removeDir(backendTarget);
  copyDirectory(path.join(backendRoot, 'backend', 'python'), path.join(backendTarget, 'backend', 'python'));
  copyDirectory(path.join(backendRoot, 'configs'), path.join(backendTarget, 'configs'));
  ensureDir(path.join(backendTarget, 'scripts'));
  copyIfExists(
    path.join(backendRoot, 'scripts', 'start_desktop_api.ps1'),
    path.join(backendTarget, 'scripts', 'start_desktop_api.ps1')
  );
  copyFile(path.join(backendRoot, 'README.md'), path.join(backendTarget, 'README.md'));

  console.log(`[PACKAGER] Desktop app created: ${appPath}`);
}

main().catch((error) => {
  console.error('[PACKAGER] Failed:', error);
  process.exitCode = 1;
});
