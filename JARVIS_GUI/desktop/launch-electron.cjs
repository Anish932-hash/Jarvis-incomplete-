const { spawn } = require('child_process');
const path = require('path');

const electronBinary = require('electron');
const mainFile = path.join(__dirname, 'main.cjs');

const env = { ...process.env };
delete env.ELECTRON_RUN_AS_NODE;

const child = spawn(electronBinary, [mainFile], {
  env,
  stdio: 'inherit',
  windowsHide: false,
});

child.on('exit', (code) => {
  process.exit(code ?? 0);
});
