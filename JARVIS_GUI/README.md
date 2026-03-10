# JARVIS GUI

This project now supports both:

1. Web UI (`next dev`)
2. Windows desktop runtime (Electron + local Python backend)

The desktop Action Panel includes:

- Direct tool execution + approval workflow
- Schedule creation/cancellation and schedule status tracking
- Runtime memory search/inspection

## Development

```powershell
npm run dev
```

## Desktop Development

The desktop runtime launches:

1. `JARVIS_BACKEND` local desktop API (`backend.python.desktop_api`)
2. This GUI in Electron

```powershell
npm run desktop:dev
```

## Desktop Windows Build

```powershell
npm run desktop:build
```

Build output:

- `dist/JARVIS-Desktop-win32-x64/` (portable packaged desktop app)

Notes:

- Python must be available (`python`) or via `.venv\Scripts\python.exe`.
- The desktop build bundles GUI + backend code; runtime dependencies (Python packages/models) must exist on target machine.
