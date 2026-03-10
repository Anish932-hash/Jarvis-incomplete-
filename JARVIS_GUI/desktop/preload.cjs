const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('jarvisDesktop', {
  request: (path, method = 'GET', payload = {}) =>
    ipcRenderer.invoke('jarvis:api:request', {
      path,
      method,
      payload,
    }),
  getAppInfo: () => ipcRenderer.invoke('jarvis:app-info'),
  openExternal: (url) => ipcRenderer.invoke('jarvis:open-external', url),
});
