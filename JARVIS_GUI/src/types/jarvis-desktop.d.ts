export interface JarvisDesktopBridge {
  request<T = unknown>(
    path: string,
    method?: 'GET' | 'POST',
    payload?: Record<string, unknown>
  ): Promise<T>;
  getAppInfo(): Promise<Record<string, unknown>>;
  openExternal(url: string): Promise<void>;
}

declare global {
  interface Window {
    jarvisDesktop?: JarvisDesktopBridge;
  }
}

export {};
