const globalAny = globalThis as typeof globalThis & {
  ResizeObserver?: typeof ResizeObserver;
};

if (typeof globalAny.ResizeObserver === 'undefined') {
  class ResizeObserverMock {
    observe(): void {}
    unobserve(): void {}
    disconnect(): void {}
  }

  globalAny.ResizeObserver = ResizeObserverMock as unknown as typeof ResizeObserver;
}
