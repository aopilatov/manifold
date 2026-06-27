// Реконнект-backoff с полным джиттером (раздел 7.11): размазывает синхронные реконнекты.

export interface BackoffOptions {
  base?: number; // мс, стартовая задержка
  max?: number; // мс, потолок
}

export function jitteredDelay(attempt: number, opts: BackoffOptions = {}): number {
  const base = opts.base ?? 500;
  const max = opts.max ?? 20_000;
  const exp = Math.min(max, base * 2 ** attempt);
  return Math.random() * exp; // full jitter
}
