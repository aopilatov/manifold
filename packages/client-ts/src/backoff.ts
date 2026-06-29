// Reconnect backoff with full jitter (section 7.11): spreads out synchronized reconnects.

export interface BackoffOptions {
  base?: number; // ms, initial delay
  max?: number; // ms, cap
}

export function jitteredDelay(attempt: number, opts: BackoffOptions = {}): number {
  const base = opts.base ?? 500;
  const max = opts.max ?? 20_000;
  const exp = Math.min(max, base * 2 ** attempt);
  return Math.random() * exp; // full jitter
}
