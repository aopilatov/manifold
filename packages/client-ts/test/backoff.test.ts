import { test } from "node:test";
import assert from "node:assert";
import { jitteredDelay } from "../src/backoff.ts";

test("jittered delay within [0, cap]", () => {
  for (let attempt = 0; attempt < 12; attempt++) {
    const d = jitteredDelay(attempt, { base: 500, max: 20_000 });
    assert.ok(d >= 0, "not negative");
    assert.ok(d <= 20_000, "not above the cap");
  }
});

test("exponential growth of the cap up to max", () => {
  // for large attempt the upper bound = max; the sample mean is close to max/2
  const samples = Array.from({ length: 200 }, () => jitteredDelay(10, { base: 500, max: 8_000 }));
  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  assert.ok(avg > 2_000 && avg < 6_000, `avg=${avg} should be around 4000`);
});
