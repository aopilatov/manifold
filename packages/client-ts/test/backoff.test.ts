import { test } from "node:test";
import assert from "node:assert";
import { jitteredDelay } from "../src/backoff.ts";

test("jittered delay в пределах [0, cap]", () => {
  for (let attempt = 0; attempt < 12; attempt++) {
    const d = jitteredDelay(attempt, { base: 500, max: 20_000 });
    assert.ok(d >= 0, "не отрицательный");
    assert.ok(d <= 20_000, "не больше потолка");
  }
});

test("экспоненциальный рост потолка до cap", () => {
  // при больших attempt верхняя граница = max; среднее из выборки близко к max/2
  const samples = Array.from({ length: 200 }, () => jitteredDelay(10, { base: 500, max: 8_000 }));
  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  assert.ok(avg > 2_000 && avg < 6_000, `avg=${avg} должен быть около 4000`);
});
