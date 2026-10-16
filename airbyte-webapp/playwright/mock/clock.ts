import type { BrowserContext } from "@playwright/test";

/**
 * Every mocked test starts at this time. The browser clock starts here and then runs at normal speed, so dates and
 * relative times, such as "5 minutes ago", render the same on every run.
 */
export const MOCK_START_TIME = new Date("2026-10-03T12:00:00Z");

/**
 * The browser clock of one test, and the same time on the mock side. The mock uses `nowSeconds()` for the
 * timestamps it writes, such as the start time of a job, so they match the time the browser shows.
 */
export function createMockClock() {
  let context: BrowserContext | undefined;
  let installedAt = Date.now();
  let skippedMs = 0;

  /** Sets the browser clock of every page in the context to MOCK_START_TIME. Time keeps running from there. */
  async function install(browserContext: BrowserContext) {
    context = browserContext;
    installedAt = Date.now();
    await browserContext.clock.install({ time: MOCK_START_TIME });
  }

  /**
   * Moves the browser clock forward, so that a timer of the webapp, such as a poll interval, fires now instead of
   * after a real wait. Each timer that is due fires once.
   */
  async function fastForward(ms: number) {
    if (!context) {
      throw new Error("The mock clock is not installed");
    }
    skippedMs += ms;
    await context.clock.fastForward(ms);
  }

  /** The current browser time in epoch seconds, the unit the Airbyte API uses for timestamps. */
  function nowSeconds() {
    const elapsedMs = Date.now() - installedAt + skippedMs;
    return Math.floor((MOCK_START_TIME.getTime() + elapsedMs) / 1000);
  }

  return { install, fastForward, nowSeconds };
}

export type MockClock = ReturnType<typeof createMockClock>;
