import { AttemptRead, AttemptStats, AttemptStatus, AttemptStreamStats } from "core/request/AirbyteClient";

import { progressBarCalculations } from "./utils";

// used for tests which rely on Date.now(), to account for if the test takes slightly longer to run sometimes
expect.extend({
  toBeWithinTolerance(received, center, tolerance) {
    const floor = center - tolerance;
    const ceiling = center + tolerance;
    const pass = received >= floor && received <= ceiling;
    return {
      message: () => `expected ${received} to be within tolerance of ${tolerance} around ${center}`,
      pass,
    };
  },
});

describe("#progressBarCalculations", () => {
  it("for an attempt with no throughput information", () => {
    const attempt = makeAttempt();
    const { displayProgressBar, totalPercentRecords } = progressBarCalculations(attempt);

    expect(displayProgressBar).toEqual(false);
    expect(totalPercentRecords).toEqual(0);
  });

  it("for an attempt with total stats", () => {
    const totalStats: AttemptStats = { recordsEmitted: 1, estimatedRecords: 100, bytesEmitted: 1, estimatedBytes: 50 };
    const attempt = makeAttempt(totalStats);
    const { displayProgressBar, totalPercentRecords, elapsedTimeMS, timeRemaining } = progressBarCalculations(attempt);

    expect(displayProgressBar).toEqual(true);
    expect(totalPercentRecords).toEqual(0.01);
    expect(elapsedTimeMS).toBeWithinTolerance(10 * 1000, 2);
    expect(timeRemaining).toBeWithinTolerance(990 * 1000, 2);
  });

  it("for an attempt with per-stream stats", () => {
    const totalStats: AttemptStats = { recordsEmitted: 3, estimatedRecords: 300, bytesEmitted: 3, estimatedBytes: 300 };
    const streamStatsA: AttemptStreamStats = {
      streamName: "A",
      stats: { recordsEmitted: 1, estimatedRecords: 100, bytesEmitted: 1, estimatedBytes: 100 },
    };
    const streamStatsB: AttemptStreamStats = {
      streamName: "B",
      stats: { recordsEmitted: 2, estimatedRecords: 100, bytesEmitted: 2, estimatedBytes: 100 },
    };
    const streamStatsC: AttemptStreamStats = {
      streamName: "C",
      stats: {},
    };

    const attempt = makeAttempt(totalStats, [streamStatsA, streamStatsB, streamStatsC]);
    const { displayProgressBar, totalPercentRecords, elapsedTimeMS, timeRemaining } = progressBarCalculations(attempt);

    expect(displayProgressBar).toEqual(true);
    expect(totalPercentRecords).toEqual(0.01);
    expect(elapsedTimeMS).toBeWithinTolerance(10 * 1000, 2);
    expect(timeRemaining).toBeWithinTolerance(990 * 1000, 2);
  });
});

const makeAttempt = (totalStats: AttemptStats = {}, streamStats: AttemptStreamStats[] = []) => {
  const now = Date.now();
  // API returns time in seconds
  const createdAt = now / 1000 - 10;
  const updatedAt = now / 1000;
  const id = 123;
  const status: AttemptStatus = "running";
  const attempt: AttemptRead = { id, status, createdAt, updatedAt, totalStats, streamStats };
  return attempt;
};
