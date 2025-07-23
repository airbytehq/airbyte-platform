import { isMoreFrequentThanHourlyFromExecutions } from "./cronFrequency";

describe("cronFrequency", () => {
  describe("isMoreFrequentThanHourlyFromExecutions", () => {
    it("should return false for invalid input (empty, single, null, undefined)", () => {
      expect(isMoreFrequentThanHourlyFromExecutions([])).toBe(false);
      expect(isMoreFrequentThanHourlyFromExecutions([1000000])).toBe(false);
      expect(isMoreFrequentThanHourlyFromExecutions(undefined as unknown as number[])).toBe(false);
      expect(isMoreFrequentThanHourlyFromExecutions(null as unknown as number[])).toBe(false);
    });

    it("should return true for sub-hourly executions", () => {
      const now = Date.now();
      const thirtyMinutesMs = 30 * 60 * 1000;
      const executions = [now, now + thirtyMinutesMs];

      expect(isMoreFrequentThanHourlyFromExecutions(executions)).toBe(true);
    });

    it("should return false for hourly executions", () => {
      const now = Date.now();
      const oneHourMs = 60 * 60 * 1000;
      const executions = [now, now + oneHourMs];

      expect(isMoreFrequentThanHourlyFromExecutions(executions)).toBe(false);
    });

    it("should return false for long interval executions", () => {
      const now = Date.now();
      const oneDayMs = 24 * 60 * 60 * 1000;
      const executions = [now, now + oneDayMs];

      expect(isMoreFrequentThanHourlyFromExecutions(executions)).toBe(false);
    });

    it("should use first two executions for frequency calculation", () => {
      const now = Date.now();
      const thirtyMinutesMs = 30 * 60 * 1000;
      const oneHourMs = 60 * 60 * 1000;

      // First interval is 30 minutes - should be true based on first interval
      const executions = [now, now + thirtyMinutesMs, now + thirtyMinutesMs + oneHourMs];

      expect(isMoreFrequentThanHourlyFromExecutions(executions)).toBe(true);
    });

    it("should handle seconds format conversion", () => {
      // Backend may return seconds instead of milliseconds
      const now = Math.floor(Date.now() / 1000);
      const oneHourInSeconds = 60 * 60;
      const executions = [now, now + oneHourInSeconds];

      expect(isMoreFrequentThanHourlyFromExecutions(executions)).toBe(false);
    });

    it("should handle invalid intervals gracefully", () => {
      const now = Date.now();

      // Negative interval (second execution before first)
      expect(isMoreFrequentThanHourlyFromExecutions([now + 1000, now])).toBe(false);

      // Zero interval (same timestamp)
      expect(isMoreFrequentThanHourlyFromExecutions([now, now])).toBe(false);
    });
  });
});
