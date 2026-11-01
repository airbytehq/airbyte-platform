import { renderHook } from "@testing-library/react";
import dayjs from "dayjs";

import { TestWrapper } from "test-utils";

import { moveTimeToFutureByPeriod, useFormatLengthOfTime, wholeDaysUntil } from "./time";

describe("moveTimeToFutureByPeriod", () => {
  it("does nothing if the time is already in the future", () => {
    let futureTime = dayjs().add(1, "minute");
    expect(moveTimeToFutureByPeriod(futureTime, 1, "hour")).toEqual(futureTime);

    futureTime = dayjs().add(3, "year");
    expect(moveTimeToFutureByPeriod(futureTime, 1, "hour")).toEqual(futureTime);

    futureTime = dayjs().add(30, "year");
    expect(moveTimeToFutureByPeriod(futureTime, 1, "hour")).toEqual(futureTime);
  });

  it("advances by enough hours to go into the future up to one hour from now", () => {
    const anHoursWorthOfMilliseconds = dayjs.duration(1, "hour").asMilliseconds();
    for (let i = 1; i <= 180; i += 5) {
      // 1 to 180 minutes in 5-minute steps

      const timeInPast = dayjs().subtract(i, "minutes");
      const timeInFuture = moveTimeToFutureByPeriod(timeInPast, 1, "hour");

      const distanceFromNowToFuture = timeInFuture.diff(dayjs(), "millisecond");
      expect(distanceFromNowToFuture).toBeGreaterThanOrEqual(0);
      expect(distanceFromNowToFuture).toBeLessThan(anHoursWorthOfMilliseconds);
    }
  });

  describe("with mocked values for easier test writing", () => {
    const mockedDateNow = jest.fn(() => dayjs("2023-03-16T20:53:13Z").valueOf());
    const originalDateNow = Date.now;
    beforeAll(() => {
      Date.now = mockedDateNow;
    });
    afterAll(() => {
      Date.now = originalDateNow;
    });

    it("brings forward in seconds", () => {
      {
        const now = Date.now();
        const past = dayjs(now).subtract(2, "second");
        const future = moveTimeToFutureByPeriod(past, 4, "second");
        expect(future.diff(now, "second")).toBe(2);
      }

      {
        const now = Date.now();
        const past = dayjs(now).subtract(32 * 4 + 1, "second");
        const future = moveTimeToFutureByPeriod(past, 4, "second");
        expect(future.diff(now, "second")).toBe(3);
      }
    });

    it("brings forward in years", () => {
      {
        const now = Date.now();
        const past = dayjs(now).subtract(2, "year");
        const future = moveTimeToFutureByPeriod(past, 4, "year");
        expect(wholeDaysUntil(future, now)).toBe(730); // 2 years = 730 days
      }

      {
        const now = Date.now();
        const past = dayjs(now).subtract(6 * 4 + 1, "year");
        const future = moveTimeToFutureByPeriod(past, 4, "year");
        expect(wholeDaysUntil(future, now)).toBe(365 * 3 - 6); // 6*4=24 years worth, including 6 leaps days
      }
    });
  });
});

describe("useFormatLengthOfTime", () => {
  it("formats time with hours, minutes, and seconds", () => {
    expect(
      renderHook(() => useFormatLengthOfTime((2 * 60 * 60 + 38 * 60 + 39) * 1000), { wrapper: TestWrapper }).result
        .current
    ).toBe("2h 38m 39s");

    expect(
      renderHook(() => useFormatLengthOfTime((1 * 60 * 60 + 15 * 60 + 30) * 1000), { wrapper: TestWrapper }).result
        .current
    ).toBe("1h 15m 30s");

    expect(
      renderHook(() => useFormatLengthOfTime((0 * 60 * 60 + 45 * 60 + 10) * 1000), { wrapper: TestWrapper }).result
        .current
    ).toBe("45m 10s");

    expect(
      renderHook(() => useFormatLengthOfTime((0 * 60 * 60 + 0 * 60 + 59) * 1000), { wrapper: TestWrapper }).result
        .current
    ).toBe("59s");
  });

  it("formats time with minutes and seconds", () => {
    expect(
      renderHook(() => useFormatLengthOfTime((38 * 60 + 39) * 1000), { wrapper: TestWrapper }).result.current
    ).toBe("38m 39s");

    expect(
      renderHook(() => useFormatLengthOfTime((15 * 60 + 30) * 1000), { wrapper: TestWrapper }).result.current
    ).toBe("15m 30s");

    expect(
      renderHook(() => useFormatLengthOfTime((45 * 60 + 10) * 1000), { wrapper: TestWrapper }).result.current
    ).toBe("45m 10s");

    expect(renderHook(() => useFormatLengthOfTime((0 * 60 + 59) * 1000), { wrapper: TestWrapper }).result.current).toBe(
      "59s"
    );
  });

  it("formats time with seconds", () => {
    expect(renderHook(() => useFormatLengthOfTime(39 * 1000), { wrapper: TestWrapper }).result.current).toBe("39s");

    expect(renderHook(() => useFormatLengthOfTime(30 * 1000), { wrapper: TestWrapper }).result.current).toBe("30s");

    expect(renderHook(() => useFormatLengthOfTime(10 * 1000), { wrapper: TestWrapper }).result.current).toBe("10s");

    expect(renderHook(() => useFormatLengthOfTime(1 * 1000), { wrapper: TestWrapper }).result.current).toBe("1s");
  });
});

describe("wholeDaysUntil", () => {
  it("floors whole elapsed days and never goes negative", () => {
    const from = "2026-06-15T12:00:00Z";
    expect(wholeDaysUntil("2026-06-16T13:00:00Z", from)).toBe(1);
    expect(wholeDaysUntil("2026-06-16T11:59:00Z", from)).toBe(0);
    expect(wholeDaysUntil("2026-06-14T12:00:00Z", from)).toBe(0);
  });

  it("is not shifted by a daylight saving change (tests run in US/Pacific)", () => {
    // 24.5 hours across the fall-back on 2026-11-01: a calendar-day diff gives 0.
    expect(wholeDaysUntil("2026-11-01T19:43:00Z", "2026-10-31T19:13:00Z")).toBe(1);
    // 47.5 hours across the spring-forward on 2026-03-08: a calendar-day diff gives 2.
    expect(wholeDaysUntil("2026-03-09T18:43:00Z", "2026-03-07T19:13:00Z")).toBe(1);
  });
});
