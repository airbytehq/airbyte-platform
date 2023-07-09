import dayjs from "dayjs";

import { moveTimeToFutureByPeriod } from "./time";

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
        expect(future.diff(now, "day")).toBe(730); // 2 years = 730 days, dayjs's diff does _things_ that makes 370 days return 1 year
      }

      {
        const now = Date.now();
        const past = dayjs(now).subtract(6 * 4 + 1, "year");
        const future = moveTimeToFutureByPeriod(past, 4, "year");
        expect(future.diff(now, "days")).toBe(365 * 3 - 6); // 6*4=24 years worth, including 6 leaps days
      }
    });
  });
});
