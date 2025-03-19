import { getStartOfFirstWindow } from "./getStartOfFirstWindow";
import { LookbackWindow } from "./lookbackConfiguration";

// Some of the expected values in the tests below depend on the timezone, which should be fixed to US/Pacific for
// frontend unit tests
describe("Timezones", () => {
  it("should always be US/Pacific", () => {
    expect(process.env.TZ).toEqual("US/Pacific");
  });
});

describe(`${getStartOfFirstWindow.name}`, () => {
  const cases: Array<{ lookback: LookbackWindow; currentTime: string; expected: string }> = [
    { lookback: "6h", currentTime: "2025-02-19T12:59:59Z", expected: "2025-02-19T06:45:00Z" },
    { lookback: "6h", currentTime: "2025-02-19T13:00:00Z", expected: "2025-02-19T07:00:00Z" },
    { lookback: "6h", currentTime: "2025-02-19T13:03:00Z", expected: "2025-02-19T07:00:00Z" },
    { lookback: "6h", currentTime: "2025-02-19T13:14:59Z", expected: "2025-02-19T07:00:00Z" },
    { lookback: "6h", currentTime: "2025-02-19T13:16:00Z", expected: "2025-02-19T07:15:00Z" },
    { lookback: "24h", currentTime: "2025-02-19T12:59:59Z", expected: "2025-02-18T12:00:00Z" },
    { lookback: "24h", currentTime: "2025-02-19T13:00:00Z", expected: "2025-02-18T13:00:00Z" },
    { lookback: "24h", currentTime: "2025-02-19T13:03:00Z", expected: "2025-02-18T13:00:00Z" },
    { lookback: "7d", currentTime: "2025-02-19T12:59:59Z", expected: "2025-02-12T08:00:00Z" },
    { lookback: "7d", currentTime: "2025-02-19T13:00:00Z", expected: "2025-02-12T08:00:00Z" },
    { lookback: "7d", currentTime: "2025-02-19T13:03:00Z", expected: "2025-02-12T08:00:00Z" },
  ];

  it.each(cases)(
    `returns the start of a $lookback lookback when the current time is $currentTime`,
    ({ lookback, currentTime, expected }) => {
      jest.useFakeTimers().setSystemTime(new Date(currentTime));
      const result = getStartOfFirstWindow(lookback);
      expect(result.utc().format()).toEqual(expected);
      jest.useRealTimers();
    }
  );
});
