import { humanizeCron } from "./cron";

describe(`${humanizeCron.name}`, () => {
  it("should return a human readable string", () => {
    const cronExpression = "0 0 0 * * *";
    const humanReadable = humanizeCron(cronExpression);
    expect(humanReadable).toBe("At 12:00 AM");
  });

  it("should not use 0-index days of week", () => {
    const cronExpression = "0 0 0 ? * 1";
    const humanReadable = humanizeCron(cronExpression);
    expect(humanReadable).toBe("At 12:00 AM, only on Sunday");
  });
});
