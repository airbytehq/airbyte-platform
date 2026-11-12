import { DataplaneHealthRead } from "core/api/types/AirbyteClient";

import { formatAge, getLastCheckInCardData, getVersionCardData, NEVER_CONNECTED } from "./utils";

const NOW = 100_000;

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: "org-1",
  status: "HEALTHY",
  last_heartbeat_timestamp: NOW - 30_000,
  ...overrides,
});

describe("formatAge", () => {
  it("uses seconds for an age under a minute", () => {
    expect(formatAge(31_000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.seconds",
      values: { seconds: 31 },
    });
  });

  it("uses minutes for an age under an hour", () => {
    expect(formatAge(72_000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.minutes",
      values: { minutes: 1, seconds: 12 },
    });
  });

  it("uses hours for an age under a day", () => {
    expect(formatAge((60 * 60 + 13 * 60 + 12) * 1000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.hours",
      values: { hours: 1, minutes: 13, seconds: 12 },
    });
  });

  it("uses days for an age of a day or more", () => {
    expect(formatAge((26 * 60 * 60 + 5) * 1000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.days",
      values: { days: 1, hours: 2 },
    });
  });

  it("never reports a negative age", () => {
    expect(formatAge(-5000).values).toEqual({ seconds: 0 });
  });
});

describe("getLastCheckInCardData", () => {
  it("is never connected for a data plane without a check-in", () => {
    expect(getLastCheckInCardData(dataplane({ last_heartbeat_timestamp: undefined }), NOW)).toEqual(NEVER_CONNECTED);
  });

  it("is a success for a healthy data plane", () => {
    expect(getLastCheckInCardData(dataplane({ status: "HEALTHY" }), NOW).level).toBe("success");
  });

  it("is a warning for a degraded data plane", () => {
    expect(getLastCheckInCardData(dataplane({ status: "DEGRADED" }), NOW).level).toBe("warning");
  });

  it("is an error for an unhealthy data plane", () => {
    expect(getLastCheckInCardData(dataplane({ status: "UNHEALTHY" }), NOW).level).toBe("error");
  });

  it("is neutral for a connected data plane whose status is unknown", () => {
    expect(getLastCheckInCardData(dataplane({ status: "UNKNOWN" }), NOW).level).toBe("neutral");
  });

  it("shows the age of the last check-in", () => {
    expect(getLastCheckInCardData(dataplane({}), NOW).value).toEqual(formatAge(30_000));
  });
});

describe("getVersionCardData", () => {
  it("is never connected for a data plane without a check-in", () => {
    expect(getVersionCardData(dataplane({ last_heartbeat_timestamp: undefined, dataplane_version: "2.4.0" }))).toEqual(
      NEVER_CONNECTED
    );
  });

  it("is a success that shows the version", () => {
    expect(getVersionCardData(dataplane({ dataplane_version: "2.4.0" }))).toEqual({
      level: "success",
      value: { text: "2.4.0" },
    });
  });

  it.each([
    ["the server stored unknown", "unknown"],
    ["the version is missing", undefined],
    ["the version is empty", ""],
  ])("is neutral and unavailable when %s", (_description, version) => {
    expect(getVersionCardData(dataplane({ dataplane_version: version }))).toEqual({
      level: "neutral",
      value: { messageId: "settings.organization.regions.dataplane.version.unavailable" },
    });
  });
});
