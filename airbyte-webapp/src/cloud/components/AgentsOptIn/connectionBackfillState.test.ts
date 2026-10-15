import dayjs from "dayjs";

import {
  FusionSearchStatusBackfill,
  FusionSearchStatusBackfillStatus,
  FusionSearchStatusResponse,
} from "core/api/types/AirbyteClient";

import {
  getBackfillRequestState,
  getConnectionBackfillState,
  getDefaultBackfillStartTime,
  getRowBackfillState,
  hasRunningBackfill,
  isValidBackfillStartTime,
} from "./connectionBackfillState";

const backfill = (
  status: FusionSearchStatusBackfillStatus,
  indexedRecords = 0,
  expectedRecords = 0
): FusionSearchStatusBackfill => ({
  status,
  indexed_records: indexedRecords,
  expected_total_records: expectedRecords,
  collected_records: 0,
  backfill_start_time: null,
  started_at: null,
  completed_at: null,
});

const response = (
  connections: Record<string, Array<FusionSearchStatusBackfill | null>>
): FusionSearchStatusResponse => ({
  sources: Object.entries(connections).map(([connectionId, backfills]) => ({
    source_id: "source-1",
    destination_id: `destination-${connectionId}`,
    connection_id: connectionId,
    streams: backfills.map((streamBackfill, index) => ({
      name: `stream-${index}`,
      backfill: streamBackfill,
      indexes: [],
    })),
  })),
});

describe("getConnectionBackfillState", () => {
  it.each([
    ["every stream has no backfill", [null, null], { status: "off" }],
    ["the connection has no streams", [], { status: "off" }],
    [
      "running while a stream is active next to a failed one",
      [backfill("complete"), backfill("running", 5, 10), backfill("failed")],
      { status: "running", percent: 50 },
    ],
    ["failed once every other stream has finished", [backfill("complete"), backfill("failed")], { status: "failed" }],
    ["a stream is starting", [backfill("starting"), backfill("complete")], { status: "running" }],
    ["a stream is finalizing", [backfill("finalizing", 10, 10), null], { status: "running", percent: 100 }],
    [
      "every stream with a backfill is complete",
      [backfill("complete"), null, backfill("complete")],
      { status: "complete" },
    ],
    ["the only backfill was cancelled", [backfill("cancelled"), null], { status: "off" }],
    [
      "a cancelled backfill sits next to a complete one",
      [backfill("cancelled"), backfill("complete")],
      { status: "off" },
    ],
  ])("returns %s", (_name, backfills, expected) => {
    expect(getConnectionBackfillState(response({ "connection-1": backfills }), "connection-1")).toEqual(expected);
  });

  it("returns off when the response does not list the connection", () => {
    expect(getConnectionBackfillState(response({ "connection-2": [backfill("running")] }), "connection-1")).toEqual({
      status: "off",
    });
  });

  it("ignores the streams of other connections of the same source", () => {
    const status = response({ "connection-1": [backfill("complete")], "connection-2": [backfill("failed")] });

    expect(getConnectionBackfillState(status, "connection-1")).toEqual({ status: "complete" });
  });

  it("sums the progress of the active streams only", () => {
    const status = response({
      "connection-1": [backfill("running", 30, 100), backfill("starting", 0, 100), backfill("complete", 500, 500)],
    });

    expect(getConnectionBackfillState(status, "connection-1")).toEqual({ status: "running", percent: 15 });
  });

  it("hides the percent while the expected total is zero", () => {
    const status = response({ "connection-1": [backfill("running", 0, 0), backfill("starting", 7, 0)] });

    expect(getConnectionBackfillState(status, "connection-1")).toEqual({ status: "running", percent: undefined });
  });

  it("rounds the percent down and caps it at 100", () => {
    expect(
      getConnectionBackfillState(response({ "connection-1": [backfill("running", 999, 1000)] }), "connection-1")
    ).toEqual({ status: "running", percent: 99 });
    expect(
      getConnectionBackfillState(response({ "connection-1": [backfill("running", 150, 100)] }), "connection-1")
    ).toEqual({ status: "running", percent: 100 });
  });
});

describe("hasRunningBackfill", () => {
  const status = response({ "connection-1": [backfill("running")], "connection-2": [backfill("complete")] });

  it.each([
    ["a listed connection is running", ["connection-1", "connection-2"], true],
    ["no listed connection is running", ["connection-2"], false],
    ["no connection is listed", [], false],
  ])("returns %s", (_name, connectionIds, expected) => {
    expect(hasRunningBackfill(status, connectionIds)).toBe(expected);
  });

  it("returns false before the first read", () => {
    expect(hasRunningBackfill(undefined, ["connection-1"])).toBe(false);
  });

  it("returns true while a stream runs next to a failed one", () => {
    expect(
      hasRunningBackfill(response({ "connection-1": [backfill("running"), backfill("failed")] }), ["connection-1"])
    ).toBe(true);
  });
});

describe("getBackfillRequestState", () => {
  const outcome = (value: "requested" | "running" | "failed") =>
    ({ stream_name: "stream", outcome: value, backfill: {} }) as never;

  it.each([
    ["requested", [outcome("requested")], { status: "running" }],
    ["running", [outcome("running"), outcome("requested")], { status: "running" }],
    ["one failed stream next to a running one", [outcome("running"), outcome("failed")], { status: "running" }],
    ["every stream failed", [outcome("failed"), outcome("failed")], { status: "failed" }],
    ["no streams", [], undefined],
  ])("returns the state for %s", (_name, streams, expected) => {
    expect(getBackfillRequestState({ streams })).toEqual(expected);
  });
});

describe("getDefaultBackfillStartTime", () => {
  it.each([
    ["midnight UTC one year earlier", "2026-10-14T15:30:00Z", "2025-10-14T00:00:00Z"],
    ["the UTC date, not the local date", "2026-10-14T23:30:00-05:00", "2025-10-15T00:00:00Z"],
    ["February 28 for a leap day", "2028-02-29T12:00:00Z", "2027-02-28T00:00:00Z"],
  ])("returns %s", (_name, now, expected) => {
    expect(getDefaultBackfillStartTime(dayjs(now))).toBe(expected);
  });
});

describe("isValidBackfillStartTime", () => {
  const now = dayjs("2026-10-15T12:00:00Z");

  it.each([
    ["accepts an empty value, which sends the default", "", true],
    ["accepts a past UTC time", "2025-10-15T00:00:00Z", true],
    ["accepts the current time", "2026-10-15T12:00:00Z", true],
    ["rejects a future time", "2026-10-15T12:00:01Z", false],
    ["rejects a date without a time", "2025-10-01", false],
    ["rejects a time without a time zone", "2025-10-01T00:00:00", false],
    ["rejects a time with an offset", "2025-10-01T00:00:00+02:00", false],
    ["rejects an impossible date", "2025-02-30T00:00:00Z", false],
    ["rejects free text", "foo", false],
  ])("%s", (_name, startTime, expected) => {
    expect(isValidBackfillStartTime(startTime, now)).toBe(expected);
  });
});

describe("getRowBackfillState", () => {
  const completeStatus = response({ "connection-1": [backfill("complete")] });
  const runningRequest = { state: { status: "running" as const }, statusVersionAtRequest: 1 };

  it("returns the request state until a newer status is read", () => {
    expect(
      getRowBackfillState("connection-1", { data: completeStatus, isError: false, dataUpdatedAt: 1 }, runningRequest)
    ).toEqual({ status: "running" });
  });

  it("keeps the request state when a later read fails", () => {
    expect(
      getRowBackfillState("connection-1", { data: completeStatus, isError: true, dataUpdatedAt: 1 }, runningRequest)
    ).toEqual({ status: "running" });
  });

  it("ignores the request state once a newer read arrives", () => {
    expect(
      getRowBackfillState("connection-1", { data: completeStatus, isError: false, dataUpdatedAt: 2 }, runningRequest)
    ).toEqual({ status: "complete" });
  });

  it("returns the state of the last read without a request", () => {
    expect(
      getRowBackfillState("connection-1", { data: completeStatus, isError: false, dataUpdatedAt: 1 }, undefined)
    ).toEqual({ status: "complete" });
  });

  it("keeps the last data when a poll fails", () => {
    expect(
      getRowBackfillState("connection-1", { data: completeStatus, isError: true, dataUpdatedAt: 1 }, undefined)
    ).toEqual({ status: "complete" });
  });

  it("returns unknown when the first read fails", () => {
    expect(
      getRowBackfillState("connection-1", { data: undefined, isError: true, dataUpdatedAt: 0 }, undefined)
    ).toEqual({ status: "unknown" });
  });

  it.each([
    ["the first read is in flight", { data: undefined, isError: false, dataUpdatedAt: 0 }],
    ["the source has no read", undefined],
  ])("returns loading when %s", (_name, statusRead) => {
    expect(getRowBackfillState("connection-1", statusRead, undefined)).toEqual({ status: "loading" });
  });
});
