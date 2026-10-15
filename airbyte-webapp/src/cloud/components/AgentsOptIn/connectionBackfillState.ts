import dayjs, { Dayjs } from "dayjs";

import { ISO8601_NO_MILLISECONDS } from "components/ui/DatePicker/utils";

import {
  FusionConnectionBackfillResponse,
  FusionSearchStatusBackfill,
  FusionSearchStatusBackfillStatus,
  FusionSearchStatusResponse,
} from "core/api/types/AirbyteClient";

/**
 * The start time that a backfill uses when the user enters none: midnight UTC one year ago, in the date picker's
 * format. The server would otherwise backfill the full history.
 */
export const getDefaultBackfillStartTime = (now: Dayjs = dayjs()) =>
  now.utc().subtract(1, "year").startOf("day").format(ISO8601_NO_MILLISECONDS);

/**
 * An entered start time is valid when it is empty, which sends the default, or a past UTC time in the date picker's
 * format. The server rejects a time without a time zone and does not reject a future time.
 */
export const isValidBackfillStartTime = (startTime: string, now: Dayjs = dayjs()) => {
  if (!startTime) {
    return true;
  }

  const parsed = dayjs.utc(startTime, ISO8601_NO_MILLISECONDS, true);
  return parsed.isValid() && !parsed.isAfter(now);
};

const ACTIVE_BACKFILL_STATUSES: ReadonlySet<FusionSearchStatusBackfillStatus> = new Set([
  "starting",
  "running",
  "finalizing",
]);

export const BACKFILL_STATUS_POLL_INTERVAL_MS = 10_000;

export type ConnectionBackfillState =
  | { status: "off" }
  // percent is undefined until the backfiller has counted the expected records.
  | { status: "running"; percent?: number }
  | { status: "complete" }
  | { status: "failed" };

// Complete streams count toward the percent so that it does not drop when a stream finishes. A failed or cancelled
// stream never indexes its expected records, so it is left out.
const backfillPercent = (backfills: FusionSearchStatusBackfill[]) => {
  const countedBackfills = backfills.filter(
    (backfill) => ACTIVE_BACKFILL_STATUSES.has(backfill.status) || backfill.status === "complete"
  );

  const expectedRecords = countedBackfills.reduce((total, backfill) => total + backfill.expected_total_records, 0);
  if (expectedRecords <= 0) {
    return undefined;
  }

  const indexedRecords = countedBackfills.reduce((total, backfill) => total + backfill.indexed_records, 0);
  return Math.min(100, Math.floor((indexedRecords * 100) / expectedRecords));
};

/**
 * Combines the backfill status of every stream of one connection into one state. Any active stream wins, so the
 * controls stay locked until every stream has finished. Then a failure anywhere wins. The connection is complete only
 * when every stream with a backfill is complete. Anything else, including a cancelled backfill or a connection that
 * the response does not list, is off.
 */
export const getConnectionBackfillState = (
  response: FusionSearchStatusResponse,
  connectionId: string
): ConnectionBackfillState => {
  const backfills = response.sources
    .filter((source) => source.connection_id === connectionId)
    .flatMap((source) => source.streams)
    .flatMap((stream) => (stream.backfill ? [stream.backfill] : []));
  const statuses = backfills.map((backfill) => backfill.status);

  const activeBackfills = backfills.filter((backfill) => ACTIVE_BACKFILL_STATUSES.has(backfill.status));
  if (activeBackfills.length > 0) {
    return { status: "running", percent: backfillPercent(backfills) };
  }

  if (statuses.includes("failed")) {
    return { status: "failed" };
  }

  if (statuses.length > 0 && statuses.every((status) => status === "complete")) {
    return { status: "complete" };
  }

  return { status: "off" };
};

export const hasRunningBackfill = (response: FusionSearchStatusResponse | undefined, connectionIds: string[]) =>
  !!response &&
  connectionIds.some((connectionId) => getConnectionBackfillState(response, connectionId).status === "running");

/**
 * The state that a start request implies until the status is read again. A requested or running stream makes the
 * connection running, as in the status read. Only a request in which every stream failed makes it failed.
 */
export const getBackfillRequestState = (
  response: FusionConnectionBackfillResponse
): ConnectionBackfillState | undefined => {
  if (response.streams.some((stream) => stream.outcome !== "failed")) {
    return { status: "running" };
  }
  if (response.streams.length > 0) {
    return { status: "failed" };
  }
  return undefined;
};

// A start request's state applies until the source status is read again after the request finished.
export interface BackfillRequest {
  state: ConnectionBackfillState;
  statusVersionAtRequest: number;
}

type RowBackfillState = ConnectionBackfillState | { status: "loading" } | { status: "unknown" };

export interface SourceStatusRead {
  data?: FusionSearchStatusResponse;
  isError: boolean;
  dataUpdatedAt: number;
}

/**
 * The state that one row shows. The start request's state wins until a newer status is read. Then the last status
 * that was read wins, also when the latest read failed. A failed first read is unknown.
 */
export const getRowBackfillState = (
  connectionId: string,
  statusRead: SourceStatusRead | undefined,
  request: BackfillRequest | undefined
): RowBackfillState => {
  if (request && request.statusVersionAtRequest === (statusRead?.dataUpdatedAt ?? 0)) {
    return request.state;
  }
  if (statusRead?.data) {
    return getConnectionBackfillState(statusRead.data, connectionId);
  }
  if (statusRead?.isError) {
    return { status: "unknown" };
  }
  return { status: "loading" };
};
