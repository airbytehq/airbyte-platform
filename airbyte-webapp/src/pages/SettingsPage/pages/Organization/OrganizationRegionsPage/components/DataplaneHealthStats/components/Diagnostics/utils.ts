import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";
import { assertNever } from "core/utils/asserts";

import { DiagnosticCardData } from "./types";
import { CardData } from "../../types";
import { formatAge, formatDuration, NEVER_CONNECTED } from "../../utils";
import { SecretStorageRow, StoreScope, StoreScopeType } from "../SecretStorageDetails";
import { StatusLevel } from "../StatusCard";
import {
  BlockedWorkloads,
  ScheduledWorkload,
  SchedulingProblem,
  WorkloadSchedulingDetailsData,
} from "../WorkloadSchedulingDetails";

type SecretStorageSummary = "yes" | "no" | "incomplete";

const SECRET_STORAGE_SUMMARY: Record<SecretStorageSummary, { level: StatusLevel; messageId: string }> = {
  yes: { level: "success", messageId: "settings.organization.regions.dataplane.diagnostics.secretStorage.yes" },
  no: { level: "error", messageId: "settings.organization.regions.dataplane.diagnostics.secretStorage.no" },
  incomplete: {
    level: "warning",
    messageId: "settings.organization.regions.dataplane.diagnostics.incomplete",
  },
};

// A data plane with no secret store reports one NOT_CONFIGURED result in place of a store, so it is not a store to show.
const getSecretStorageResults = (dataplane: DataplaneHealthRead): DataplaneDiagnosticRead[] =>
  (dataplane.diagnostics ?? []).filter(
    (diagnostic) =>
      diagnostic.kind === DataplaneDiagnosticKind.SECRET_STORAGE &&
      diagnostic.status !== DataplaneDiagnosticStatus.NOT_CONFIGURED
  );

// Any failure wins, then a check that could not complete.
const summarizeSecretStorage = (results: DataplaneDiagnosticRead[]): SecretStorageSummary => {
  if (results.some((result) => result.status === DataplaneDiagnosticStatus.FAILED)) {
    return "no";
  }
  if (results.some((result) => result.status === DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE)) {
    return "incomplete";
  }
  return "yes";
};

// lastChecked is the age of the oldest result, since the summary is only as fresh as its stalest store.
// A data plane that never connected has no results to summarize, so it is shown as never connected.
// Returns undefined when a connected data plane has reported no secret store result, so no card is shown.
export const getSecretStorageCardData = (
  dataplane: DataplaneHealthRead,
  now: number
): DiagnosticCardData | undefined => {
  if (dataplane.last_heartbeat_timestamp === undefined) {
    return NEVER_CONNECTED;
  }

  const secretStorageList = getSecretStorageResults(dataplane);

  if (secretStorageList.length === 0) {
    return undefined;
  }

  const oldestCheckedAt = Math.min(...secretStorageList.map((result) => result.checked_at));
  const { level, messageId } = SECRET_STORAGE_SUMMARY[summarizeSecretStorage(secretStorageList)];

  return {
    level,
    value: { messageId },
    lastChecked: formatAge(now - oldestCheckedAt),
  };
};

const SCOPE_TYPE_BY_OBSERVATION: Partial<Record<string, StoreScopeType>> = {
  workspace: "workspace",
  organization: "organization",
};

// The observation is free-form, so anything that is not a known scope type with an id is treated as unknown.
export const getStoreScope = (observation: DataplaneDiagnosticRead["observation"]): StoreScope | undefined => {
  const type = observation?.scope_type;
  const id = observation?.scope_id;
  if (typeof type !== "string" || typeof id !== "string") {
    return undefined;
  }

  const scopeType = SCOPE_TYPE_BY_OBSERVATION[type.toLowerCase()];
  return scopeType && { type: scopeType, id };
};

// A missing, empty or malformed name leaves the store to be shown by its id.
export const getStoreName = ({ target_name: name }: DataplaneDiagnosticRead): string | undefined =>
  typeof name === "string" && name !== "" ? name : undefined;

const LEVEL_BY_DIAGNOSTIC_STATUS: Record<DataplaneDiagnosticStatus, StatusLevel> = {
  PASSED: "success",
  FAILED: "error",
  UNABLE_TO_COMPLETE: "warning",
  NOT_CONFIGURED: "neutral",
};

// One row for the latest result of each secret store. A data plane that never connected has none.
export const getSecretStorageRows = (dataplane: DataplaneHealthRead, now: number): SecretStorageRow[] =>
  getSecretStorageResults(dataplane).map((result) => ({
    id: result.target_id,
    name: getStoreName(result),
    level: LEVEL_BY_DIAGNOSTIC_STATUS[result.status],
    scope: getStoreScope(result.observation),
    lastChecked: formatAge(now - result.checked_at),
    error:
      result.error_code !== undefined && result.error_message !== undefined
        ? { code: result.error_code, message: result.error_message }
        : undefined,
  }));

type Observation = DataplaneDiagnosticRead["observation"];

const WORKLOAD_SCHEDULING_MESSAGE_PREFIX = "settings.organization.regions.dataplane.diagnostics.workloadScheduling";

const NONE_BLOCKED: BlockedWorkloads = { kind: "none" };
const NOT_MEASURED: BlockedWorkloads = { kind: "notMeasured" };

// The observation is free-form, so the event fields are checked before they are read, and one that is not what the
// data plane documents is treated as not reported. The blocked fields are the exception: the server rejects a failed
// result without them, so they are read as the server documents them.
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isText = (value: unknown): value is string => typeof value === "string" && value !== "";

const isEpochMillis = (value: unknown): value is number => typeof value === "number" && value > 0;

// This check always measures something, so a not configured result is not expected and is left out.
const getWorkloadSchedulingDiagnostic = (dataplane: DataplaneHealthRead): DataplaneDiagnosticRead | undefined =>
  dataplane.diagnostics?.find(
    (diagnostic) =>
      diagnostic.kind === DataplaneDiagnosticKind.WORKLOAD_SCHEDULING &&
      diagnostic.status !== DataplaneDiagnosticStatus.NOT_CONFIGURED
  );

// The server rejects a failed result without these fields, so they are read as the server documents them.
interface BlockedObservation extends Record<string, unknown> {
  blocked_count: number;
  longest_blocked_since: number;
  longest_blocked_workload_type: string;
}

const readBlockedWorkloads = (observation: Observation, now: number): BlockedWorkloads => {
  const { blocked_count, longest_blocked_since, longest_blocked_workload_type } = observation as BlockedObservation;

  return {
    kind: "blocked",
    count: blocked_count,
    longest: { workloadType: longest_blocked_workload_type, waiting: formatDuration(now - longest_blocked_since) },
  };
};

const getBlockedWorkloads = (diagnostic: DataplaneDiagnosticRead, now: number): BlockedWorkloads => {
  switch (diagnostic.status) {
    case DataplaneDiagnosticStatus.PASSED:
      return NONE_BLOCKED;
    case DataplaneDiagnosticStatus.FAILED:
      return readBlockedWorkloads(diagnostic.observation, now);
    case DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE:
    case DataplaneDiagnosticStatus.NOT_CONFIGURED:
      // A not configured result is left out before this point, so it is only listed to keep the switch exhaustive.
      return NOT_MEASURED;
    default:
      return assertNever(diagnostic.status);
  }
};

const toCardData = (blocked: BlockedWorkloads): CardData => {
  switch (blocked.kind) {
    case "none":
      return { level: "success", value: { messageId: `${WORKLOAD_SCHEDULING_MESSAGE_PREFIX}.noneBlocked` } };
    case "blocked":
      return {
        level: "error",
        value: { messageId: `${WORKLOAD_SCHEDULING_MESSAGE_PREFIX}.blocked`, values: { count: blocked.count } },
      };
    case "notMeasured":
      return {
        level: "warning",
        value: { messageId: "settings.organization.regions.dataplane.diagnostics.incomplete" },
      };
    default:
      return assertNever(blocked);
  }
};

export const getWorkloadSchedulingCardData = (
  dataplane: DataplaneHealthRead,
  now: number
): DiagnosticCardData | undefined => {
  if (dataplane.last_heartbeat_timestamp === undefined) {
    return NEVER_CONNECTED;
  }

  const diagnostic = getWorkloadSchedulingDiagnostic(dataplane);

  if (!diagnostic) {
    return undefined;
  }

  return {
    ...toCardData(getBlockedWorkloads(diagnostic, now)),
    lastChecked: formatAge(now - diagnostic.checked_at),
  };
};

const readLastScheduledWorkload = (observation: Observation, now: number): ScheduledWorkload | undefined => {
  const event = observation?.last_scheduled;

  if (!isRecord(event) || !isEpochMillis(event.scheduled_at) || !isText(event.workload_type)) {
    return undefined;
  }

  return { workloadType: event.workload_type, age: formatAge(now - event.scheduled_at) };
};

const readLastSchedulingProblem = (observation: Observation, now: number): SchedulingProblem | undefined => {
  const event = observation?.last_problem;

  if (!isRecord(event) || !isEpochMillis(event.observed_at) || !isText(event.reason)) {
    return undefined;
  }

  // The message says why the workload could not be scheduled. The launcher sends it empty when the scheduler gave none,
  // so the reason stands in.
  const message = isText(event.message) ? event.message : event.reason;

  return { message, age: formatAge(now - event.observed_at) };
};

// Returns undefined when there is no workload scheduling result, so there are no details to show.
export const getWorkloadSchedulingDetails = (
  dataplane: DataplaneHealthRead,
  now: number
): WorkloadSchedulingDetailsData | undefined => {
  const diagnostic = getWorkloadSchedulingDiagnostic(dataplane);

  if (!diagnostic) {
    return undefined;
  }

  return {
    blocked: getBlockedWorkloads(diagnostic, now),
    lastScheduled: readLastScheduledWorkload(diagnostic.observation, now),
    lastProblem: readLastSchedulingProblem(diagnostic.observation, now),
    error:
      diagnostic.error_code !== undefined && diagnostic.error_message !== undefined
        ? { code: diagnostic.error_code, message: diagnostic.error_message }
        : undefined,
  };
};
