import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

import { SecretStorageCardData } from "./types";
import { formatAge, NEVER_CONNECTED } from "../../utils";
import { SecretStorageRow, StoreScope, StoreScopeType } from "../SecretStorageDetails";
import { StatusLevel } from "../StatusCard";

type SecretStorageSummary = "yes" | "no" | "incomplete";

const SECRET_STORAGE_SUMMARY: Record<SecretStorageSummary, { level: StatusLevel; messageId: string }> = {
  yes: { level: "success", messageId: "settings.organization.regions.dataplane.diagnostics.secretStorage.yes" },
  no: { level: "error", messageId: "settings.organization.regions.dataplane.diagnostics.secretStorage.no" },
  incomplete: {
    level: "warning",
    messageId: "settings.organization.regions.dataplane.diagnostics.secretStorage.incomplete",
  },
};

const getSecretStorageResults = (dataplane: DataplaneHealthRead): DataplaneDiagnosticRead[] =>
  (dataplane.diagnostics ?? []).filter((diagnostic) => diagnostic.kind === DataplaneDiagnosticKind.SECRET_STORAGE);

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
): SecretStorageCardData | undefined => {
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
