import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

import { StatusCardProps, StatusLevel } from "./components/StatusCard";
import { healthStatusMessageId } from "../../healthStatus";

export type CardData = Pick<StatusCardProps, "level" | "value">;

// A data plane without a check-in always has the UNKNOWN status.
export const NEVER_CONNECTED: CardData = {
  level: "neutral",
  value: { messageId: healthStatusMessageId.UNKNOWN },
};

export interface AgeMessage {
  messageId: string;
  values: Record<string, number>;
}

export const formatAge = (ageMs: number): AgeMessage => {
  const totalSeconds = Math.max(0, Math.floor(ageMs / 1000));
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (days > 0) {
    return { messageId: "settings.organization.regions.dataplane.checkIn.days", values: { days, hours } };
  }
  if (hours > 0) {
    return { messageId: "settings.organization.regions.dataplane.checkIn.hours", values: { hours, minutes, seconds } };
  }
  if (minutes > 0) {
    return { messageId: "settings.organization.regions.dataplane.checkIn.minutes", values: { minutes, seconds } };
  }
  return { messageId: "settings.organization.regions.dataplane.checkIn.seconds", values: { seconds } };
};

interface SecretStorageCardData extends CardData {
  // Absent when there is no check to date, such as for a data plane that never connected.
  lastChecked?: AgeMessage;
}

type SecretStorageSummary = "yes" | "no" | "incomplete";

const SECRET_STORAGE_MESSAGE_ID = "settings.organization.regions.dataplane.diagnostics.secretStorage";

const SECRET_STORAGE_SUMMARY: Record<SecretStorageSummary, { level: StatusLevel; messageId: string }> = {
  yes: { level: "success", messageId: `${SECRET_STORAGE_MESSAGE_ID}.yes` },
  no: { level: "error", messageId: `${SECRET_STORAGE_MESSAGE_ID}.no` },
  incomplete: { level: "warning", messageId: `${SECRET_STORAGE_MESSAGE_ID}.incomplete` },
};

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

  const secretStorageList = (dataplane.diagnostics ?? []).filter(
    (diagnostic) => diagnostic.kind === DataplaneDiagnosticKind.SECRET_STORAGE
  );

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
