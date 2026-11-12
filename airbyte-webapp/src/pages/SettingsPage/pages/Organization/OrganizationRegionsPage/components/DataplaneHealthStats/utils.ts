import { DataplaneHealthRead } from "core/api/types/AirbyteClient";

import { StatusLevel } from "./components/StatusCard";
import { CardData, AgeMessage } from "./types";
import { healthStatusMessageId } from "../../healthStatus";

// A data plane without a check-in always has the UNKNOWN status.
export const NEVER_CONNECTED: CardData = {
  level: "neutral",
  value: { messageId: healthStatusMessageId.UNKNOWN },
};

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

const UNKNOWN_VERSION = "unknown";

const LEVEL_BY_HEALTH_STATUS: Record<DataplaneHealthRead["status"], StatusLevel> = {
  HEALTHY: "success",
  DEGRADED: "warning",
  UNHEALTHY: "error",
  UNKNOWN: "neutral",
};

export const getLastCheckInCardData = (dataplane: DataplaneHealthRead, now: number): CardData => {
  if (dataplane.last_heartbeat_timestamp === undefined) {
    return NEVER_CONNECTED;
  }

  return {
    level: LEVEL_BY_HEALTH_STATUS[dataplane.status],
    value: formatAge(now - dataplane.last_heartbeat_timestamp),
  };
};

// The server stores "unknown" when a check-in carries no version, so a connected data plane can lack one.
export const getVersionCardData = (dataplane: DataplaneHealthRead): CardData => {
  if (dataplane.last_heartbeat_timestamp === undefined) {
    return NEVER_CONNECTED;
  }

  const version = dataplane.dataplane_version;
  if (!version || version === UNKNOWN_VERSION) {
    return {
      level: "neutral",
      value: { messageId: "settings.organization.regions.dataplane.version.unavailable" },
    };
  }

  return { level: "success", value: { text: version } };
};
