import { DataplaneHealthRead, DataplaneVersionSupportStatus } from "core/api/types/AirbyteClient";

import { StatusLevel } from "./components/StatusCard";
import { CardData, AgeMessage } from "./types";
import { healthStatusMessageId } from "../../healthStatus";

// A data plane without a check-in always has the UNKNOWN status.
export const NEVER_CONNECTED: CardData = {
  level: "neutral",
  value: { messageId: healthStatusMessageId.UNKNOWN },
};

interface AgeMessageIds {
  days: string;
  hours: string;
  minutes: string;
  seconds: string;
}

const CHECK_IN_MESSAGE_IDS: AgeMessageIds = {
  days: "settings.organization.regions.dataplane.checkIn.days",
  hours: "settings.organization.regions.dataplane.checkIn.hours",
  minutes: "settings.organization.regions.dataplane.checkIn.minutes",
  seconds: "settings.organization.regions.dataplane.checkIn.seconds",
};

// The same pieces as the check-in messages, without the "ago", for a time that is still running.
const DURATION_MESSAGE_IDS: AgeMessageIds = {
  days: "settings.organization.regions.dataplane.duration.days",
  hours: "settings.organization.regions.dataplane.duration.hours",
  minutes: "settings.organization.regions.dataplane.duration.minutes",
  seconds: "settings.organization.regions.dataplane.duration.seconds",
};

const formatTime = (ageMs: number, messageIds: AgeMessageIds): AgeMessage => {
  const totalSeconds = Math.max(0, Math.floor(ageMs / 1000));
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (days > 0) {
    return { messageId: messageIds.days, values: { days, hours } };
  }
  if (hours > 0) {
    return { messageId: messageIds.hours, values: { hours, minutes, seconds } };
  }
  if (minutes > 0) {
    return { messageId: messageIds.minutes, values: { minutes, seconds } };
  }
  return { messageId: messageIds.seconds, values: { seconds } };
};

export const formatAge = (ageMs: number): AgeMessage => formatTime(ageMs, CHECK_IN_MESSAGE_IDS);

export const formatDuration = (durationMs: number): AgeMessage => formatTime(durationMs, DURATION_MESSAGE_IDS);

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

const COMPATIBILITY_BY_SUPPORT_STATUS: Record<DataplaneVersionSupportStatus, CardData> = {
  SUPPORTED: {
    level: "success",
    value: { messageId: "settings.organization.regions.dataplane.compatibility.supported" },
  },
  SUPPORT_ENDED: {
    level: "error",
    value: { messageId: "settings.organization.regions.dataplane.compatibility.notSupported" },
  },
  UNKNOWN: {
    level: "neutral",
    value: { messageId: "settings.organization.regions.dataplane.compatibility.unknown" },
  },
};

// The server leaves the support out unless the data plane health UI is enabled, so there is no card without it.
export const getCompatibilityCardData = (dataplane: DataplaneHealthRead): CardData | undefined =>
  dataplane.dataplane_version_support && COMPATIBILITY_BY_SUPPORT_STATUS[dataplane.dataplane_version_support.status];
