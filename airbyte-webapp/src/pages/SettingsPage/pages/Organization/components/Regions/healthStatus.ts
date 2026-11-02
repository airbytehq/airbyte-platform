import { IconColor, IconType } from "components/ui/Icon";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";

type HealthStatus = DataplaneHealthRead["status"];

export const healthStatusMessageId: Record<HealthStatus, string> = {
  HEALTHY: "dataplaneHealth.status.healthy",
  DEGRADED: "dataplaneHealth.status.degraded",
  UNHEALTHY: "dataplaneHealth.status.unhealthy",
  UNKNOWN: "settings.organization.regions.neverConnected",
};

export const healthStatusBadgeVariant: Record<HealthStatus, "green" | "yellow" | "red" | "grey"> = {
  HEALTHY: "green",
  DEGRADED: "yellow",
  UNHEALTHY: "red",
  UNKNOWN: "grey",
};

export const healthStatusSummaryMessageId: Record<HealthStatus, string> = {
  HEALTHY: "dataplaneHealth.summary.healthy",
  DEGRADED: "dataplaneHealth.summary.degraded",
  UNHEALTHY: "dataplaneHealth.summary.unhealthy",
  UNKNOWN: "settings.organization.regions.summary.neverConnected",
};

export interface HealthStatusIcon {
  type: IconType;
  color: IconColor;
}

export const healthStatusIcon: Record<HealthStatus, HealthStatusIcon> = {
  HEALTHY: { type: "statusSuccess", color: "success" },
  DEGRADED: { type: "statusWarning", color: "warning" },
  UNHEALTHY: { type: "statusError", color: "error" },
  UNKNOWN: { type: "statusInactive", color: "disabled" },
};

// The counters in the region header follow this order, healthiest first.
export const HEALTH_STATUSES: HealthStatus[] = ["HEALTHY", "DEGRADED", "UNHEALTHY", "UNKNOWN"];
