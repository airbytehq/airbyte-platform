import { Impact } from "./types";

export const IMPACT_DISPLAY_MESSAGES: Record<Impact, string> = {
  MAJOROUTAGE: "There are ongoing incidents (major outage)",
  PARTIALOUTAGE: "There are ongoing incidents (partial outage)",
  DEGRADEDPERFORMANCE: "There are ongoing incidents (degraded performance)",
} as const;

export const INCIDENT_STATUS = {
  degradedPerformance: "DEGRADEDPERFORMANCE",
  partialOutage: "PARTIALOUTAGE",
  majorOutage: "MAJOROUTAGE",
} as const;

export const impactPriority: Record<Impact, number> = {
  MAJOROUTAGE: 3,
  PARTIALOUTAGE: 2,
  DEGRADEDPERFORMANCE: 1,
} as const;

export const PAGE_STATUS = {
  up: "UP" as const,
  hasIssues: "HASISSUES" as const,
  underMaintenance: "UNDERMAINTENANCE" as const,
};
