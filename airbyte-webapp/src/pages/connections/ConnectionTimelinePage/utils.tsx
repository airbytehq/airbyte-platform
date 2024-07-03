import { FailureReason, JobStatus } from "core/api/types/AirbyteClient";

export interface ConnectionTimelineEvent {
  id: string;
  connectionId: string;
  userName?: string;
  timestamp: number;
  eventType: ConnectionTimelineEventType;
  eventSummary: Record<string, string | string[] | Record<string, string | string[]>>;
}

export enum ConnectionTimelineEventType {
  sync_succeeded = "sync_succeeded",
  sync_failed = "sync_failed",
  sync_incomplete = "sync_incomplete",
  sync_cancelled = "sync_cancelled",
  sync_partially_succeeded = "sync_partially_succeeded",
  clear_succeeded = "clear_succeeded",
  clear_failed = "clear_failed",
  clear_incomplete = "clear_incomplete",
  clear_cancelled = "clear_cancelled",
  clear_partially_succeeded = "clear_partially_succeeded",
  refresh_succeeded = "refresh_succeeded",
  refresh_failed = "refresh_failed",
  refresh_incomplete = "refresh_incomplete",
  refresh_cancelled = "refresh_cancelled",
  refresh_partially_succeeded = "refresh_partially_succeeded",
}

export interface ConnectionTimelineEventList {
  events: ConnectionTimelineEvent[];
}

export interface ConnectionTimelineJobStatsProps {
  bytesCommitted?: number;
  recordsCommitted?: number;
  jobId: number;
  failureSummary?: FailureReason;
  jobStartedAt: number;
  jobEndedAt: number;
  attemptsCount: number;
  jobStatus: JobStatus;
}

export const castEventSummaryToConnectionTimelineJobStatsProps = (
  eventSummary: Record<string, string | string[] | Record<string, string | string[]>>
): ConnectionTimelineJobStatsProps | undefined => {
  if (
    typeof eventSummary.jobId !== "string" ||
    typeof eventSummary.jobStartedAt !== "string" ||
    typeof eventSummary.jobEndedAt !== "string" ||
    typeof eventSummary.attemptsCount !== "string" ||
    typeof eventSummary.jobStatus !== "string"
  ) {
    return undefined;
  }

  return {
    bytesCommitted: typeof eventSummary.bytesCommitted === "string" ? parseInt(eventSummary.bytesCommitted) : undefined,
    recordsCommitted:
      typeof eventSummary.recordsCommitted === "string" ? parseInt(eventSummary.recordsCommitted) : undefined,
    jobId: parseInt(eventSummary.jobId),
    failureSummary: eventSummary.failureSummary ? (eventSummary.failureSummary as unknown as FailureReason) : undefined,
    jobStartedAt: parseInt(eventSummary.jobStartedAt),
    jobEndedAt: parseInt(eventSummary.jobEndedAt),
    attemptsCount: parseInt(eventSummary.attemptsCount),
    jobStatus:
      eventSummary.jobStatus === "failed"
        ? "failed"
        : eventSummary.jobStatus === "cancelled"
        ? "cancelled"
        : eventSummary.jobStatus === "succeeded"
        ? "succeeded"
        : (undefined as unknown as JobStatus),
  };
};
