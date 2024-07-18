import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon, IconColor } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { JobStatus } from "core/api/types/AirbyteClient";

/**
 * TYPES -- may change as mock data is removed
 */

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
  clear_succeeded = "clear_succeeded",
  clear_failed = "clear_failed",
  clear_incomplete = "clear_incomplete",
  clear_cancelled = "clear_cancelled",
  refresh_succeeded = "refresh_succeeded",
  refresh_failed = "refresh_failed",
  refresh_incomplete = "refresh_incomplete",
  refresh_cancelled = "refresh_cancelled",
}

export interface ConnectionTimelineEventList {
  events: ConnectionTimelineEvent[];
}

export interface ConnectionTimelineJobStatsProps {
  bytesCommitted?: number;
  recordsCommitted?: number;
  jobId: number;
  failureMessage?: string;
  jobStartedAt: number;
  jobEndedAt: number;
  attemptsCount: number;
  jobStatus: JobStatus;
}

/**
 * GENERAL TIMELINE UTILITIES
 */

export const titleIdMap: Record<ConnectionTimelineEvent["eventType"], string> = {
  [ConnectionTimelineEventType.clear_cancelled]: "connection.timeline.clear_cancelled",
  [ConnectionTimelineEventType.clear_failed]: "connection.timeline.clear_failed",
  [ConnectionTimelineEventType.clear_incomplete]: "connection.timeline.clear_incomplete",
  [ConnectionTimelineEventType.clear_succeeded]: "connection.timeline.clear_succeeded",
  [ConnectionTimelineEventType.refresh_cancelled]: "connection.timeline.refresh_cancelled",
  [ConnectionTimelineEventType.refresh_failed]: "connection.timeline.refresh_failed",
  [ConnectionTimelineEventType.refresh_incomplete]: "connection.timeline.refresh_incomplete",
  [ConnectionTimelineEventType.refresh_succeeded]: "connection.timeline.refresh_succeeded",
  [ConnectionTimelineEventType.sync_cancelled]: "connection.timeline.sync_cancelled",
  [ConnectionTimelineEventType.sync_failed]: "connection.timeline.sync_failed",
  [ConnectionTimelineEventType.sync_incomplete]: "connection.timeline.sync_incomplete",
  [ConnectionTimelineEventType.sync_succeeded]: "connection.timeline.sync_succeeded",
};

/**
 * JOB EVENT UTILITIES
 */

export const getStatusIcon = (jobStatus: "failed" | "incomplete" | "cancelled" | "succeeded") => {
  if (jobStatus === "failed") {
    return "statusError";
  } else if (jobStatus === "incomplete") {
    return "statusWarning";
  } else if (jobStatus === "cancelled") {
    return "statusCancelled";
  }
  return "statusSuccess";
};

export const extractJobIdFromSummary = (eventSummary: ConnectionTimelineEvent["eventSummary"]) => {
  if (typeof eventSummary.jobId !== "string" || Number.isNaN(parseInt(eventSummary.jobId))) {
    return undefined;
  }
  return parseInt(eventSummary.jobId);
};
export const castEventSummaryToConnectionTimelineJobStatsProps = (
  eventSummary: ConnectionTimelineEvent["eventSummary"]
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
    failureMessage: eventSummary.message.toString(),
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
        : eventSummary.jobStatus === "incomplete"
        ? "incomplete"
        : (undefined as unknown as JobStatus),
  };
};

export const extractStreamsFromTimelineEvent = (eventSummary: ConnectionTimelineEvent["eventSummary"]) => {
  return Array.isArray(eventSummary.streams) && eventSummary.streams.length > 0 ? eventSummary.streams : [];
};

/**
 * FILTER UTILITIES
 */

export interface TimelineFilterValues {
  status: "success" | "failure" | "incomplete" | "cancelled" | null;
  eventType: "sync" | "clear" | "refresh" | null;
  eventId: string | null;
  openLogs: "true" | "false" | null;
}

export const eventTypeByFilterValue: Record<
  Exclude<TimelineFilterValues["eventType"], null>,
  ConnectionTimelineEventType[]
> = {
  sync: [
    ConnectionTimelineEventType.sync_succeeded,
    ConnectionTimelineEventType.sync_failed,
    ConnectionTimelineEventType.sync_incomplete,
    ConnectionTimelineEventType.sync_cancelled,
  ],
  clear: [
    ConnectionTimelineEventType.clear_succeeded,
    ConnectionTimelineEventType.clear_failed,
    ConnectionTimelineEventType.clear_incomplete,
    ConnectionTimelineEventType.clear_cancelled,
  ],
  refresh: [
    ConnectionTimelineEventType.refresh_succeeded,
    ConnectionTimelineEventType.refresh_failed,
    ConnectionTimelineEventType.refresh_incomplete,
    ConnectionTimelineEventType.refresh_cancelled,
  ],
};

export const eventTypeByStatusFilterValue: Record<
  Exclude<TimelineFilterValues["status"], null>,
  ConnectionTimelineEventType[]
> = {
  success: [
    ConnectionTimelineEventType.sync_succeeded,
    ConnectionTimelineEventType.clear_succeeded,
    ConnectionTimelineEventType.refresh_succeeded,
  ],
  failure: [
    ConnectionTimelineEventType.sync_failed,
    ConnectionTimelineEventType.clear_failed,
    ConnectionTimelineEventType.refresh_failed,
  ],
  incomplete: [
    ConnectionTimelineEventType.sync_incomplete,
    ConnectionTimelineEventType.clear_incomplete,
    ConnectionTimelineEventType.refresh_incomplete,
  ],
  cancelled: [
    ConnectionTimelineEventType.sync_cancelled,
    ConnectionTimelineEventType.clear_cancelled,
    ConnectionTimelineEventType.refresh_cancelled,
  ],
};

type filterIconType = "statusSuccess" | "statusError" | "statusWarning" | "sync" | "statusCancelled";

const timelineStatusFilterColors: Record<Exclude<TimelineFilterValues["status"], null>, IconColor> = {
  success: "success",
  failure: "error",
  cancelled: "disabled",
  incomplete: "warning",
};

const generateStatusFilterOption = (value: TimelineFilterValues["status"], id: string, iconType: filterIconType) => {
  return {
    label:
      value === null ? null : (
        <FlexContainer gap="sm" alignItems="center">
          <FlexItem>
            <Icon type={iconType} size="md" color={timelineStatusFilterColors[value]} />
          </FlexItem>
          <FlexItem>
            <Text color="grey" bold as="span">
              &nbsp; <FormattedMessage id={id} />
            </Text>
          </FlexItem>
        </FlexContainer>
      ),
    value,
  };
};

const generateEventTypeFilterOption = (value: TimelineFilterValues["eventType"], id: string) => ({
  label: (
    <Text color="grey" bold as="span">
      <FormattedMessage id={id} />
    </Text>
  ),
  value,
});

export const statusFilterOptions = [
  {
    label: (
      <Text color="grey" bold>
        <FormattedMessage id="connection.timeline.filters.allStatuses" />
      </Text>
    ),
    value: null,
  },
  generateStatusFilterOption("success", "connection.timeline.filters.success", "statusSuccess"),
  generateStatusFilterOption("failure", "connection.timeline.filters.failure", "statusError"),
  generateStatusFilterOption("incomplete", "connection.timeline.filters.incomplete", "statusWarning"),
  generateStatusFilterOption("cancelled", "connection.timeline.filters.cancelled", "statusCancelled"),
];

export const eventTypeFilterOptions = [
  {
    label: (
      <Text color="grey" bold>
        <FormattedMessage id="connection.timeline.filters.allEventTypes" />
      </Text>
    ),
    value: null,
  },
  generateEventTypeFilterOption("sync", "connection.timeline.filters.sync"),
  generateEventTypeFilterOption("refresh", "connection.timeline.filters.refresh"),
  generateEventTypeFilterOption("clear", "connection.timeline.filters.clear"),
];
