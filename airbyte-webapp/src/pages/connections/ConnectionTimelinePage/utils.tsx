import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon, IconColor } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { ConnectionEventType } from "core/api/types/AirbyteClient";

/**
 * GENERAL TIMELINE UTILITIES
 */

export const titleIdMap: Record<ConnectionEventType, string> = {
  [ConnectionEventType.CLEAR_CANCELLED]: "connection.timeline.clear_cancelled",
  [ConnectionEventType.CLEAR_FAILED]: "connection.timeline.clear_failed",
  [ConnectionEventType.CLEAR_INCOMPLETE]: "connection.timeline.clear_incomplete",
  [ConnectionEventType.CLEAR_STARTED]: "connection.timeline.clear_started",
  [ConnectionEventType.CLEAR_SUCCEEDED]: "connection.timeline.clear_succeeded",
  [ConnectionEventType.CONNECTION_SETTINGS_UPDATE]: "connection.timeline.connection_settings_update",
  [ConnectionEventType.CONNECTION_ENABLED]: "connection.timeline.connection_enabled",
  [ConnectionEventType.CONNECTION_DISABLED]: "connection.timeline.connection_disabled",
  [ConnectionEventType.REFRESH_CANCELLED]: "connection.timeline.refresh_cancelled",
  [ConnectionEventType.REFRESH_FAILED]: "connection.timeline.refresh_failed",
  [ConnectionEventType.REFRESH_INCOMPLETE]: "connection.timeline.refresh_incomplete",
  [ConnectionEventType.REFRESH_SUCCEEDED]: "connection.timeline.refresh_succeeded",
  [ConnectionEventType.SYNC_CANCELLED]: "connection.timeline.sync_cancelled",
  [ConnectionEventType.SYNC_FAILED]: "connection.timeline.sync_failed",
  [ConnectionEventType.SYNC_INCOMPLETE]: "connection.timeline.sync_incomplete",
  [ConnectionEventType.SYNC_SUCCEEDED]: "connection.timeline.sync_succeeded",
  [ConnectionEventType.SYNC_STARTED]: "connection.timeline.sync_started",
  [ConnectionEventType.REFRESH_STARTED]: "connection.timeline.refresh_started",
  [ConnectionEventType.SCHEMA_UPDATE]: "connection.timeline.schema_update",

  // todo
  [ConnectionEventType.CONNECTOR_UPDATE]: "",
  [ConnectionEventType.UNKNOWN]: "",
};

/**
 * JOB-SPECIFIC EVENT UTILITIES
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

/**
 * FILTER UTILITIES
 */

export interface TimelineFilterValues {
  status: "success" | "failure" | "incomplete" | "cancelled" | "";
  eventCategory: "sync" | "clear" | "refresh" | "connection_settings" | "schema_update" | "";
  startDate: string;
  endDate: string;
  eventId: string;
  openLogs: "true" | "false" | "";
  jobId: string;
  attemptNumber: string;
}

export const eventTypeByStatusFilterValue: Record<
  Exclude<TimelineFilterValues["status"], "">,
  ConnectionEventType[]
> = {
  success: [
    ConnectionEventType.SYNC_SUCCEEDED,
    ConnectionEventType.CLEAR_SUCCEEDED,
    ConnectionEventType.REFRESH_SUCCEEDED,
  ],
  failure: [ConnectionEventType.SYNC_FAILED, ConnectionEventType.CLEAR_FAILED, ConnectionEventType.REFRESH_FAILED],
  incomplete: [
    ConnectionEventType.SYNC_INCOMPLETE,
    ConnectionEventType.CLEAR_INCOMPLETE,
    ConnectionEventType.REFRESH_INCOMPLETE,
  ],
  cancelled: [
    ConnectionEventType.SYNC_CANCELLED,
    ConnectionEventType.CLEAR_CANCELLED,
    ConnectionEventType.REFRESH_CANCELLED,
  ],
};

export const eventTypeByTypeFilterValue: Record<
  Exclude<TimelineFilterValues["eventCategory"], "">,
  ConnectionEventType[]
> = {
  sync: [
    ConnectionEventType.SYNC_SUCCEEDED,
    ConnectionEventType.SYNC_CANCELLED,
    ConnectionEventType.SYNC_FAILED,
    ConnectionEventType.SYNC_INCOMPLETE,
    ConnectionEventType.SYNC_STARTED,
  ],
  clear: [
    ConnectionEventType.CLEAR_SUCCEEDED,
    ConnectionEventType.CLEAR_CANCELLED,
    ConnectionEventType.CLEAR_FAILED,
    ConnectionEventType.CLEAR_INCOMPLETE,
    ConnectionEventType.CLEAR_STARTED,
  ],
  refresh: [
    ConnectionEventType.REFRESH_SUCCEEDED,
    ConnectionEventType.REFRESH_CANCELLED,
    ConnectionEventType.REFRESH_FAILED,
    ConnectionEventType.REFRESH_INCOMPLETE,
    ConnectionEventType.REFRESH_STARTED,
  ],
  connection_settings: [
    ConnectionEventType.CONNECTION_ENABLED,
    ConnectionEventType.CONNECTION_DISABLED,
    ConnectionEventType.CONNECTION_SETTINGS_UPDATE,
  ],
  schema_update: [ConnectionEventType.SCHEMA_UPDATE],
};

export const getStatusByEventType = (eventType: ConnectionEventType) => {
  if (
    eventType === ConnectionEventType.SYNC_FAILED ||
    eventType === ConnectionEventType.CLEAR_FAILED ||
    eventType === ConnectionEventType.REFRESH_FAILED
  ) {
    return "failed";
  } else if (
    eventType === ConnectionEventType.SYNC_INCOMPLETE ||
    eventType === ConnectionEventType.CLEAR_INCOMPLETE ||
    eventType === ConnectionEventType.REFRESH_INCOMPLETE
  ) {
    return "incomplete";
  } else if (
    eventType === ConnectionEventType.SYNC_CANCELLED ||
    eventType === ConnectionEventType.CLEAR_CANCELLED ||
    eventType === ConnectionEventType.REFRESH_CANCELLED
  ) {
    return "cancelled";
  }
  return "succeeded";
};
type filterIconType = "statusSuccess" | "statusError" | "statusWarning" | "sync" | "statusCancelled";

const timelineStatusFilterColors: Record<Exclude<TimelineFilterValues["status"], "">, IconColor> = {
  success: "success",
  failure: "error",
  cancelled: "disabled",
  incomplete: "warning",
};

const generateStatusFilterOption = (value: TimelineFilterValues["status"], id: string, iconType: filterIconType) => {
  return {
    label:
      value === "" ? null : (
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

const generateEventTypeFilterOption = (value: TimelineFilterValues["eventCategory"], id: string) => ({
  label:
    value === "" ? null : (
      <Text color="grey" bold as="span">
        <FormattedMessage id={id} />
      </Text>
    ),
  value,
});

export const statusFilterOptions = (filterValues: TimelineFilterValues) => {
  return [
    {
      label: (
        <Text color={!["sync", "clear", "refresh", ""].includes(filterValues.eventCategory) ? "grey300" : "grey"} bold>
          <FormattedMessage id="connection.timeline.filters.allStatuses" />
        </Text>
      ),
      value: "",
    },
    generateStatusFilterOption("success", "connection.timeline.filters.success", "statusSuccess"),
    generateStatusFilterOption("failure", "connection.timeline.filters.failure", "statusError"),
    generateStatusFilterOption("incomplete", "connection.timeline.filters.incomplete", "statusWarning"),
    generateStatusFilterOption("cancelled", "connection.timeline.filters.cancelled", "statusCancelled"),
  ];
};

export const eventTypeFilterOptions = (filterValues: TimelineFilterValues) => {
  return [
    {
      label: (
        <Text color="grey" bold>
          <FormattedMessage id="connection.timeline.filters.allEventTypes" />
        </Text>
      ),
      value: "",
    },
    generateEventTypeFilterOption("sync", "connection.timeline.filters.sync"),
    generateEventTypeFilterOption("refresh", "connection.timeline.filters.refresh"),
    generateEventTypeFilterOption("clear", "connection.timeline.filters.clear"),
    ...(filterValues.status === ""
      ? [
          generateEventTypeFilterOption("connection_settings", "connection.timeline.filters.connection_settings"),
          generateEventTypeFilterOption("schema_update", "connection.timeline.filters.schema_update"),
        ]
      : []),
  ];
};
