import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon, IconColor } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import {
  CatalogDiff,
  ConnectionEventType,
  ConnectionSyncProgressRead,
  FieldTransform,
  FieldTransformTransformType,
  JobConfigType,
  StreamFieldStatusChanged,
  StreamTransform,
  StreamTransformTransformType,
} from "core/api/types/AirbyteClient";

import { CatalogConfigDiffExtended } from "./components/CatalogChangeEventItem";
import { ConnectionRunningEventType, ConnectionTimelineRunningEvent } from "./types";

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
  [ConnectionEventType.SCHEMA_CONFIG_UPDATE]: "connection.timeline.schema_config_update",

  // todo
  [ConnectionEventType.CONNECTOR_UPDATE]: "",
  [ConnectionEventType.UNKNOWN]: "",

  // TODO: waiting for the backend to add these
  // issue_link: https://github.com/airbytehq/airbyte-internal-issues/issues/10947
  // [ConnectionEventType.MAPPING_CREATE]: "connection.timeline.mapping_create",
  // [ConnectionEventType.MAPPING_UPDATE]: "connection.timeline.mapping_update",
  // [ConnectionEventType.MAPPING_DELETE]: "connection.timeline.mapping_delete",
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
 * Create a purely FE running event from a job
 */
export const createRunningEventFromJob = (
  syncProgressData: ConnectionSyncProgressRead,
  connectionId: string
): ConnectionTimelineRunningEvent => {
  const jobMapping: Record<
    keyof Pick<typeof JobConfigType, "sync" | "refresh" | "clear" | "reset_connection">,
    ConnectionRunningEventType
  > = {
    [JobConfigType.sync]: ConnectionRunningEventType.SYNC_RUNNING,
    [JobConfigType.refresh]: ConnectionRunningEventType.REFRESH_RUNNING,
    [JobConfigType.clear]: ConnectionRunningEventType.CLEAR_RUNNING,
    [JobConfigType.reset_connection]: ConnectionRunningEventType.CONNECTION_RESET_RUNNING,
  };

  const eventType = jobMapping[syncProgressData.configType as keyof typeof jobMapping];

  return {
    id: "running",
    eventType,
    connectionId,
    createdAt: syncProgressData.syncStartedAt ?? Date.now() / 1000,
    summary: {
      streams: syncProgressData.streams.map((stream) => ({
        streamName: stream.streamName,
        streamNamespace: stream.streamNamespace,
        configType: stream.configType,
      })),
      configType: syncProgressData.configType,
      jobId: syncProgressData.jobId ?? 0,
    },
    user: { email: "", name: "", id: "", isDeleted: false },
  };
};

/**
 * FILTER UTILITIES
 */

export interface TimelineFilterValues {
  status: "success" | "failure" | "incomplete" | "cancelled" | "";
  eventCategory: "sync" | "clear" | "refresh" | "connection_settings" | "schema_update" | "schema_config_update" | "";
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
  schema_config_update: [ConnectionEventType.SCHEMA_CONFIG_UPDATE],
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
          generateEventTypeFilterOption("schema_config_update", "connection.timeline.filters.schema_config_update"),
        ]
      : []),
  ];
};

export const isSemanticVersionTags = (newTag: string, oldTag: string): boolean =>
  [newTag, oldTag].every((tag) => /^\d+\.\d+\.\d+$/.test(tag));

export const isVersionUpgraded = (newVersion: string, oldVersion: string): boolean => {
  const parseVersion = (version: string) => version.split(".").map(Number);
  const newParsedVersion = parseVersion(newVersion);
  const oldParsedVersion = parseVersion(oldVersion);

  for (let i = 0; i < Math.max(newParsedVersion.length, oldParsedVersion.length); i++) {
    const num1 = newParsedVersion[i] || 0;
    const num2 = oldParsedVersion[i] || 0;
    if (num1 > num2) {
      return true;
    }
    if (num1 < num2) {
      return false;
    }
  }
  return false;
};

/**
 * Merge stream changes to avoid duplicate stream names
 */
const mergeStreamChanges = (changes: StreamFieldStatusChanged[]): StreamFieldStatusChanged[] => {
  return Array.from(
    changes
      .reduce((acc, change) => {
        const key = `${change.streamNamespace ?? ""}_${change.streamName}`;
        if (!acc.has(key)) {
          acc.set(key, { ...change });
        } else {
          const existing = acc.get(key)!;
          existing.fields = [...new Set([...existing.fields!, ...change.fields!])];
        }
        return acc;
      }, new Map<string, StreamFieldStatusChanged>())
      .values()
  );
};

/**
 * Transform non-user changes from the catalog diff to the catalog config diff to be used in the CatalogConfigDiffModal
 */
export const transformCatalogDiffToCatalogConfigDiff = (catalogDiff: CatalogDiff): CatalogConfigDiffExtended => {
  const changes = catalogDiff.transforms.reduce(
    (acc, transform: StreamTransform) => {
      const { streamDescriptor, transformType } = transform;
      const baseChange = {
        streamName: streamDescriptor.name,
        streamNamespace: streamDescriptor.namespace,
      };

      switch (transformType) {
        case StreamTransformTransformType.add_stream:
          acc.streamsAdded.push(baseChange);
          break;
        case StreamTransformTransformType.remove_stream:
          acc.streamsRemoved.push(baseChange);
          break;
        case StreamTransformTransformType.update_stream:
          if (transform.updateStream) {
            transform.updateStream.fieldTransforms.forEach((fieldTransform: FieldTransform) => {
              const streamChange = {
                ...baseChange,
                fields: [fieldTransform.fieldName.join(".")],
              };

              if (fieldTransform.transformType === FieldTransformTransformType.add_field) {
                acc.fieldsAdded.push(streamChange);
              } else if (fieldTransform.transformType === FieldTransformTransformType.remove_field) {
                acc.fieldsRemoved.push(streamChange);
              }
            });
          }
          break;
      }
      return acc;
    },
    {
      streamsAdded: [] as StreamFieldStatusChanged[],
      streamsRemoved: [] as StreamFieldStatusChanged[],
      fieldsAdded: [] as StreamFieldStatusChanged[],
      fieldsRemoved: [] as StreamFieldStatusChanged[],
    }
  );

  return {
    streamsAdded: changes.streamsAdded.length > 0 ? mergeStreamChanges(changes.streamsAdded) : [],
    streamsRemoved: changes.streamsRemoved.length > 0 ? mergeStreamChanges(changes.streamsRemoved) : [],
    fieldsAdded: changes.fieldsAdded.length > 0 ? mergeStreamChanges(changes.fieldsAdded) : [],
    fieldsRemoved: changes.fieldsRemoved.length > 0 ? mergeStreamChanges(changes.fieldsRemoved) : [],
  };
};
