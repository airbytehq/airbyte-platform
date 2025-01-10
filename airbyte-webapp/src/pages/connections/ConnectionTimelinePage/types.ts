import * as yup from "yup";

import {
  ConnectionEventType,
  ConnectionScheduleDataBasicScheduleTimeUnit,
  ConnectionScheduleType,
  FailureOrigin,
  FailureReason,
  FailureType,
  FieldTransformTransformType,
  Geography,
  JobConfigType,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  StreamAttributeTransformTransformType,
  StreamMapperType,
  StreamTransformTransformType,
} from "core/api/types/AirbyteClient";

/**
 * add a new event type to the connection timeline:
 *
 * 1. Add a new schema object for the event summary (unless it matches one that already exists)
 * 2. Add a new schema for the event, OR add the event_type to an existing one
 * 3. use <schemaYouMade>.isValidSync(event) to check if the event is of that type in the return statement of ConnectionTimelinePage.  You can use InferType<typeof <schemaYouMade>> to reference the type in code.
 */

/**
 * SCHEMA OBJECTS
 */

const connectionAutoDisabledReasons = [
  "TOO_MANY_FAILED_JOBS_WITH_NO_RECENT_SUCCESS",
  "SCHEMA_CHANGES_ARE_BREAKING",
  "DISABLE_CONNECTION_IF_ANY_SCHEMA_CHANGES",
  "INVALID_CREDIT_BALANCE",
  "CONNECTOR_NOT_SUPPORTED",
  "WORKSPACE_IS_DELINQUENT",
  "INVOICE_MARKED_UNCOLLECTIBLE",
  "INVALID_PAYMENT_METHOD",
  "UNSUBSCRIBED",

  // this is from `ConnectionAutoUpdatedReason` but is also stamped onto the disabledReason field
  "SCHEMA_CHANGE_AUTO_PROPAGATE",

  // these two are no longer written for new events, but can exist in existing timelines.
  // can be removed once all such events are expired/removed
  "ONLY_FAILED_JOBS_RECENTLY",
  "TOO_MANY_CONSECUTIVE_FAILED_JOBS_IN_A_ROW",
];

const connectorChangeReasons = ["SYSTEM", "USER"];

// property-specific schemas
/**
 * @typedef {import("core/api/types/AirbyteClient").StreamDescriptor}
 */
const streamDescriptorSchema = yup.object({
  name: yup.string().required(),
  namespace: yup.string().optional(),
});

const jobRunningStreamSchema = yup.object({
  streamName: yup.string().required(),
  streamNamespace: yup.string().optional(),
  configType: yup.mixed<JobConfigType>().oneOf(["sync", "refresh", "clear", "reset_connection"]).required(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").FieldSchema}
 */
const fieldSchema = yup.object({
  schema: yup.object().optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").FieldSchemaUpdate}
 */
const fieldSchemaUpdateSchema = yup.object({
  newSchema: fieldSchema.required(),
  oldSchema: fieldSchema.required(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").FieldTransform}
 */
const fieldTransformSchema = yup.object({
  addField: fieldSchema.optional(),
  breaking: yup.boolean().required(),
  fieldName: yup.array().of(yup.string()).required(),
  removeField: fieldSchema.optional(),
  transformType: yup
    .mixed<FieldTransformTransformType>()
    .oneOf(["add_field", "remove_field", "update_field_schema"])
    .required(),
  updateFieldSchema: fieldSchemaUpdateSchema.optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamAttributePrimaryKeyUpdate}
 */
const streamAttributePrimaryKeyUpdateSchema = yup.object({
  newPrimaryKey: yup.array().of(yup.array().of(yup.string())).optional(),
  oldPrimaryKey: yup.array().of(yup.array().of(yup.string())).optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamAttributeTransform}
 */
const streamAttributeTransformSchema = yup.object({
  breaking: yup.boolean().required(),
  transformType: yup.mixed<StreamAttributeTransformTransformType>().oneOf(["update_primary_key"]).required(),
  updatePrimaryKey: streamAttributePrimaryKeyUpdateSchema.optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamTransformUpdateStream}
 */
const streamTransformUpdateStreamSchema = yup.object({
  fieldTransforms: yup.array().of(fieldTransformSchema).optional(),
  streamAttributeTransforms: yup.array().of(streamAttributeTransformSchema).optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamTransform}
 */
const streamTransformsSchema = yup.object({
  streamDescriptor: streamDescriptorSchema.required(),
  transformType: yup
    .mixed<StreamTransformTransformType>()
    .oneOf(["add_stream", "remove_stream", "update_stream"])
    .required(),
  updateStream: streamTransformUpdateStreamSchema.optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").CatalogDiff}
 */
const catalogDiffSchema = yup.object({
  transforms: yup.array().of(streamTransformsSchema).required(),
});

const sourceDefinitionUpdateSchema = yup.object({
  name: yup.string().required(),
  sourceDefinitionId: yup.string().required(),
  newDockerImageTag: yup.string().required(),
  oldDockerImageTag: yup.string().required(),
  changeReason: yup.string().oneOf(connectorChangeReasons).required(),
});

const destinationDefinitionUpdateSchema = yup.object({
  name: yup.string().required(),
  destinationDefinitionId: yup.string().required(),
  newDockerImageTag: yup.string().required(),
  oldDockerImageTag: yup.string().required(),
  changeReason: yup.string().oneOf(connectorChangeReasons).required(),
});

export type TimelineFailureReason = Omit<FailureReason, "timestamp">;

export const jobFailureReasonSchema = yup.object({
  failureType: yup.mixed<FailureType>().optional(),
  failureOrigin: yup.mixed<FailureOrigin>().optional(),
  externalMessage: yup.string().optional(),
  internalMessage: yup.string().optional(),
  retryable: yup.boolean().optional(),
  timestamp: yup.number().optional(),
  stacktrace: yup.string().optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").UserReadInConnectionEvent}
 */
export const userInEventSchema = yup.object({
  email: yup.string().optional(),
  id: yup.string().optional(),
  name: yup.string().optional(),
});

// artificial job events
export const jobRunningSummarySchema = yup.object({
  jobId: yup.number().required(),
  streams: yup.array().of(jobRunningStreamSchema).required(),
  configType: yup.mixed<JobConfigType>().oneOf(["clear", "reset_connection", "sync", "refresh"]).required(),
});

// jobs
export const syncEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  bytesLoaded: yup.number().required(),
  recordsLoaded: yup.number().optional(),
  jobId: yup.number().required(),
});

export const syncFailureEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  bytesLoaded: yup.number().optional(),
  recordsLoaded: yup.number().optional(),
  jobId: yup.number().required(),
  failureReason: jobFailureReasonSchema.nullable(),
});

export const refreshEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  bytesLoaded: yup.number().required(),
  streams: yup.array().of(streamDescriptorSchema).required(),
  jobId: yup.number().required(),
});

export const clearEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  streams: yup.array().of(streamDescriptorSchema).required(),
  jobId: yup.number().required(),
});

export const jobStartedSummarySchema = yup.object({
  streams: yup.array().of(streamDescriptorSchema).optional(),
  startTimeEpochSeconds: yup.number().required(),
  jobId: yup.number().required(),
});

export const connectionDisabledEventSummarySchema = yup.object({
  disabledReason: yup.string().oneOf(connectionAutoDisabledReasons),
});

const ConnectionScheduleDataBasicScheduleSchema = yup.object().shape({
  timeUnit: yup
    .mixed<ConnectionScheduleDataBasicScheduleTimeUnit>()
    .oneOf(["minutes", "hours", "days", "weeks", "months"])
    .optional(),
  units: yup.number().optional(),
});

const ConnectionScheduleDataCronSchema = yup.object().shape({
  cronExpression: yup.string().optional(),
  cronTimeZone: yup.string().optional(),
});

export const scheduleDataSchema = yup.object().shape({
  basicSchedule: ConnectionScheduleDataBasicScheduleSchema.optional(),
  cron: ConnectionScheduleDataCronSchema.optional(),
});

export const connectionSettingsUpdateEventSummaryPatchesShape = {
  scheduleType: yup.object({
    from: yup.string().oneOf(Object.values(ConnectionScheduleType)),
    to: yup.string().oneOf(Object.values(ConnectionScheduleType)),
  }),
  scheduleData: yup.object().shape({ from: scheduleDataSchema, to: scheduleDataSchema }),
  name: yup.object().shape({ from: yup.string(), to: yup.string() }),
  namespaceDefinition: yup.object().shape({
    from: yup.string().oneOf(Object.values(NamespaceDefinitionType)),
    to: yup.string().oneOf(Object.values(NamespaceDefinitionType)),
  }),
  namespaceFormat: yup.object().shape({ from: yup.string(), to: yup.string() }),
  prefix: yup.object().shape({ from: yup.string(), to: yup.string() }),
  geography: yup
    .object()
    .shape({ from: yup.string().oneOf(Object.values(Geography)), to: yup.string().oneOf(Object.values(Geography)) }),
  notifySchemaChanges: yup.object().shape({ from: yup.boolean(), to: yup.boolean() }),
  nonBreakingChangesPreference: yup.object().shape({
    from: yup.string().oneOf(Object.values(NonBreakingChangesPreference)),
    to: yup.string().oneOf(Object.values(NonBreakingChangesPreference)),
  }),

  backfillPreference: yup.object().shape({ from: yup.string(), to: yup.string() }),
} as const;

export const patchFields = Object.keys(connectionSettingsUpdateEventSummaryPatchesShape) as Array<
  keyof typeof connectionSettingsUpdateEventSummaryPatchesShape
>;

export const connectionSettingsUpdateEventSummarySchema = yup.object({
  patches: yup
    .object(connectionSettingsUpdateEventSummaryPatchesShape)
    // ensure that at least one of the known patch fields is present
    // pull `originalValue` from the test context as the obj argument provided has all of the known fields set to non-confirming objects
    .test((_, testContext) => {
      const { originalValue } = testContext as unknown as { originalValue: Record<string, unknown> };
      return Object.keys(originalValue).some(
        // resourceRequirements is a valid patch, and we want to continue logging it, but we do not want to surface it in the UI at this time.
        (key) => (patchFields as string[]).includes(key)
      );
    })
    .required(),
});

export const schemaUpdateSummarySchema = yup.object({
  catalogDiff: catalogDiffSchema.required(),
  updateReason: yup.mixed().oneOf(["SCHEMA_CHANGE_AUTO_PROPAGATE"]).optional(),
});

export const mappingEventSummarySchema = yup.object({
  streamName: yup.string().required(),
  streamNamespace: yup.string().optional(),
  mapperType: yup
    .mixed<StreamMapperType>()
    .oneOf([...Object.values(StreamMapperType)])
    .required(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").ConnectionEvent}
 */
export const generalEventSchema = yup.object({
  id: yup.string().required(),
  connectionId: yup.string().required(),
  user: userInEventSchema.optional(),
  createdAt: yup.number().required(),
  eventType: yup
    .mixed<ConnectionEventType | "RUNNING_JOB">()
    .oneOf([...Object.values(ConnectionEventType), "RUNNING_JOB"])
    .required(),
  summary: yup.mixed().required(),
});

export const syncEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([ConnectionEventType.SYNC_SUCCEEDED, ConnectionEventType.SYNC_CANCELLED])
    .required(),
  summary: syncEventSummarySchema.required(),
});

export const syncFailEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([ConnectionEventType.SYNC_FAILED, ConnectionEventType.SYNC_INCOMPLETE])
    .required(),
  summary: syncFailureEventSummarySchema.required(),
});

export const refreshEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([
      ConnectionEventType.REFRESH_SUCCEEDED,
      ConnectionEventType.REFRESH_FAILED,
      ConnectionEventType.REFRESH_INCOMPLETE,
      ConnectionEventType.REFRESH_CANCELLED,
    ])
    .required(),
  summary: refreshEventSummarySchema.required(),
});

export const clearEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([
      ConnectionEventType.CLEAR_SUCCEEDED,
      ConnectionEventType.CLEAR_FAILED,
      ConnectionEventType.CLEAR_INCOMPLETE,
      ConnectionEventType.CLEAR_CANCELLED,
    ])
    .required(),
  summary: clearEventSummarySchema.required(),
});

export const jobStartedEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([ConnectionEventType.CLEAR_STARTED, ConnectionEventType.REFRESH_STARTED, ConnectionEventType.SYNC_STARTED])
    .required(),
  summary: jobStartedSummarySchema.required(),
});

export const jobRunningSchema = generalEventSchema.shape({
  eventType: yup.string().oneOf(["RUNNING_JOB"]).required(),
  summary: jobRunningSummarySchema.required(),
});

export const connectionEnabledEventSchema = generalEventSchema.shape({
  eventType: yup.mixed<ConnectionEventType>().oneOf([ConnectionEventType.CONNECTION_ENABLED]).required(),
});

export const connectionDisabledEventSchema = generalEventSchema.shape({
  eventType: yup.mixed<ConnectionEventType>().oneOf([ConnectionEventType.CONNECTION_DISABLED]).required(),
  summary: connectionDisabledEventSummarySchema.required(),
});

export const connectionSettingsUpdateEventSchema = generalEventSchema.shape({
  eventType: yup.mixed<ConnectionEventType>().oneOf([ConnectionEventType.CONNECTION_SETTINGS_UPDATE]).required(),
  summary: connectionSettingsUpdateEventSummarySchema.required(),
});

export const schemaUpdateEventSchema = generalEventSchema.shape({
  eventType: yup.mixed<ConnectionEventType>().oneOf([ConnectionEventType.SCHEMA_UPDATE]).required(),
  summary: schemaUpdateSummarySchema.required(),
});

export const sourceConnectorUpdateEventSchema = generalEventSchema.shape({
  eventType: yup.mixed<ConnectionEventType>().oneOf([ConnectionEventType.CONNECTOR_UPDATE]).required(),
  summary: sourceDefinitionUpdateSchema.required(),
});

export const destinationConnectorUpdateEventSchema = generalEventSchema.shape({
  eventType: yup.mixed<ConnectionEventType>().oneOf([ConnectionEventType.CONNECTOR_UPDATE]).required(),
  summary: destinationDefinitionUpdateSchema.required(),
});

export const mappingEventSchema = generalEventSchema.shape({
  // TODO: add mapping event types from AirbyteClient once they are defined
  // eventType: yup.mixed<ConnectionEventType>().oneOf([ConnectionEventType.MAPPING_CREATE, ConnectionEventType.MAPPING_UPDATE, ConnectionEventType.MAPPING_DELETE]).required(),
  eventType: yup.mixed().oneOf(["MAPPING_CREATE", "MAPPING_UPDATE", "MAPPING_DELETE"]).required(),
  summary: mappingEventSummarySchema.required(),
});

export interface ConnectionTimelineRunningEvent {
  id: string;
  eventType: string;
  connectionId: string;
  createdAt: number;
  summary: {
    streams: Array<{
      streamName: string;
      streamNamespace: string | undefined;
      configType: JobConfigType;
    }>;
    configType: JobConfigType;
    jobId: number;
  };
  user: {
    email: string;
    name: string;
    id: string;
  };
}
