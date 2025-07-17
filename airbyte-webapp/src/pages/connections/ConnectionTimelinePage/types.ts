import { z } from "zod";

import {
  CatalogConfigDiff,
  CatalogDiff,
  ConnectionEventType,
  ConnectionScheduleDataBasicScheduleTimeUnit,
  ConnectionScheduleType,
  DestinationSyncMode,
  FailureOrigin,
  FailureReason,
  FailureType,
  FieldSchema,
  FieldSchemaUpdate,
  FieldTransform,
  FieldTransformTransformType,
  JobConfigType,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  StreamAttributePrimaryKeyUpdate,
  StreamAttributeTransform,
  StreamAttributeTransformTransformType,
  StreamCursorFieldDiff,
  StreamDescriptor,
  StreamFieldStatusChangedStatus,
  StreamMapperType,
  StreamPrimaryKeyDiff,
  StreamSyncModeDiff,
  StreamTransform,
  StreamTransformTransformType,
  StreamTransformUpdateStream,
  SyncMode,
  UserReadInConnectionEvent,
} from "core/api/types/AirbyteClient";
import { ToZodSchema } from "core/utils/zod";

import { CatalogChangeEventItem } from "./components/CatalogChangeEventItem";
import { ClearEventItem } from "./components/ClearEventItem";
import { ConnectionDisabledEventItem } from "./components/ConnectionDisabledEventItem";
import { ConnectionEnabledEventItem } from "./components/ConnectionEnabledEventItem";
import { ConnectionSettingsUpdateEventItem } from "./components/ConnectionSettingsUpdateEventItem";
import { ConnectorUpdateEventItem } from "./components/ConnectorUpdateEventItem";
import { JobStartEventItem } from "./components/JobStartEventItem";
import { MappingEventItem } from "./components/MappingEventItem";
import { RefreshEventItem } from "./components/RefreshEventItem";
import { RunningJobItem } from "./components/RunningJobItem";
import { SchemaUpdateEventItem } from "./components/SchemaUpdateEventItem";
import { SyncEventItem } from "./components/SyncEventItem";
import { SyncFailEventItem } from "./components/SyncFailEventItem";

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
  "MANUALLY_LOCKED",

  // this is from `ConnectionAutoUpdatedReason` but is also stamped onto the disabledReason field
  "SCHEMA_CHANGE_AUTO_PROPAGATE",

  // these two are no longer written for new events, but can exist in existing timelines.
  // can be removed once all such events are expired/removed
  "ONLY_FAILED_JOBS_RECENTLY",
  "TOO_MANY_CONSECUTIVE_FAILED_JOBS_IN_A_ROW",
] as const;

const connectorChangeReasons = ["SYSTEM", "USER"] as const;
// TODO: ask BE team to use already defined types - ConnectorType
const ConnectorType = ["SOURCE", "DESTINATION"] as const;

/**
 * RUNNING JOB EVENT TYPE
 * This is a list of all possible running job events from JobConfigType.
 * @typedef {import("core/api/types/AirbyteClient").JobConfigType}
 */
export const ConnectionRunningEventType = {
  SYNC_RUNNING: "SYNC_RUNNING",
  REFRESH_RUNNING: "REFRESH_RUNNING",
  CLEAR_RUNNING: "CLEAR_RUNNING",
  CONNECTION_RESET_RUNNING: "CONNECTION_RESET_RUNNING",
} as const;
// eslint-disable-next-line @typescript-eslint/no-redeclare
export type ConnectionRunningEventType = (typeof ConnectionRunningEventType)[keyof typeof ConnectionRunningEventType];

// property-specific schemas
/**
 * @typedef {import("core/api/types/AirbyteClient").StreamDescriptor}
 */
const streamDescriptorSchema = z.object({
  name: z.string(),
  namespace: z.string().optional(),
} satisfies ToZodSchema<StreamDescriptor>);

const jobRunningStreamSchema = z.object({
  streamName: z.string(),
  streamNamespace: z.string().optional(),
  configType: z.enum([JobConfigType.sync, JobConfigType.refresh, JobConfigType.clear, JobConfigType.reset_connection]),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").FieldSchema}
 */
const fieldSchema = z.object({
  schema: z.record(z.any()).optional(),
} satisfies ToZodSchema<FieldSchema>);

/**
 * @typedef {import("core/api/types/AirbyteClient").FieldSchemaUpdate}
 */
const fieldSchemaUpdateSchema = z.object({
  newSchema: fieldSchema,
  oldSchema: fieldSchema,
} satisfies ToZodSchema<FieldSchemaUpdate>);

/**
 * @typedef {import("core/api/types/AirbyteClient").FieldTransform}
 */
const fieldTransformSchema = z.object({
  addField: fieldSchema.optional(),
  breaking: z.boolean(),
  fieldName: z.array(z.string()),
  removeField: fieldSchema.optional(),
  transformType: z.nativeEnum(FieldTransformTransformType),
  updateFieldSchema: fieldSchemaUpdateSchema.optional(),
} satisfies ToZodSchema<FieldTransform>);

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamAttributePrimaryKeyUpdate}
 */
const streamAttributePrimaryKeyUpdateSchema = z.object({
  newPrimaryKey: z.array(z.array(z.string())).optional(),
  oldPrimaryKey: z.array(z.array(z.string())).optional(),
} satisfies ToZodSchema<StreamAttributePrimaryKeyUpdate>);

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamAttributeTransform}
 */
const streamAttributeTransformSchema = z.object({
  breaking: z.boolean(),
  transformType: z.enum([StreamAttributeTransformTransformType.update_primary_key]),
  updatePrimaryKey: streamAttributePrimaryKeyUpdateSchema.optional(),
} satisfies ToZodSchema<StreamAttributeTransform>);

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamTransformUpdateStream}
 */
const streamTransformUpdateStreamSchema = z.object({
  fieldTransforms: z.array(fieldTransformSchema),
  streamAttributeTransforms: z.array(streamAttributeTransformSchema),
} satisfies ToZodSchema<StreamTransformUpdateStream>);

/**
 * @typedef {import("core/api/types/AirbyteClient").StreamTransform}
 */
const streamTransformSchema = z.object({
  streamDescriptor: streamDescriptorSchema,
  transformType: z.nativeEnum(StreamTransformTransformType),
  updateStream: streamTransformUpdateStreamSchema.optional(),
} satisfies ToZodSchema<StreamTransform>);

/**
 * @typedef {import("core/api/types/AirbyteClient").CatalogDiff}
 */
const catalogDiffSchema = z.object({
  transforms: z.array(streamTransformSchema),
} satisfies ToZodSchema<CatalogDiff>);

const connectorUpdateSchema = z.object({
  toVersion: z.string(),
  fromVersion: z.string(),
  connectorName: z.string(),
  connectorType: z.enum(ConnectorType),
  // TODO: ask BE team to add this prop, untill then it is optional
  changeReason: z.enum(connectorChangeReasons).optional(),
  // TODO: ask BE team how to handle this prop, untill then it is optional
  triggeredBy: z.enum(["BREAKING_CHANGE_MANUAL"]).optional(),
});

const streamFieldStatusChangedSchema = z.object({
  streamName: z.string().optional(),
  streamNamespace: z.string().optional(),
  fields: z.array(z.string()).optional(),
  status: z.nativeEnum(StreamFieldStatusChangedStatus).optional(),
});

const syncModeSchema = z.nativeEnum(SyncMode);
const destinationSyncModeSchema = z.nativeEnum(DestinationSyncMode);

const syncModeChangedSchema = z.object({
  currentDestinationSyncMode: destinationSyncModeSchema.optional(),
  currentSourceSyncMode: syncModeSchema.optional(),
  prevDestinationSyncMode: destinationSyncModeSchema.optional(),
  prevSourceSyncMode: syncModeSchema.optional(),
  streamName: z.string().optional(),
  streamNamespace: z.string().optional(),
} satisfies ToZodSchema<StreamSyncModeDiff>);

const streamPrimaryKeyDiffSchema = z.object({
  current: z.array(z.array(z.string())).optional(),
  prev: z.array(z.array(z.string())).optional(),
  streamName: z.string().optional(),
  streamNamespace: z.string().optional(),
} satisfies ToZodSchema<StreamPrimaryKeyDiff>);

const streamCursorFieldDiffSchema = z.object({
  current: z.array(z.string()).optional(),
  prev: z.array(z.string()).optional(),
  streamName: z.string().optional(),
  streamNamespace: z.string().optional(),
} satisfies ToZodSchema<StreamCursorFieldDiff>);

const catalogConfigDiffSchema = z.object({
  streamsEnabled: z.array(streamFieldStatusChangedSchema).optional(),
  streamsDisabled: z.array(streamFieldStatusChangedSchema).optional(),
  fieldsEnabled: z.array(streamFieldStatusChangedSchema).optional(),
  fieldsDisabled: z.array(streamFieldStatusChangedSchema).optional(),
  syncModesChanged: z.array(syncModeChangedSchema).optional(),
  cursorFieldsChanged: z.array(streamCursorFieldDiffSchema).optional(),
  primaryKeysChanged: z.array(streamPrimaryKeyDiffSchema).optional(),
} satisfies ToZodSchema<CatalogConfigDiff>);

export type TimelineFailureReason = Omit<FailureReason, "timestamp">;

export const jobFailureReasonSchema = z.object({
  failureType: z.nativeEnum(FailureType).optional(),
  failureOrigin: z.nativeEnum(FailureOrigin).optional(),
  externalMessage: z.string().optional(),
  internalMessage: z.string().optional(),
  retryable: z.boolean().optional(),
  timestamp: z.number().optional(),
  stacktrace: z.string().optional(),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").UserReadInConnectionEvent}
 */
export const userInEventSchema = z.object({
  email: z.string().optional(),
  id: z.string().optional(),
  name: z.string().optional(),
  isDeleted: z.boolean(),
} satisfies ToZodSchema<UserReadInConnectionEvent>);

// artificial job events
export const jobRunningSummarySchema = z.object({
  jobId: z.number(),
  streams: z.array(jobRunningStreamSchema),
  configType: z.enum([JobConfigType.clear, JobConfigType.reset_connection, JobConfigType.sync, JobConfigType.refresh]),
});

// jobs
export const jobSummarySchema = z.object({
  attemptsCount: z.number().optional(),
  bytesLoaded: z.number().optional(),
  recordsLoaded: z.number().optional(),
  recordsRejected: z.number().optional(),
  rejectedRecordsMeta: z
    .object({
      storageUri: z.string().optional(),
      cloudConsoleUrl: z.string().optional(),
    })
    .optional(),
  endTimeEpochSeconds: z.number(),
  startTimeEpochSeconds: z.number(),
  jobId: z.number(),
});

export const syncEventSummarySchema = jobSummarySchema;

export const syncFailureEventSummarySchema = jobSummarySchema.extend({
  failureReason: jobFailureReasonSchema.nullish(),
});

export const refreshEventSummarySchema = jobSummarySchema.extend({
  streams: z.array(streamDescriptorSchema),
  failureReason: jobFailureReasonSchema.nullish(),
});

export const clearEventSummarySchema = jobSummarySchema
  .omit({
    bytesLoaded: true,
    recordsLoaded: true,
  })
  .extend({
    streams: z.array(streamDescriptorSchema),
  });

export const jobStartedSummarySchema = jobSummarySchema
  .omit({
    bytesLoaded: true,
    recordsLoaded: true,
    endTimeEpochSeconds: true,
  })
  .extend({
    streams: z.array(streamDescriptorSchema).optional(),
  });

export const connectionDisabledEventSummarySchema = z.object({
  disabledReason: z.enum(connectionAutoDisabledReasons).optional(),
});

const ConnectionScheduleDataBasicScheduleSchema = z.object({
  timeUnit: z.nativeEnum(ConnectionScheduleDataBasicScheduleTimeUnit),
  units: z.number(),
});

const ConnectionScheduleDataCronSchema = z.object({
  cronExpression: z.string().optional(),
  cronTimeZone: z.string().optional(),
});

export const scheduleDataSchema = z.object({
  basicSchedule: ConnectionScheduleDataBasicScheduleSchema.optional(),
  cron: ConnectionScheduleDataCronSchema.optional(),
});

export const connectionSettingsUpdateEventSummaryPatchesShape = {
  scheduleType: z.object({
    from: z.nativeEnum(ConnectionScheduleType),
    to: z.nativeEnum(ConnectionScheduleType),
  }),
  scheduleData: z.object({
    from: scheduleDataSchema,
    to: scheduleDataSchema,
  }),
  name: z.object({
    from: z.string(),
    to: z.string(),
  }),
  namespaceDefinition: z.object({
    from: z.nativeEnum(NamespaceDefinitionType),
    to: z.nativeEnum(NamespaceDefinitionType),
  }),
  namespaceFormat: z.object({ from: z.string(), to: z.string() }),
  prefix: z.object({ from: z.string(), to: z.string() }),
  geography: z.object({
    from: z.string(),
    to: z.string(),
  }),
  notifySchemaChanges: z.object({
    from: z.boolean(),
    to: z.boolean(),
  }),
  nonBreakingChangesPreference: z.object({
    from: z.nativeEnum(NonBreakingChangesPreference),
    to: z.nativeEnum(NonBreakingChangesPreference),
  }),

  backfillPreference: z.object({ from: z.string(), to: z.string() }),
} as const;

export const connectionSettingsUpdateEventSummarySchema = z.object({
  patches: z
    .record(
      z.object({
        from: z.union([z.string(), z.boolean(), scheduleDataSchema]).optional(),
        to: z.union([z.string(), z.boolean(), scheduleDataSchema]).optional(),
      })
    )
    // ensure that at least one of the known patch fields is present
    .refine(
      (data: Partial<typeof connectionSettingsUpdateEventSummaryPatchesShape>) =>
        Object.keys(data).length > 0 &&
        // resourceRequirements is a valid patch, and we want to continue logging it, but we do not want to surface it in the UI at this time.
        Object.keys(data).some((key) => key in connectionSettingsUpdateEventSummaryPatchesShape),
      "At least one valid patch field must be present"
    ),
});

export const schemaUpdateSummarySchema = z.object({
  catalogDiff: catalogDiffSchema,
  updateReason: z.enum(["SCHEMA_CHANGE_AUTO_PROPAGATE"]).optional(),
});

export const schemaConfigUpdateSchema = z.object({
  airbyteCatalogDiff: z.object({
    catalogDiff: catalogDiffSchema,
    catalogConfigDiff: catalogConfigDiffSchema,
  }),
});

export const mappingEventSummarySchema = z.object({
  streamName: z.string(),
  streamNamespace: z.string().optional(),
  mapperType: z.nativeEnum(StreamMapperType),
});

/**
 * @typedef {import("core/api/types/AirbyteClient").ConnectionEvent}
 */
export const generalEventSchema = z.object({
  id: z.string(),
  connectionId: z.string(),
  user: userInEventSchema.optional(),
  createdAt: z.number().optional(),
  eventType: z.union([z.nativeEnum(ConnectionEventType), z.nativeEnum(ConnectionRunningEventType)]),
  summary: z
    .object({
      patches: z.record(z.unknown()).optional(),
    })
    .passthrough(),
});

export const syncEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.SYNC_SUCCEEDED, ConnectionEventType.SYNC_CANCELLED]),
  summary: syncEventSummarySchema,
});

export const syncFailEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.SYNC_FAILED, ConnectionEventType.SYNC_INCOMPLETE]),
  summary: syncFailureEventSummarySchema,
});

export const refreshEventSchema = generalEventSchema.extend({
  eventType: z.enum([
    ConnectionEventType.REFRESH_SUCCEEDED,
    ConnectionEventType.REFRESH_FAILED,
    ConnectionEventType.REFRESH_INCOMPLETE,
    ConnectionEventType.REFRESH_CANCELLED,
  ]),
  summary: refreshEventSummarySchema,
});

export const clearEventSchema = generalEventSchema.extend({
  eventType: z.enum([
    ConnectionEventType.CLEAR_SUCCEEDED,
    ConnectionEventType.CLEAR_FAILED,
    ConnectionEventType.CLEAR_INCOMPLETE,
    ConnectionEventType.CLEAR_CANCELLED,
  ]),
  summary: clearEventSummarySchema,
});

export const jobStartedEventSchema = generalEventSchema.extend({
  eventType: z.enum([
    ConnectionEventType.CLEAR_STARTED,
    ConnectionEventType.REFRESH_STARTED,
    ConnectionEventType.SYNC_STARTED,
  ]),
  summary: jobStartedSummarySchema,
});

export const jobRunningSchema = generalEventSchema.extend({
  eventType: z.nativeEnum(ConnectionRunningEventType),
  summary: jobRunningSummarySchema,
});

export const connectionEnabledEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.CONNECTION_ENABLED]),
});

export const connectionDisabledEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.CONNECTION_DISABLED]),
  summary: connectionDisabledEventSummarySchema,
});

export const connectionSettingsUpdateEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.CONNECTION_SETTINGS_UPDATE]),
  summary: connectionSettingsUpdateEventSummarySchema,
});

export const schemaUpdateEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.SCHEMA_UPDATE]),
  summary: schemaUpdateSummarySchema,
});

export const connectorUpdateEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.CONNECTOR_UPDATE]),
  summary: connectorUpdateSchema,
});

export const schemaConfigUpdateEventSchema = generalEventSchema.extend({
  eventType: z.enum([ConnectionEventType.SCHEMA_CONFIG_UPDATE]),
  summary: schemaConfigUpdateSchema,
});

export const mappingEventSchema = generalEventSchema.extend({
  // TODO: add mapping event types from AirbyteClient once they are defined
  // eventType: z.enum([ConnectionEventType.MAPPING_CREATE, ConnectionEventType.MAPPING_UPDATE, ConnectionEventType.MAPPING_DELETE]).required(),
  eventType: z.enum(["MAPPING_CREATE", "MAPPING_UPDATE", "MAPPING_DELETE"]),
  summary: mappingEventSummarySchema,
});

export interface ConnectionTimelineRunningEvent {
  id: string;
  eventType: ConnectionRunningEventType;
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
  user: UserReadInConnectionEvent;
}

export interface EventTypeToSchema {
  [ConnectionEventType.SYNC_SUCCEEDED]: z.infer<typeof syncEventSchema>;
  [ConnectionEventType.SYNC_CANCELLED]: z.infer<typeof syncEventSchema>;
  [ConnectionEventType.SYNC_FAILED]: z.infer<typeof syncFailEventSchema>;
  [ConnectionEventType.SYNC_INCOMPLETE]: z.infer<typeof syncFailEventSchema>;
  [ConnectionEventType.SYNC_STARTED]: z.infer<typeof jobStartedEventSchema>;
  [ConnectionRunningEventType.SYNC_RUNNING]: z.infer<typeof jobRunningSchema>;
  [ConnectionEventType.REFRESH_SUCCEEDED]: z.infer<typeof refreshEventSchema>;
  [ConnectionEventType.REFRESH_FAILED]: z.infer<typeof refreshEventSchema>;
  [ConnectionEventType.REFRESH_INCOMPLETE]: z.infer<typeof refreshEventSchema>;
  [ConnectionEventType.REFRESH_CANCELLED]: z.infer<typeof refreshEventSchema>;
  [ConnectionEventType.REFRESH_STARTED]: z.infer<typeof jobStartedEventSchema>;
  [ConnectionRunningEventType.REFRESH_RUNNING]: z.infer<typeof jobRunningSchema>;
  [ConnectionEventType.CLEAR_SUCCEEDED]: z.infer<typeof clearEventSchema>;
  [ConnectionEventType.CLEAR_FAILED]: z.infer<typeof clearEventSchema>;
  [ConnectionEventType.CLEAR_INCOMPLETE]: z.infer<typeof clearEventSchema>;
  [ConnectionEventType.CLEAR_CANCELLED]: z.infer<typeof clearEventSchema>;
  [ConnectionEventType.CLEAR_STARTED]: z.infer<typeof jobStartedEventSchema>;
  [ConnectionRunningEventType.CLEAR_RUNNING]: z.infer<typeof jobRunningSchema>;
  [ConnectionEventType.CONNECTION_ENABLED]: z.infer<typeof connectionEnabledEventSchema>;
  [ConnectionEventType.CONNECTION_DISABLED]: z.infer<typeof connectionDisabledEventSchema>;
  [ConnectionEventType.CONNECTION_SETTINGS_UPDATE]: z.infer<typeof connectionSettingsUpdateEventSchema>;
  [ConnectionRunningEventType.CONNECTION_RESET_RUNNING]: z.infer<typeof jobRunningSchema>;
  [ConnectionEventType.SCHEMA_UPDATE]: z.infer<typeof schemaUpdateEventSchema>;
  [ConnectionEventType.CONNECTOR_UPDATE]: z.infer<typeof connectorUpdateEventSchema>;
  [ConnectionEventType.SCHEMA_CONFIG_UPDATE]: z.infer<typeof schemaConfigUpdateEventSchema>;
  MAPPING_CREATE: z.infer<typeof mappingEventSchema>;
  MAPPING_UPDATE: z.infer<typeof mappingEventSchema>;
  MAPPING_DELETE: z.infer<typeof mappingEventSchema>;
}

export const eventTypeToSchemaMap: {
  [K in keyof EventTypeToSchema]: {
    schema: z.ZodSchema<EventTypeToSchema[K]>;
    component: React.FC<{ event: EventTypeToSchema[K] }>;
  };
} = {
  [ConnectionEventType.SYNC_SUCCEEDED]: { schema: syncEventSchema, component: SyncEventItem },
  [ConnectionEventType.SYNC_CANCELLED]: { schema: syncEventSchema, component: SyncEventItem },
  [ConnectionEventType.SYNC_FAILED]: { schema: syncFailEventSchema, component: SyncFailEventItem },
  [ConnectionEventType.SYNC_INCOMPLETE]: { schema: syncFailEventSchema, component: SyncFailEventItem },
  [ConnectionEventType.SYNC_STARTED]: { schema: jobStartedEventSchema, component: JobStartEventItem },
  [ConnectionRunningEventType.SYNC_RUNNING]: { schema: jobRunningSchema, component: RunningJobItem },
  [ConnectionEventType.REFRESH_SUCCEEDED]: { schema: refreshEventSchema, component: RefreshEventItem },
  [ConnectionEventType.REFRESH_FAILED]: { schema: refreshEventSchema, component: RefreshEventItem },
  [ConnectionEventType.REFRESH_INCOMPLETE]: { schema: refreshEventSchema, component: RefreshEventItem },
  [ConnectionEventType.REFRESH_CANCELLED]: { schema: refreshEventSchema, component: RefreshEventItem },
  [ConnectionEventType.REFRESH_STARTED]: { schema: jobStartedEventSchema, component: JobStartEventItem },
  [ConnectionRunningEventType.REFRESH_RUNNING]: { schema: jobRunningSchema, component: RunningJobItem },
  [ConnectionEventType.CLEAR_SUCCEEDED]: { schema: clearEventSchema, component: ClearEventItem },
  [ConnectionEventType.CLEAR_FAILED]: { schema: clearEventSchema, component: ClearEventItem },
  [ConnectionEventType.CLEAR_INCOMPLETE]: { schema: clearEventSchema, component: ClearEventItem },
  [ConnectionEventType.CLEAR_CANCELLED]: { schema: clearEventSchema, component: ClearEventItem },
  [ConnectionEventType.CLEAR_STARTED]: { schema: jobStartedEventSchema, component: JobStartEventItem },
  [ConnectionRunningEventType.CLEAR_RUNNING]: { schema: jobRunningSchema, component: RunningJobItem },
  [ConnectionEventType.CONNECTION_ENABLED]: {
    schema: connectionEnabledEventSchema,
    component: ConnectionEnabledEventItem,
  },
  [ConnectionEventType.CONNECTION_DISABLED]: {
    schema: connectionDisabledEventSchema,
    component: ConnectionDisabledEventItem,
  },
  [ConnectionEventType.CONNECTION_SETTINGS_UPDATE]: {
    schema: connectionSettingsUpdateEventSchema,
    component: ConnectionSettingsUpdateEventItem,
  },
  [ConnectionRunningEventType.CONNECTION_RESET_RUNNING]: { schema: jobRunningSchema, component: RunningJobItem },
  [ConnectionEventType.SCHEMA_UPDATE]: { schema: schemaUpdateEventSchema, component: SchemaUpdateEventItem },
  [ConnectionEventType.CONNECTOR_UPDATE]: { schema: connectorUpdateEventSchema, component: ConnectorUpdateEventItem },
  [ConnectionEventType.SCHEMA_CONFIG_UPDATE]: {
    schema: schemaConfigUpdateEventSchema,
    component: CatalogChangeEventItem,
  },
  MAPPING_CREATE: { schema: mappingEventSchema, component: MappingEventItem },
  MAPPING_UPDATE: { schema: mappingEventSchema, component: MappingEventItem },
  MAPPING_DELETE: { schema: mappingEventSchema, component: MappingEventItem },
} as const;
