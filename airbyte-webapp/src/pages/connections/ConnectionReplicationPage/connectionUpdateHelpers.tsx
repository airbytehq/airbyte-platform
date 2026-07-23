import { AirbyteCatalogWithAnySchema } from "area/connection/components/ConnectionForm/formConfig";
import { ConnectionValues } from "core/api";
import {
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  CatalogDiff,
  ConnectionScheduleData,
  ConnectionScheduleType,
  StreamMapperType,
  SyncMode,
} from "core/api/types/AirbyteClient";
import { equal } from "core/utils/objects";

/**
 * Types for consolidated changes modal
 */
export type ChangeWarning = "fullRefreshHighFrequency" | "refresh" | "clear";

export interface UserDecisions {
  fullRefreshHighFrequency?: "accept" | "reject";
  refresh?: "accept" | "reject";
  clear?: "accept" | "reject";
}

export interface ConnectionUpdateActions {
  skipReset: boolean;
  fullRefreshStreamsToRevert?: AirbyteStreamAndConfiguration[]; // Streams to revert to stored config if user rejected fullRefreshHighFrequency
}

export type ChangesMap = {
  [key in ChangeWarning]: ChangeItem[];
};
export interface ChangeItem {
  id: string;
  streamName: string;
  storedConfig?: AirbyteStreamAndConfiguration; // For reverting fullRefreshHighFrequency changes
}

const createLookupById = (catalog: AirbyteCatalog) => {
  const getStreamId = (stream: AirbyteStreamAndConfiguration) => {
    return `${stream.stream?.namespace ?? ""}-${stream.stream?.name}`;
  };

  return catalog.streams.reduce<Record<string, AirbyteStreamAndConfiguration>>((agg, stream) => {
    agg[getStreamId(stream)] = stream;
    return agg;
  }, {});
};

export const determineRecommendRefresh = (formSyncCatalog: AirbyteCatalog, storedSyncCatalog: AirbyteCatalog) => {
  const lookupConnectionValuesStreamById = createLookupById(storedSyncCatalog);

  return formSyncCatalog.streams.some((streamNode) => {
    if (!streamNode.config?.selected) {
      return false;
    }

    const formStream = structuredClone(streamNode);

    const connectionStream = structuredClone(
      lookupConnectionValuesStreamById[`${formStream.stream?.namespace ?? ""}-${formStream.stream?.name}`]
    );

    const promptBecauseOfSyncModes =
      // if we changed the stream to incremental from full refresh overwrite
      (formStream.config?.syncMode === "incremental" &&
        connectionStream.config?.syncMode === "full_refresh" &&
        connectionStream.config?.destinationSyncMode === "overwrite") ||
      (connectionStream.config?.syncMode === "incremental" &&
        formStream.config?.syncMode === "incremental" &&
        // if it was + is incremental but we change the selected fields, pk, or cursor
        (!equal(formStream.config?.selectedFields, connectionStream.config?.selectedFields) ||
          !equal(formStream.config?.primaryKey, connectionStream.config?.primaryKey) ||
          !equal(formStream.config?.cursorField, connectionStream.config?.cursorField)));

    // todo: once mappers ui is rolled out and we are no longer using this field, this can be removed
    const promptBecauseOfHashing = !equal(formStream.config?.hashedFields, connectionStream.config?.hashedFields);

    // Changes that should recommend a refresh/clear are those that change the destination output catalog
    const promptBecauseOfMappingsChanges =
      formStream.config?.mappers?.some((formStreamMapper) => {
        const storedMapper = connectionStream.config?.mappers?.find((m) => m.id === formStreamMapper.id);

        // return true for any new mapping EXCEPT row filtering ones
        if (!storedMapper) {
          return formStreamMapper.type !== StreamMapperType["row-filtering"];
        }

        // mapping type changes will change the suffix or newFieldName... there's an edge case here where if a user decides to add a mapper that changes a
        // hashed field `apple` to a rename mapping that uses the  `apple_hashed` for the newFieldname... the server would not actually do a refresh/clear
        // given the likelihood of that configuration, this should be sufficient for now, though.
        if (storedMapper.type !== formStreamMapper.type) {
          return true;
        }

        // return true if a hashing or encryption mapping has a new target field
        if (
          (formStreamMapper.type === StreamMapperType.hashing ||
            formStreamMapper.type === StreamMapperType.encryption) &&
          "targetField" in formStreamMapper.mapperConfiguration &&
          "targetField" in storedMapper.mapperConfiguration &&
          formStreamMapper.mapperConfiguration.targetField !== storedMapper.mapperConfiguration.targetField
        ) {
          return true;
        }

        // return true if a field renaming mapping changes either its original or new field name
        if (
          formStreamMapper.type === StreamMapperType["field-renaming"] &&
          (("originalFieldName" in formStreamMapper.mapperConfiguration &&
            "originalFieldName" in storedMapper.mapperConfiguration &&
            formStreamMapper.mapperConfiguration.originalFieldName !==
              storedMapper.mapperConfiguration.originalFieldName) ||
            ("newFieldName" in formStreamMapper.mapperConfiguration &&
              "newFieldName" in storedMapper.mapperConfiguration &&
              formStreamMapper.mapperConfiguration.newFieldName !== storedMapper.mapperConfiguration.newFieldName))
        ) {
          return true;
        }

        return false;
      }) ||
      connectionStream.config?.mappers?.some((storedMapper) => {
        // return true for any removed mapping EXCEPT row filtering ones
        return (
          !formStream.config?.mappers?.find((m) => m.id === storedMapper.id) &&
          storedMapper.type !== StreamMapperType["row-filtering"]
        );
      });

    return promptBecauseOfSyncModes || promptBecauseOfHashing || promptBecauseOfMappingsChanges;
  });
};

export const recommendActionOnConnectionUpdate = ({
  catalogDiff,
  formSyncCatalog,
  storedSyncCatalog,
}: {
  catalogDiff?: CatalogDiff;
  formSyncCatalog: AirbyteCatalog;
  storedSyncCatalog: AirbyteCatalog;
}) => {
  const hasCatalogDiffInEnabledStream = catalogDiff?.transforms.some(({ streamDescriptor }) => {
    const stream = formSyncCatalog.streams.find(
      ({ stream }) => streamDescriptor.name === stream?.name && streamDescriptor.namespace === stream.namespace
    );
    return stream?.config?.selected;
  });

  const hasUserChangesInEnabledStreams = !equal(
    formSyncCatalog.streams.filter((s) => s.config?.selected),
    storedSyncCatalog.streams.filter((s) => s.config?.selected)
  );

  const shouldRecommendRefresh = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);

  const shouldTrackAction = hasUserChangesInEnabledStreams || hasCatalogDiffInEnabledStream;

  return {
    shouldRecommendRefresh,
    shouldTrackAction,
  };
};

/**
 * Determines if a stream needs refresh/clear based on configuration changes
 * @param formStream - The form's stream configuration
 * @param connectionStream - The stored/original stream configuration
 * @returns true if the stream needs a refresh/clear
 */
const checkIfStreamNeedsRefresh = (
  formStream: AirbyteStreamAndConfiguration,
  connectionStream: AirbyteStreamAndConfiguration
): boolean => {
  const promptBecauseOfSyncModes =
    // if we changed the stream to incremental from full refresh overwrite
    (formStream.config?.syncMode === "incremental" &&
      connectionStream.config?.syncMode === "full_refresh" &&
      connectionStream.config?.destinationSyncMode === "overwrite") ||
    (connectionStream.config?.syncMode === "incremental" &&
      formStream.config?.syncMode === "incremental" &&
      // if it was + is incremental but we change the selected fields, pk, or cursor
      (!equal(formStream.config?.selectedFields, connectionStream.config?.selectedFields) ||
        !equal(formStream.config?.primaryKey, connectionStream.config?.primaryKey) ||
        !equal(formStream.config?.cursorField, connectionStream.config?.cursorField)));

  // Changes that should recommend a refresh/clear are those that change the destination output catalog
  const promptBecauseOfMappingsChanges =
    formStream.config?.mappers?.some((formStreamMapper) => {
      const storedMapper = connectionStream.config?.mappers?.find((m) => m.id === formStreamMapper.id);

      // return true for any new mapping EXCEPT row filtering ones
      if (!storedMapper) {
        return formStreamMapper.type !== StreamMapperType["row-filtering"];
      }

      // mapping type changes will change the suffix or newFieldName... there's an edge case here where if a user decides to add a mapper that changes a
      // hashed field `apple` to a rename mapping that uses the  `apple_hashed` for the newFieldname... the server would not actually do a refresh/clear
      // given the likelihood of that configuration, this should be sufficient for now, though.
      if (storedMapper.type !== formStreamMapper.type) {
        return true;
      }

      // return true if a hashing or encryption mapping has a new target field
      if (
        (formStreamMapper.type === StreamMapperType.hashing || formStreamMapper.type === StreamMapperType.encryption) &&
        "targetField" in formStreamMapper.mapperConfiguration &&
        "targetField" in storedMapper.mapperConfiguration &&
        formStreamMapper.mapperConfiguration.targetField !== storedMapper.mapperConfiguration.targetField
      ) {
        return true;
      }

      // return true if a field renaming mapping changes either its original or new field name
      if (
        formStreamMapper.type === StreamMapperType["field-renaming"] &&
        (("originalFieldName" in formStreamMapper.mapperConfiguration &&
          "originalFieldName" in storedMapper.mapperConfiguration &&
          formStreamMapper.mapperConfiguration.originalFieldName !==
            storedMapper.mapperConfiguration.originalFieldName) ||
          ("newFieldName" in formStreamMapper.mapperConfiguration &&
            "newFieldName" in storedMapper.mapperConfiguration &&
            formStreamMapper.mapperConfiguration.newFieldName !== storedMapper.mapperConfiguration.newFieldName))
      ) {
        return true;
      }

      return false;
    }) ||
    connectionStream.config?.mappers?.some((storedMapper) => {
      // return true for any removed mapping EXCEPT row filtering ones
      return (
        !formStream.config?.mappers?.find((m) => m.id === storedMapper.id) &&
        storedMapper.type !== StreamMapperType["row-filtering"]
      );
    });

  return promptBecauseOfSyncModes || Boolean(promptBecauseOfMappingsChanges);
};

/**
 * Returns all streams that need refresh/clear based on configuration changes
 * @param formSyncCatalog - The form's current sync catalog
 * @param storedSyncCatalog - The stored/original sync catalog
 * @returns Array of streams that need refresh/clear
 */
export const getRecommendRefreshStreams = (
  formSyncCatalog: AirbyteCatalog,
  storedSyncCatalog: AirbyteCatalog
): { shouldRefreshWarning: boolean; recommendedRefreshStreams: AirbyteStreamAndConfiguration[] } => {
  const lookupConnectionValuesStreamById = createLookupById(storedSyncCatalog);

  const recommendedRefreshStreams = formSyncCatalog.streams.filter((streamNode) => {
    if (!streamNode.config?.selected || !streamNode.stream?.name) {
      return false;
    }

    const formStream = structuredClone(streamNode);

    const connectionStream = structuredClone(
      lookupConnectionValuesStreamById[`${formStream.stream?.namespace ?? ""}-${formStream.stream?.name}`]
    );

    if (!connectionStream) {
      return false;
    }

    return checkIfStreamNeedsRefresh(formStream, connectionStream);
  });

  return { shouldRefreshWarning: recommendedRefreshStreams.length > 0, recommendedRefreshStreams };
};

/**
 * Determines if a schedule is high-frequency (runs more than once per 24 hours)
 * @param scheduleData - The schedule data containing either basicSchedule or cron
 * @param scheduleType - The type of schedule (manual, basic, or cron)
 * @returns true if the schedule runs more than once per 24 hours, false otherwise
 */
const isHighFrequencySchedule = (
  scheduleData?: ConnectionScheduleData,
  scheduleType?: ConnectionScheduleType
): boolean => {
  // Manual schedules have no automated frequency
  if (!scheduleType || scheduleType === ConnectionScheduleType.manual) {
    return false;
  }

  // Handle basic schedules
  if (scheduleType === ConnectionScheduleType.basic && scheduleData?.basicSchedule) {
    const { units, timeUnit } = scheduleData.basicSchedule;

    // Minutes: all are high frequency (less than 24 hours)
    if (timeUnit === "minutes") {
      return true;
    }

    // Hours: high frequency if less than 24
    if (timeUnit === "hours") {
      return units < 24;
    }

    // Days, weeks, months: never high frequency is there any lib(minimum is 24 hours)
    if (timeUnit === "days" || timeUnit === "weeks" || timeUnit === "months") {
      return false;
    }
  }

  // For cron schedules, we would need more complex parsing
  // For MVP, return false (conservative approach)
  // This can be enhanced in Phase 2 with proper cron parsing
  return false;
};

/**
 * Finds streams that are being changed FROM something else TO full_refresh mode
 * @param formSyncCatalog - The form's current sync catalog
 * @param storedSyncCatalog - The stored/original sync catalog
 * @returns Array of streams being changed to full refresh
 */
const findStreamsChangedToFullRefresh = (
  formSyncCatalog: AirbyteCatalogWithAnySchema,
  storedSyncCatalog: AirbyteCatalogWithAnySchema
): AirbyteStreamAndConfiguration[] => {
  const storedStreamsById = createLookupById(storedSyncCatalog);

  return formSyncCatalog.streams.filter((formStream) => {
    if (!formStream.config?.selected || !formStream.stream || !formStream.stream.name) {
      return false;
    }

    const streamId = `${formStream.stream.namespace ?? ""}-${formStream.stream.name}`;
    const storedStream = storedStreamsById[streamId];

    return (
      formStream.config.syncMode === SyncMode.full_refresh && storedStream?.config?.syncMode !== SyncMode.full_refresh
    );
  });
};

/**
 * Determines if a full refresh high-frequency warning should be shown
 * Warning is shown when user attempts to set 2+ streams to full refresh with high-frequency schedule
 * @param params - Object containing form catalog, stored catalog, schedule data, and schedule type
 * @returns Object with shouldWarn boolean and affectedStreamsCount number
 */
export const getFullRefreshHighFrequencyWarningInfo = ({
  formSyncCatalog,
  storedSyncCatalog,
  scheduleData,
  scheduleType,
}: {
  formSyncCatalog: AirbyteCatalog;
  storedSyncCatalog: AirbyteCatalog;
  scheduleData?: ConnectionScheduleData;
  scheduleType?: ConnectionScheduleType;
}): { highFrequencyWarning: boolean; affectedStreams: AirbyteStreamAndConfiguration[] } => {
  // Check if schedule is high-frequency
  const isHighFrequency = isHighFrequencySchedule(scheduleData, scheduleType);

  if (!isHighFrequency) {
    return { highFrequencyWarning: false, affectedStreams: [] };
  }

  // Find streams being changed to full refresh
  const affectedStreams = findStreamsChangedToFullRefresh(formSyncCatalog, storedSyncCatalog);

  // Warn if 1 or more streams are being changed to full refresh
  return {
    highFrequencyWarning: affectedStreams.length > 0,
    affectedStreams,
  };
};

export const analyzeConnectionChanges = ({
  formSyncCatalog,
  storedSyncCatalog,
  scheduleData,
  scheduleType,
  destinationSupportsRefresh,
  isCloudApp = false,
}: {
  formSyncCatalog: AirbyteCatalog;
  storedSyncCatalog: AirbyteCatalog;
  scheduleData?: ConnectionScheduleData;
  scheduleType?: ConnectionScheduleType;
  destinationSupportsRefresh: boolean;
  isCloudApp?: boolean;
}): Partial<ChangesMap> => {
  const changes = {} as Partial<ChangesMap>;

  // Check 1: High-frequency full refresh warnings (Cloud only - no billing in OSS)
  if (isCloudApp) {
    const { highFrequencyWarning, affectedStreams: fullRefreshStreams } = getFullRefreshHighFrequencyWarningInfo({
      formSyncCatalog,
      storedSyncCatalog,
      scheduleData,
      scheduleType,
    });

    if (highFrequencyWarning && fullRefreshStreams.length > 0) {
      const storedStreamsById = createLookupById(storedSyncCatalog);
      changes.fullRefreshHighFrequency = fullRefreshStreams
        .filter(
          (
            stream
          ): stream is AirbyteStreamAndConfiguration & {
            stream: NonNullable<AirbyteStreamAndConfiguration["stream"]>;
          } => !!stream.stream
        )
        .map((stream) => {
          const streamId = `${stream.stream.namespace ?? ""}-${stream.stream.name}`;
          return {
            id: `high-freq-${stream.stream.name}`,
            streamName: stream.stream.name,
            storedConfig: storedStreamsById[streamId],
          };
        });
    }
  }

  // Check 2: Refresh/clear recommendations
  const { shouldRefreshWarning, recommendedRefreshStreams } = getRecommendRefreshStreams(
    formSyncCatalog,
    storedSyncCatalog
  );

  if (shouldRefreshWarning && recommendedRefreshStreams.length > 0) {
    const mode = destinationSupportsRefresh ? "refresh" : "clear";
    changes[mode] = recommendedRefreshStreams
      .filter(
        (
          stream
        ): stream is AirbyteStreamAndConfiguration & {
          stream: NonNullable<AirbyteStreamAndConfiguration["stream"]>;
        } => !!stream.stream
      )
      .map((stream) => ({
        id: `refresh-${stream.stream.name}`,
        streamName: stream.stream.name,
      }));
  }

  return changes;
};

/**
 * Determines what actions to take based on user decisions from the modal
 * @param decisions - User's accept/reject decisions for each warning type
 * @returns Object with skipReset, clearAffectedStreams, and refreshAffectedStreams flags
 */
export const determineConnectionUpdateActions = (
  decisions: UserDecisions,
  changes?: Partial<ChangesMap>
): ConnectionUpdateActions => {
  let skipReset = true;
  let fullRefreshStreamsToRevert: AirbyteStreamAndConfiguration[] = [];

  // If user rejected fullRefreshHighFrequency warning, revert those streams to their stored config
  if (changes?.fullRefreshHighFrequency && decisions.fullRefreshHighFrequency === "reject") {
    fullRefreshStreamsToRevert = changes.fullRefreshHighFrequency
      .map((item) => item.storedConfig)
      .filter((config): config is AirbyteStreamAndConfiguration => config !== undefined);
    skipReset = true;
  }

  // If user accepted clear or refresh, we need to perform a reset (don't skip)
  if (decisions.clear === "accept" || decisions.refresh === "accept") {
    skipReset = false;
  }

  return {
    skipReset,
    ...(fullRefreshStreamsToRevert.length > 0 && { fullRefreshStreamsToRevert }),
  };
};

/**
 * Reverts all configuration changes for streams that were changed to full_refresh
 * Used when user rejects the fullRefreshHighFrequency warning
 * Restores the entire stream configuration to its stored state while preserving other form values
 * @param values - The current form values with potentially modified sync catalog
 * @param streamsToRevert - Array of stream configurations to revert to
 * @returns Updated form values with full stream configuration reverted and other form fields preserved
 */
export const discardFullRefreshChanges = (
  values: ConnectionValues,
  streamsToRevert: AirbyteStreamAndConfiguration[]
): ConnectionValues => {
  const streamsById = createLookupById({ streams: streamsToRevert } as AirbyteCatalog);

  return {
    ...values,
    syncCatalog: {
      ...values.syncCatalog,
      streams: values.syncCatalog.streams.map((stream) => {
        const key = `${stream.stream?.namespace ?? ""}-${stream.stream?.name}`;
        return streamsById[key] ?? stream;
      }),
    },
  };
};
