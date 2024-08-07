import { useMemo } from "react";
import { FieldArrayWithId } from "react-hook-form";

import { useCurrentWorkspace, useGetDestinationDefinitionSpecification } from "core/api";
import {
  AirbyteCatalog,
  DestinationSyncMode,
  SyncMode,
  ConnectionScheduleData,
  ConnectionScheduleType,
  Geography,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  SchemaChangeBackfillPreference,
} from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { ConnectionFormMode, ConnectionOrPartialConnection } from "hooks/services/ConnectionForm/ConnectionFormService";

import { analyzeSyncCatalogBreakingChanges } from "./calculateInitialCatalog";
import { pruneUnsupportedModes, replicateSourceModes } from "./preferredSyncModes";
import {
  BASIC_FREQUENCY_DEFAULT_VALUE,
  SOURCE_SPECIFIC_FREQUENCY_DEFAULT,
} from "./ScheduleFormField/useBasicFrequencyDropdownData";
import { createConnectionValidationSchema } from "./schema";
import { updateStreamSyncMode } from "../syncCatalog/SyncCatalog/updateStreamSyncMode";

/**
 * react-hook-form form values type for the connection form
 */
export interface FormConnectionFormValues {
  name: string;
  scheduleType: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat?: string;
  prefix: string;
  nonBreakingChangesPreference?: NonBreakingChangesPreference;
  geography?: Geography;
  syncCatalog: AirbyteCatalog;
  notifySchemaChanges?: boolean;
  backfillPreference?: SchemaChangeBackfillPreference;
}

/**
 * type shortcut for syncCatalog.streams[field]
 * react-hook-form useFieldArray returns an array of fields({ stream, config }) with id
 */
export type SyncStreamFieldWithId = FieldArrayWithId<FormConnectionFormValues, "syncCatalog.streams", "id">;

/**
 * supported sync modes for the sync catalog row
 */
export const SUPPORTED_MODES: Array<[SyncMode, DestinationSyncMode]> = [
  [SyncMode.incremental, DestinationSyncMode.append_dedup],
  [SyncMode.full_refresh, DestinationSyncMode.overwrite],
  [SyncMode.incremental, DestinationSyncMode.append],
  [SyncMode.full_refresh, DestinationSyncMode.append],
  [SyncMode.full_refresh, DestinationSyncMode.overwrite_dedup],
];

/**
 * useConnectionValidationSchema with additional arguments
 */
export const useConnectionValidationSchema = () => {
  const allowSubOneHourCronExpressions = useFeature(FeatureItem.AllowSyncSubOneHourCronExpressions);
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  return useMemo(
    () => createConnectionValidationSchema(allowSubOneHourCronExpressions, allowAutoDetectSchema),
    [allowAutoDetectSchema, allowSubOneHourCronExpressions]
  );
};

// react-hook-form form values type for the connection form.
export const useInitialFormValues = (
  connection: ConnectionOrPartialConnection,
  mode: ConnectionFormMode
): FormConnectionFormValues => {
  const workspace = useCurrentWorkspace();
  const destDefinitionSpecification = useGetDestinationDefinitionSpecification(connection.destination.destinationId);
  const { catalogDiff, syncCatalog, schemaChange } = connection;
  const { notificationSettings } = useCurrentWorkspace();
  const supportedSyncModes: SyncMode[] = useMemo(() => {
    const foundModes = new Set<SyncMode>();
    for (let i = 0; i < connection.syncCatalog.streams.length; i++) {
      const stream = connection.syncCatalog.streams[i];
      stream.stream?.supportedSyncModes?.forEach((mode) => foundModes.add(mode));
    }
    return Array.from(foundModes);
  }, [connection.syncCatalog.streams]);

  if (mode === "create") {
    const availableModes = pruneUnsupportedModes(
      replicateSourceModes,
      supportedSyncModes,
      destDefinitionSpecification.supportedDestinationSyncModes
    );

    // when creating, apply the first applicable `replicateSourceModes` entry to each stream
    syncCatalog.streams.forEach((airbyteStream) => {
      for (let i = 0; i < availableModes.length; i++) {
        const [syncMode, destinationSyncMode] = availableModes[i];

        if (airbyteStream?.stream && airbyteStream?.config) {
          if (!airbyteStream.stream?.supportedSyncModes?.includes(syncMode)) {
            continue;
          }

          airbyteStream.config = updateStreamSyncMode(airbyteStream.stream, airbyteStream.config, {
            syncMode,
            destinationSyncMode,
          });
          break;
        }
      }
    });
  }

  const defaultNonBreakingChangesPreference = NonBreakingChangesPreference.propagate_columns;

  return useMemo(() => {
    const initialValues: FormConnectionFormValues = {
      name: connection.name ?? `${connection.source.name} → ${connection.destination.name}`,
      scheduleType: connection.scheduleType ?? ConnectionScheduleType.basic,
      scheduleData: connection.scheduleData
        ? connection.scheduleData
        : connection.scheduleType === ConnectionScheduleType.manual
        ? undefined
        : {
            basicSchedule:
              SOURCE_SPECIFIC_FREQUENCY_DEFAULT[connection.source?.sourceDefinitionId] ?? BASIC_FREQUENCY_DEFAULT_VALUE,
          },
      namespaceDefinition: connection.namespaceDefinition || NamespaceDefinitionType.destination,
      // set connection's namespaceFormat if it's defined, otherwise there is no need to set it
      ...{
        ...(connection.namespaceFormat && {
          namespaceFormat: connection.namespaceFormat,
        }),
      },
      ...{
        prefix: connection.prefix ?? "",
      },
      nonBreakingChangesPreference: connection.nonBreakingChangesPreference ?? defaultNonBreakingChangesPreference,
      geography: connection.geography || workspace.defaultGeography || "auto",
      syncCatalog: analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, schemaChange),
      notifySchemaChanges:
        connection.notifySchemaChanges ??
        (notificationSettings?.sendOnConnectionUpdate?.notificationType &&
          notificationSettings.sendOnConnectionUpdate.notificationType.length > 0),
      backfillPreference: connection.backfillPreference ?? SchemaChangeBackfillPreference.disabled,
    };

    return initialValues;
  }, [
    connection.name,
    connection.source.name,
    connection.source?.sourceDefinitionId,
    connection.destination.name,
    connection.scheduleType,
    connection.scheduleData,
    connection.namespaceDefinition,
    connection.namespaceFormat,
    connection.prefix,
    connection.nonBreakingChangesPreference,
    connection.geography,
    connection.notifySchemaChanges,
    connection.backfillPreference,
    defaultNonBreakingChangesPreference,
    workspace.defaultGeography,
    syncCatalog,
    catalogDiff,
    schemaChange,
    notificationSettings?.sendOnConnectionUpdate,
  ]);
};
