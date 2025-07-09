import { useMemo } from "react";
import { FieldArrayWithId } from "react-hook-form";

import { useCurrentWorkspace, useGetDataplaneGroup, useGetDestinationDefinitionSpecification } from "core/api";
import {
  AirbyteCatalog,
  DestinationSyncMode,
  SyncMode,
  ConnectionScheduleData,
  ConnectionScheduleType,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  SchemaChangeBackfillPreference,
  Tag,
  AirbyteStreamAndConfiguration,
  AirbyteStream,
} from "core/api/types/AirbyteClient";
import { ConnectionFormMode, ConnectionOrPartialConnection } from "hooks/services/ConnectionForm/ConnectionFormService";

import { analyzeSyncCatalogBreakingChanges } from "./calculateInitialCatalog";
import { pruneUnsupportedModes, replicateSourceModes } from "./preferredSyncModes";
import {
  BASIC_FREQUENCY_DEFAULT_VALUE,
  SOURCE_SPECIFIC_FREQUENCY_DEFAULT,
} from "./ScheduleFormField/useBasicFrequencyDropdownData";
import { updateStreamSyncMode } from "../SyncCatalogTable/utils";

type AirbyteCatalogWithAnySchema = Omit<AirbyteCatalog, "streams"> & {
  streams: Array<
    Omit<AirbyteStreamAndConfiguration, "stream"> & {
      stream?: Omit<AirbyteStream, "jsonSchema"> & {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        jsonSchema?: Record<string, any>;
      };
    }
  >;
};

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
  geography?: string;
  syncCatalog: AirbyteCatalogWithAnySchema;
  notifySchemaChanges?: boolean;
  backfillPreference?: SchemaChangeBackfillPreference;
  tags?: Tag[];
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

// react-hook-form form values type for the connection form.
export const useInitialFormValues = (
  connection: ConnectionOrPartialConnection,
  mode: ConnectionFormMode,
  destinationSupportsFileTransfer?: boolean
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
  const { getDataplaneGroup } = useGetDataplaneGroup();

  // Handle file-based streams selection and file inclusion based on destination file transfer support.
  if (destinationSupportsFileTransfer !== undefined) {
    syncCatalog.streams.forEach((airbyteStream) => {
      const isFileBased = airbyteStream?.stream?.isFileBased;
      const config = airbyteStream?.config;
      if (!isFileBased || !config) {
        return;
      }

      if (!destinationSupportsFileTransfer) {
        config.selected = false;
        config.includeFiles = false;
        return;
      }

      config.includeFiles = true;
    });
  }

  return useMemo(() => {
    const dataplaneGroupId = workspace.dataplaneGroupId;
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
      geography: getDataplaneGroup(dataplaneGroupId)?.name ?? "auto",
      syncCatalog: analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, schemaChange),
      notifySchemaChanges:
        connection.notifySchemaChanges ??
        (notificationSettings?.sendOnConnectionUpdate?.notificationType &&
          notificationSettings.sendOnConnectionUpdate.notificationType.length > 0),
      backfillPreference: connection.backfillPreference ?? SchemaChangeBackfillPreference.disabled,
      tags: connection.tags ?? [],
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
    connection.notifySchemaChanges,
    connection.backfillPreference,
    connection.tags,
    defaultNonBreakingChangesPreference,
    workspace.dataplaneGroupId,
    syncCatalog,
    catalogDiff,
    schemaChange,
    notificationSettings?.sendOnConnectionUpdate,
    getDataplaneGroup,
  ]);
};
