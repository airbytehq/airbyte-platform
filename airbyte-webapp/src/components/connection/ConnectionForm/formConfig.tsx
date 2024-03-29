import { useMemo } from "react";
import { FieldArrayWithId } from "react-hook-form";

import { NormalizationType } from "area/connection/types";
import { isDbtTransformation, isNormalizationTransformation } from "area/connection/utils";
import { useCurrentWorkspace } from "core/api";
import {
  AirbyteCatalog,
  DestinationSyncMode,
  OperationCreate,
  SyncMode,
  ActorDefinitionVersionRead,
  ConnectionScheduleData,
  ConnectionScheduleType,
  Geography,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  OperationRead,
  SchemaChangeBackfillPreference,
} from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { ConnectionOrPartialConnection } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { analyzeSyncCatalogBreakingChanges } from "./calculateInitialCatalog";
import {
  BASIC_FREQUENCY_DEFAULT_VALUE,
  SOURCE_SPECIFIC_FREQUENCY_DEFAULT,
} from "./ScheduleFormField/useBasicFrequencyDropdownData";
import { createConnectionValidationSchema } from "./schema";
import { DbtOperationRead } from "../TransformationForm";

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
  normalization?: NormalizationType;
  transformations?: OperationRead[];
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

/**
 * get transformation operations only
 * @param operations
 */
export const getInitialTransformations = (operations: OperationRead[]): DbtOperationRead[] =>
  operations?.filter(isDbtTransformation) ?? [];

/**
 * get normalization initial normalization type
 * @param operations
 * @param isEditMode
 */
export const getInitialNormalization = (
  operations?: Array<OperationRead | OperationCreate>,
  isEditMode?: boolean
): NormalizationType => {
  const initialNormalization =
    operations?.find(isNormalizationTransformation)?.operatorConfiguration?.normalization?.option;

  return initialNormalization
    ? NormalizationType[initialNormalization]
    : isEditMode
    ? NormalizationType.raw
    : NormalizationType.basic;
};

// react-hook-form form values type for the connection form.
export const useInitialFormValues = (
  connection: ConnectionOrPartialConnection,
  destDefinitionVersion: ActorDefinitionVersionRead,
  isEditMode?: boolean
): FormConnectionFormValues => {
  const workspace = useCurrentWorkspace();
  const { catalogDiff, syncCatalog, schemaChange } = connection;
  const useSimpliedCreation = useExperiment("connection.simplifiedCreation", false);

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
      ...{
        ...(destDefinitionVersion.supportsDbt && {
          normalization: getInitialNormalization(connection.operations ?? [], isEditMode),
        }),
      },
      ...{
        ...(destDefinitionVersion.supportsDbt && {
          transformations: getInitialTransformations(connection.operations ?? []),
        }),
        syncCatalog: analyzeSyncCatalogBreakingChanges(syncCatalog, catalogDiff, schemaChange),
      },
      notifySchemaChanges: connection.notifySchemaChanges ?? useSimpliedCreation,
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
    connection.operations,
    connection.notifySchemaChanges,
    connection.backfillPreference,
    defaultNonBreakingChangesPreference,
    workspace.defaultGeography,
    destDefinitionVersion.supportsDbt,
    isEditMode,
    syncCatalog,
    catalogDiff,
    schemaChange,
    useSimpliedCreation,
  ]);
};
