import { useMemo } from "react";
import { FieldArrayWithId } from "react-hook-form";

import { NormalizationType } from "area/connection/types";
import { isDbtTransformation, isNormalizationTransformation } from "area/connection/utils";
import { useCurrentWorkspace } from "core/api";
import {
  AirbyteCatalog,
  DestinationDefinitionSpecificationRead,
  DestinationSyncMode,
  OperationCreate,
  SchemaChange,
  SyncMode,
} from "core/api/types/AirbyteClient";
import {
  ActorDefinitionVersionRead,
  ConnectionScheduleData,
  ConnectionScheduleType,
  Geography,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  OperationRead,
} from "core/request/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import {
  ConnectionOrPartialConnection,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { calculateInitialCatalogHookForm } from "./calculateInitialCatalogHookForm";
import { BASIC_FREQUENCY_DEFAULT_VALUE } from "./ScheduleHookFormField/useBasicFrequencyDropdownDataHookForm";
import { createConnectionValidationSchema } from "./schema";
import { DbtOperationRead } from "../TransformationHookForm";

/**
 * react-hook-form form values type for the connection form
 */
export interface HookFormConnectionFormValues {
  name?: string;
  scheduleType: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat?: string;
  prefix?: string;
  nonBreakingChangesPreference?: NonBreakingChangesPreference;
  geography?: Geography;
  normalization?: NormalizationType;
  transformations?: OperationRead[];
  syncCatalog: AirbyteCatalog;
}

/**
 * type shortcut for syncCatalog.streams[field]
 * react-hook-form useFieldArray returns an array of fields({ stream, config }) with id
 */
export type SyncStreamFieldWithId = FieldArrayWithId<HookFormConnectionFormValues, "syncCatalog.streams", "id">;

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
export const useConnectionHookFormValidationSchema = () => {
  const allowSubOneHourCronExpressions = useFeature(FeatureItem.AllowSyncSubOneHourCronExpressions);
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);
  const { mode } = useConnectionFormService();

  return useMemo(
    () => createConnectionValidationSchema(mode, allowSubOneHourCronExpressions, allowAutoDetectSchema),
    [allowAutoDetectSchema, allowSubOneHourCronExpressions, mode]
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
export const useInitialHookFormValues = (
  connection: ConnectionOrPartialConnection,
  destDefinitionVersion: ActorDefinitionVersionRead,
  destDefinitionSpecification: DestinationDefinitionSpecificationRead,
  isEditMode?: boolean
): HookFormConnectionFormValues => {
  const autoPropagationEnabled = useExperiment("autopropagation.enabled", false);
  const workspace = useCurrentWorkspace();
  const { catalogDiff } = connection;

  const defaultNonBreakingChangesPreference = autoPropagationEnabled
    ? NonBreakingChangesPreference.propagate_columns
    : NonBreakingChangesPreference.ignore;

  // used to determine if we should calculate optimal sync mode
  const newStreamDescriptors = catalogDiff?.transforms
    .filter((transform) => transform.transformType === "add_stream")
    .map((stream) => stream.streamDescriptor);

  // used to determine if we need to clear any primary keys or cursor fields that were removed
  const streamTransformsWithBreakingChange = useMemo(() => {
    if (connection.schemaChange === SchemaChange.breaking) {
      return catalogDiff?.transforms.filter((streamTransform) => {
        if (streamTransform.transformType === "update_stream") {
          return streamTransform.updateStream?.filter((fieldTransform) => fieldTransform.breaking === true);
        }
        return false;
      });
    }
    return undefined;
  }, [catalogDiff?.transforms, connection]);

  const initialSchema = useMemo(
    () =>
      calculateInitialCatalogHookForm(
        connection.syncCatalog,
        destDefinitionSpecification?.supportedDestinationSyncModes || [],
        streamTransformsWithBreakingChange,
        isEditMode,
        newStreamDescriptors
      ),
    [
      connection.syncCatalog,
      destDefinitionSpecification?.supportedDestinationSyncModes,
      streamTransformsWithBreakingChange,
      isEditMode,
      newStreamDescriptors,
    ]
  );

  return useMemo(() => {
    const initialValues: HookFormConnectionFormValues = {
      // set name field
      ...(isEditMode
        ? {}
        : {
            name: connection.name ?? `${connection.source.name} → ${connection.destination.name}`,
          }),
      scheduleType: connection.scheduleType ?? ConnectionScheduleType.basic,
      // set scheduleData field if it's defined, otherwise there is no need to set it
      ...{
        ...(connection.scheduleData
          ? { scheduleData: connection.scheduleData }
          : connection.scheduleType === ConnectionScheduleType.manual
          ? {}
          : {
              scheduleData: { basicSchedule: BASIC_FREQUENCY_DEFAULT_VALUE },
            }),
      },
      namespaceDefinition: connection.namespaceDefinition || NamespaceDefinitionType.destination,
      // set connection's namespaceFormat if it's defined, otherwise there is no need to set it
      ...{
        ...(connection.namespaceFormat && {
          namespaceFormat: connection.namespaceFormat,
        }),
      },
      // prefix is not required, so we don't need to set it to empty string if it's not defined in connection
      ...{
        ...(connection.prefix && {
          prefix: connection.prefix,
        }),
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
        syncCatalog: initialSchema,
      },
    };

    return initialValues;
  }, [
    connection.name,
    connection.source.name,
    connection.destination.name,
    connection.scheduleType,
    connection.scheduleData,
    connection.namespaceDefinition,
    connection.namespaceFormat,
    connection.prefix,
    connection.nonBreakingChangesPreference,
    connection.geography,
    connection.operations,
    defaultNonBreakingChangesPreference,
    workspace.defaultGeography,
    destDefinitionVersion.supportsDbt,
    isEditMode,
    initialSchema,
  ]);
};
