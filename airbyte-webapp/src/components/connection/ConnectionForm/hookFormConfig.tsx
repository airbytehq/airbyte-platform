import { useMemo } from "react";
import * as yup from "yup";

import { NormalizationType } from "area/connection/types";
import { validateCronExpression, validateCronFrequencyOneHourOrMore } from "area/connection/utils";
import { useCurrentWorkspace } from "core/api";
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
import { ConnectionOrPartialConnection } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useConnectionHookFormService } from "hooks/services/ConnectionForm/ConnectionHookFormService";
import { useExperiment } from "hooks/services/Experiment";

import { getInitialNormalization, getInitialTransformations } from "./formConfig";
import { BASIC_FREQUENCY_DEFAULT_VALUE } from "./ScheduleHookFormField/useBasicFrequencyDropdownDataHookForm";
import { dbtOperationReadOrCreateSchema } from "../TransformationHookForm";

export type ConnectionHookFormMode = "create" | "edit" | "readonly";

/**
 * react-hook-form form values type for the connection form.
 * copied from
 * @see FormikConnectionFormValues
 */
export interface HookFormConnectionFormValues {
  name?: string;
  // don't know why scheduleType was optional previously, since it's required in ali request
  scheduleType: ConnectionScheduleType;
  // previously we set it to undefined if scheduleType is 'manual', so we can remove 'null'
  scheduleData?: ConnectionScheduleData;
  // this one also was optional
  namespaceDefinition: NamespaceDefinitionType;
  // this one fully depends on namespaceDefinition
  namespaceFormat?: string;
  prefix?: string;
  nonBreakingChangesPreference?: NonBreakingChangesPreference | null;
  geography?: Geography;
  // surprisingly but seems like we didn't handle this fields in the schema form, but why?
  normalization?: NormalizationType;
  transformations?: OperationRead[];

  // syncCatalog: SyncSchema;
  // syncCatalog: {
  //   streams: Array<{
  //     id?: string;
  //     // how to override the type?
  //     stream?: Omit<AirbyteStream, "name"> & { name?: string };
  //     config?: AirbyteStreamConfiguration;
  //   }>;
  // };
}

/**
 * yup schema for the schedule data
 * @param allowSubOneHourCronExpressions
 */
const getScheduleDataSchema = (allowSubOneHourCronExpressions: boolean) =>
  yup.mixed().when("scheduleType", (scheduleType) => {
    if (scheduleType === ConnectionScheduleType.manual) {
      return yup.mixed<ConnectionScheduleData>().notRequired();
    }

    if (scheduleType === ConnectionScheduleType.basic) {
      return yup.object({
        basicSchedule: yup
          .object({
            units: yup.number().required("form.empty.error"),
            timeUnit: yup.string().required("form.empty.error"),
          })
          .defined("form.empty.error"),
      });
    }

    return yup.object({
      cron: yup
        .object({
          cronExpression: yup
            .string()
            .trim()
            .required("form.empty.error")
            .test("validCron", (value, { createError, path }) => {
              const validation = validateCronExpression(value);
              return (
                validation.isValid ||
                createError({
                  path,
                  message: validation.message ?? "form.cronExpression.invalid",
                })
              );
            })
            .test(
              "validCronFrequency",
              "form.cronExpression.underOneHourNotAllowed",
              (expression) => allowSubOneHourCronExpressions || validateCronFrequencyOneHourOrMore(expression)
            ),
          cronTimeZone: yup.string().required("form.empty.error"),
        })
        .defined("form.empty.error"),
    });
  });

// const streamAndConfigurationSchema = yup.object({
//   id: yup
//     .string()
//     // This is required to get rid of id fields we are using to detect stream for edition
//     .when("$isRequest", (isRequest: boolean, schema: yup.StringSchema) => (isRequest ? schema.strip(true) : schema)),
//   stream: yup
//     .object({
//       name: yup.string().required("form.empty.error"),
//     })
//     .optional(),
//   config: yup
//     .object({
//       selected: yup.boolean(),
//       syncMode: yup.string(),
//       destinationSyncMode: yup.string(),
//       primaryKey: yup.array().of(yup.array().of(yup.string())),
//       cursorField: yup.array().of(yup.string()),
//     })
//     .test({
//       message: "form.empty.error",
// eslint-disable-next-line jest/no-commented-out-tests
//       test(value) {
//         if (!value.selected) {
//           return true;
//         }
//
//         const errors: yup.ValidationError[] = [];
//         const pathRoot = "syncCatalog";
//
//         // it's possible that primaryKey array is always present
//         // however yup couldn't determine type correctly even with .required() call
//         if (DestinationSyncMode.append_dedup === value.destinationSyncMode && value.primaryKey?.length === 0) {
//           errors.push(
//             this.createError({
//               message: "connectionForm.primaryKey.required",
//               path: `${pathRoot}.streams[${this.parent.id}].config.primaryKey`,
//             })
//           );
//         }
//
//         // it's possible that cursorField array is always present
//         // however yup couldn't determine type correctly even with .required() call
//         if (
//           SyncMode.incremental === value.syncMode &&
//           !this.parent.stream.sourceDefinedCursor &&
//           value.cursorField?.length === 0
//         ) {
//           errors.push(
//             this.createError({
//               message: "connectionForm.cursorField.required",
//               path: `${pathRoot}.streams[${this.parent.id}].config.cursorField`,
//             })
//           );
//         }
//
//         return errors.length > 0 ? new yup.ValidationError(errors) : true;
//       },
//     }),
// });

/**
 * yup schema for the sync catalog
 */
// const syncCatalogSchema = yup.object({
//   streams: yup
//     .array()
//     .of(streamAndConfigurationSchema)
//     .test(
//       "syncCatalog.streams.required",
//       "connectionForm.streams.required",
//       (streams) => streams?.some(({ config }) => !!config.selected) ?? false
//     ),
// });

export const namespaceDefinitionSchema = yup
  .mixed<NamespaceDefinitionType>()
  .oneOf(Object.values(NamespaceDefinitionType));

export const namespaceFormatSchema = yup.string().when("namespaceDefinition", {
  is: NamespaceDefinitionType.customformat,
  then: yup.string().trim().required("form.empty.error"),
});

/**
 * generate yup schema for the create connection form
 * @param mode
 * @param allowSubOneHourCronExpressions
 * @param allowAutoDetectSchema
 */
const createConnectionValidationSchema = (
  mode: ConnectionHookFormMode,
  allowSubOneHourCronExpressions: boolean,
  allowAutoDetectSchema: boolean
) =>
  yup
    .object({
      // The connection name during Editing is handled separately from the form
      name: mode === "create" ? yup.string().required("form.empty.error") : yup.string().notRequired(),
      // scheduleType can't de 'undefined', make it required()
      scheduleType: yup.mixed<ConnectionScheduleType>().oneOf(Object.values(ConnectionScheduleType)).required(),
      scheduleData: getScheduleDataSchema(allowSubOneHourCronExpressions),
      namespaceDefinition: namespaceDefinitionSchema.required("form.empty.error"),
      namespaceFormat: namespaceFormatSchema,
      prefix: yup.string().optional(),
      nonBreakingChangesPreference: allowAutoDetectSchema
        ? yup.mixed().oneOf(Object.values(NonBreakingChangesPreference)).required("form.empty.error")
        : yup.mixed().notRequired(),
      // make "geography" optional since same as in interface
      geography: yup.mixed<Geography>().oneOf(Object.values(Geography)).optional(),
      // syncCatalog: syncCatalogSchema,
      normalization: yup.mixed<NormalizationType>().oneOf(Object.values(NormalizationType)).optional(),
      transformations: yup.array().of(dbtOperationReadOrCreateSchema).optional(),
    })
    .noUnknown();

/**
 * useConnectionValidationSchema with additional arguments
 */
export const useConnectionHookFormValidationSchema = () => {
  const allowSubOneHourCronExpressions = useFeature(FeatureItem.AllowSyncSubOneHourCronExpressions);
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);
  const { mode } = useConnectionHookFormService();

  return useMemo(
    () => createConnectionValidationSchema(mode, allowSubOneHourCronExpressions, allowAutoDetectSchema),
    [allowAutoDetectSchema, allowSubOneHourCronExpressions, mode]
  );
};

// react-hook-form form values type for the connection form.
export const useInitialHookFormValues = (
  connection: ConnectionOrPartialConnection,
  destDefinitionVersion: ActorDefinitionVersionRead,
  isNotCreateMode?: boolean
  // destDefinitionSpecification: DestinationDefinitionSpecificationRead,
): HookFormConnectionFormValues => {
  const autoPropagationEnabled = useExperiment("autopropagation.enabled", false);
  const workspace = useCurrentWorkspace();
  // const { catalogDiff } = connection;

  const defaultNonBreakingChangesPreference = autoPropagationEnabled
    ? NonBreakingChangesPreference.propagate_columns
    : NonBreakingChangesPreference.ignore;

  // used to determine if we should calculate optimal sync mode
  // const newStreamDescriptors = catalogDiff?.transforms
  //   .filter((transform) => transform.transformType === "add_stream")
  //   .map((stream) => stream.streamDescriptor);

  // used to determine if we need to clear any primary keys or cursor fields that were removed
  // const streamTransformsWithBreakingChange = useMemo(() => {
  //   if (connection.schemaChange === SchemaChange.breaking) {
  //     return catalogDiff?.transforms.filter((streamTransform) => {
  //       if (streamTransform.transformType === "update_stream") {
  //         return streamTransform.updateStream?.filter((fieldTransform) => fieldTransform.breaking === true);
  //       }
  //       return false;
  //     });
  //   }
  //   return undefined;
  // }, [catalogDiff?.transforms, connection]);

  // will be used later on
  // const initialSchema = useMemo(
  //   () =>
  //     calculateInitialCatalog(
  //       connection.syncCatalog,
  //       destDefinitionSpecification?.supportedDestinationSyncModes || [],
  //       streamTransformsWithBreakingChange,
  //       isNotCreateMode,
  //       newStreamDescriptors
  //     ),
  //   [
  //     streamTransformsWithBreakingChange,
  //     connection.syncCatalog,
  //     destDefinitionSpecification?.supportedDestinationSyncModes,
  //     isNotCreateMode,
  //     newStreamDescriptors,
  //   ]
  // );

  return useMemo(() => {
    const initialValues: HookFormConnectionFormValues = {
      name: connection.name ?? `${connection.source.name} → ${connection.destination.name}`,
      scheduleType: connection.scheduleType ?? ConnectionScheduleType.basic,
      scheduleData: connection.scheduleData ?? { basicSchedule: BASIC_FREQUENCY_DEFAULT_VALUE },
      namespaceDefinition: connection.namespaceDefinition || NamespaceDefinitionType.destination,
      // set connection's namespaceFormat if it's defined, otherwise there is no need to set it (that's why all connections has ${SOURCE_NAMESPACE} string)
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
          normalization: getInitialNormalization(connection.operations ?? [], isNotCreateMode),
        }),
      },
      ...{
        ...(destDefinitionVersion.supportsDbt && {
          transformations: getInitialTransformations(connection.operations ?? []),
        }),
        // syncCatalog: initialSchema,
      },
    };

    return initialValues;
  }, [
    connection.name,
    connection.source.name,
    connection.destination.name,
    connection.geography,
    connection.namespaceDefinition,
    connection.namespaceFormat,
    connection.nonBreakingChangesPreference,
    connection.operations,
    connection.prefix,
    connection.scheduleData,
    connection.scheduleType,
    destDefinitionVersion.supportsDbt,
    isNotCreateMode,
    defaultNonBreakingChangesPreference,
    workspace,
  ]);
};
