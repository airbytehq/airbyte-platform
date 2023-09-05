import { useMemo } from "react";
import * as yup from "yup";

import { validateCronExpression, validateCronFrequencyOneHourOrMore } from "area/connection/utils";
import {
  ConnectionScheduleData,
  ConnectionScheduleType,
  Geography,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
} from "core/request/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useConnectionHookFormService } from "hooks/services/ConnectionForm/ConnectionHookFormService";

export type ConnectionHookFormMode = "create" | "edit" | "readonly";

/**
 * react-hook-form form values type for the connection form.
 * copied from
 * @see FormikConnectionFormValues
 */
export interface HookFormConnectionFormValues {
  name?: string;
  scheduleType?: ConnectionScheduleType | null;
  scheduleData?: ConnectionScheduleData | null;
  nonBreakingChangesPreference?: NonBreakingChangesPreference | null;
  prefix: string;
  namespaceDefinition?: NamespaceDefinitionType;
  namespaceFormat?: string;
  geography?: Geography;
  // syncCatalog: SyncSchema;
  // syncCatalog: {
  //   streams: Array<{
  //     id?: string;
  //     // how to override the type?
  //     stream?: Omit<AirbyteStream, "name"> & { name?: string };
  //     config?: AirbyteStreamConfiguration;
  //   }>;
  // };
  // surprisingly but seems like we didn't handle this fields in the schema form, but why?
  // normalization?: NormalizationType;
  // transformations?: OperationRead[];
}

/**
 * yup schema for the schedule data
 * @param allowSubOneHourCronExpressions
 */
const getScheduleDataSchema = (allowSubOneHourCronExpressions: boolean) =>
  yup.mixed().when("scheduleType", (scheduleType) => {
    if (scheduleType === ConnectionScheduleType.basic) {
      return yup.object({
        basicSchedule: yup
          .object({
            units: yup.number().required("form.empty.error"),
            timeUnit: yup.string().required("form.empty.error"),
          })
          .defined("form.empty.error"),
      });
    } else if (scheduleType === ConnectionScheduleType.manual) {
      return yup.mixed().notRequired();
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
      geography: yup.mixed<Geography>().oneOf(Object.values(Geography)),
      scheduleType: yup.mixed<ConnectionScheduleType>().oneOf(Object.values(ConnectionScheduleType)),
      scheduleData: getScheduleDataSchema(allowSubOneHourCronExpressions),
      nonBreakingChangesPreference: allowAutoDetectSchema
        ? yup.mixed().oneOf(Object.values(NonBreakingChangesPreference)).required("form.empty.error")
        : yup.mixed().notRequired(),
      namespaceDefinition: yup
        .mixed<NamespaceDefinitionType>()
        .oneOf(Object.values(NamespaceDefinitionType))
        .required("form.empty.error"),
      namespaceFormat: yup.string().when("namespaceDefinition", {
        is: NamespaceDefinitionType.customformat,
        then: yup.string().trim().required("form.empty.error"),
      }),
      prefix: yup.string().default(""),
      // syncCatalog: syncCatalogSchema,
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
