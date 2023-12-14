import * as yup from "yup";
import { SchemaOf } from "yup";

import { NormalizationType } from "area/connection/types";
import { validateCronExpression, validateCronFrequencyOneHourOrMore } from "area/connection/utils";
import {
  AirbyteStreamAndConfiguration,
  AirbyteStream,
  AirbyteStreamConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  DestinationSyncMode,
  Geography,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  SyncMode,
} from "core/api/types/AirbyteClient";
import { ConnectionFormMode } from "hooks/services/ConnectionForm/ConnectionFormService";

import { dbtOperationReadOrCreateSchema } from "../TransformationForm";

/**
 * yup schema for the schedule data
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

/**
 * yup schema for the stream
 */
const streamSchema: SchemaOf<AirbyteStream> = yup.object({
  name: yup.string().required("form.empty.error"),
  supportedSyncModes: yup
    .array()
    .of(yup.mixed().oneOf(Object.values(SyncMode)))
    .required("form.empty.error"),
  jsonSchema: yup.object().optional(),
  sourceDefinedCursor: yup.boolean().optional(),
  defaultCursorField: yup.array().of(yup.string()).optional(),
  sourceDefinedPrimaryKey: yup.array().of(yup.array().of(yup.string())).optional(),
  namespace: yup.string().optional(),
});

/**
 * yup schema for the stream configuration
 */
const streamConfigSchema: SchemaOf<AirbyteStreamConfiguration> = yup.object({
  syncMode: yup.mixed<SyncMode>().oneOf(Object.values(SyncMode)).required("form.empty.error"),
  destinationSyncMode: yup
    .mixed<DestinationSyncMode>()
    .oneOf(Object.values(DestinationSyncMode))
    .required("form.empty.error"),
  cursorField: yup.array().of(yup.string()).optional(),
  selected: yup.boolean().optional(),
  suggested: yup.boolean().optional(),
  fieldSelectionEnabled: yup.boolean().optional(),
  selectedFields: yup
    .array()
    .of(yup.object({ fieldPath: yup.array().of(yup.string().optional()).optional() }))
    .optional(),
  aliasName: yup.string().optional(),
  primaryKey: yup.array().of(yup.array().of(yup.string())).optional(),
});

export const streamAndConfigurationSchema: SchemaOf<AirbyteStreamAndConfiguration> = yup.object({
  stream: streamSchema.required(),
  config: streamConfigSchema
    .test({
      message: "form.empty.error",
      test(value) {
        if (!value.selected) {
          return true;
        }

        const errors: yup.ValidationError[] = [];
        const pathRoot = "syncCatalog";

        // it's possible that primaryKey array is always present
        // however yup couldn't determine type correctly even with .required() call

        if (DestinationSyncMode.append_dedup === value.destinationSyncMode && value.primaryKey?.length === 0) {
          errors.push(
            this.createError({
              message: "connectionForm.primaryKey.required",
              path: `${pathRoot}.streams[${this.parent.stream?.name}_${this.parent.stream?.namespace}].config.primaryKey`,
            })
          );
        }

        // it's possible that cursorField array is always present
        // however yup couldn't determine type correctly even with .required() call
        if (
          SyncMode.incremental === value.syncMode &&
          !this.parent.stream.sourceDefinedCursor &&
          value.cursorField?.length === 0
        ) {
          errors.push(
            this.createError({
              message: "connectionForm.cursorField.required",
              path: `${pathRoot}.streams[${this.parent.stream?.name}_${this.parent.stream?.namespace}].config.cursorField`,
            })
          );
        }

        return errors.length > 0 ? new yup.ValidationError(errors) : true;
      },
    })
    .required(),
});

/**
 * yup schema for the sync catalog
 */
const syncCatalogSchema = yup.object({
  streams: yup
    .array()
    .of(streamAndConfigurationSchema)
    .test(
      "syncCatalog.streams.required",
      "connectionForm.streams.required",
      (streams) => streams?.some(({ config }) => !!config?.selected) ?? false
    ),
});

/**
 * yup schema for the namespace definition
 */
export const namespaceDefinitionSchema = yup
  .mixed<NamespaceDefinitionType>()
  .oneOf(Object.values(NamespaceDefinitionType));

/**
 * yup schema for the namespace format
 */
export const namespaceFormatSchema = yup.string().when("namespaceDefinition", {
  is: NamespaceDefinitionType.customformat,
  then: yup.string().trim().required("form.empty.error"),
});

/**
 * generate yup schema for the create connection form
 */
export const createConnectionValidationSchema = (
  mode: ConnectionFormMode,
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
      geography: yup.mixed<Geography>().oneOf(Object.values(Geography)).optional(),
      normalization: yup.mixed<NormalizationType>().oneOf(Object.values(NormalizationType)).optional(),
      transformations: yup.array().of(dbtOperationReadOrCreateSchema).optional(),
      syncCatalog: syncCatalogSchema,
    })
    .noUnknown();
