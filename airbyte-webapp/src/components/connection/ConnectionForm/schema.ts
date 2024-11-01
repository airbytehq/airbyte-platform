import { useMemo } from "react";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { useDescribeCronExpressionFetchQuery } from "core/api";
import {
  AirbyteStreamAndConfiguration,
  AirbyteStream,
  AirbyteStreamConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  DestinationSyncMode,
  NamespaceDefinitionType,
  SyncMode,
  StreamMapperType,
} from "core/api/types/AirbyteClient";
import { traverseSchemaToField } from "core/domain/catalog";
import { FeatureItem, useFeature } from "core/services/features";
import { NON_I18N_ERROR_TYPE } from "core/utils/form";

export const I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED = "form.cronExpression.underOneHourNotAllowed";

function nextExecutionsMoreFrequentThanOncePerHour(nextExecutions: number[]): boolean {
  if (nextExecutions.length > 1) {
    const [firstExecution, secondExecution] = nextExecutions;
    return secondExecution - firstExecution < 3600;
  }
  return false;
}

/**
 * yup schema for the schedule data
 */
export const useGetScheduleDataSchema = () => {
  const allowSubOneHourCronExpressions = useFeature(FeatureItem.AllowSyncSubOneHourCronExpressions);
  const validateCronExpression = useDescribeCronExpressionFetchQuery();

  return useMemo(() => {
    return yup.mixed().when("scheduleType", (scheduleType) => {
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
              .test("validCron", async (value, { createError, path }) => {
                if (!value) {
                  return createError({
                    path,
                    message: "form.empty.error",
                  });
                }
                try {
                  const response = await validateCronExpression(value);
                  if (!response.isValid) {
                    return createError({
                      path,
                      message: response.validationErrorMessage,
                      type: NON_I18N_ERROR_TYPE,
                    });
                  }
                  if (
                    !allowSubOneHourCronExpressions &&
                    nextExecutionsMoreFrequentThanOncePerHour(response.nextExecutions)
                  ) {
                    return createError({
                      path,
                      message: I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED,
                    });
                  }
                } catch (error) {
                  return createError({
                    path,
                    message: error.message,
                    type: NON_I18N_ERROR_TYPE,
                  });
                }
                return true;
              }),
            cronTimeZone: yup.string().required("form.empty.error"),
          })
          .defined("form.empty.error"),
      });
    });
  }, [validateCronExpression, allowSubOneHourCronExpressions]);
};

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
  isResumable: yup.boolean().optional(),
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
  hashedFields: yup
    .array()
    .of(yup.object({ fieldPath: yup.array().of(yup.string().optional()).optional() }))
    .optional(),
  mappers: yup
    .array()
    .of(
      yup.object({
        type: yup.mixed<StreamMapperType>().oneOf(Object.values(StreamMapperType)),
        mapperConfiguration: yup.object(),
      })
    )
    .optional(),
  aliasName: yup.string().optional(),
  primaryKey: yup.array().of(yup.array().of(yup.string())).optional(),
  minimumGenerationId: yup.number().optional(),
  generationId: yup.number().optional(),
  syncId: yup.number().optional(),
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

        if (
          (DestinationSyncMode.append_dedup === value.destinationSyncMode ||
            DestinationSyncMode.overwrite_dedup === value.destinationSyncMode) &&
          value.primaryKey?.length === 0
        ) {
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
          value.cursorField?.filter(Boolean).length === 0 // filter out empty strings
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
export const syncCatalogSchema = yup.object({
  streams: yup
    .array()
    .of(streamAndConfigurationSchema)
    .test(
      "syncCatalog.streams.required",
      "connectionForm.streams.required",
      (streams) => streams?.some(({ config }) => !!config?.selected) ?? false
    )
    .test(
      "syncCatalog.streams.mappers",
      "connectionForm.streams.existingMappers",
      (streams) =>
        !streams?.some(
          (stream) => stream.config?.mappers?.some((mapper) => mapper.type !== StreamMapperType.hashing)
        ) ?? true
    )
    .test("syncCatalog.streams.hash", "connectionForm.streams.hashFieldCollision", (streams) => {
      // group all top-level included fields by stream name & namespace
      const selectedFieldNamesByStream = (streams ?? []).reduce<Record<string, Set<string>>>(
        (acc, { stream, config }) => {
          if (!stream || !config?.selected) {
            return acc;
          }

          const namespace = stream.namespace ?? "";
          const name = stream.name ?? "";
          const key = `${namespace}_${name}`;

          const selectedFields = config.selectedFields?.map((field) => field.fieldPath?.join(".")) ?? [];
          const hasSelectedFields = selectedFields.length > 0;

          const traversedFields = traverseSchemaToField(stream.jsonSchema, stream.name);
          const topLevelFields = traversedFields.reduce<Set<string>>((acc, field) => {
            if (field.path.length === 1) {
              if (!hasSelectedFields || selectedFields.includes(field.path[0])) {
                acc.add(field.path[0]);
              }
            }
            return acc;
          }, new Set());

          acc[key] = topLevelFields;
          return acc;
        },
        {}
      );

      // check if any included, hashed field within a given stream will conflict
      // with another stream with the same resulting field name
      const hasConflictingStream = streams?.some(({ stream, config }) => {
        if (!config?.selected) {
          // stream isn't selected
          return false;
        }

        const { hashedFields } = config;
        if (!hashedFields) {
          // stream doesn't have hashed fields
          return false;
        }

        const streamName = stream?.name;
        const namespace = stream?.namespace ?? "";
        const selectedFieldNames = selectedFieldNamesByStream[`${namespace}_${streamName}`];

        const resolvedHashedFields = hashedFields.map(({ fieldPath }) => fieldPath?.join("."));
        if (
          resolvedHashedFields.some(
            // check if this field is selected and conflicts with another selected field
            (field) => selectedFieldNames.has(field ?? "") && selectedFieldNames.has(`${field}_hashed`)
          )
        ) {
          return true;
        }

        return false;
      });

      return !hasConflictingStream;
    }),
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
