import { useMemo } from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { DropDownOptionDataItem } from "components/ui/DropDown";

import { NormalizationType } from "area/connection/types";
import { validateCronExpression, validateCronFrequencyOneHourOrMore } from "area/connection/utils";
import { isDbtTransformation, isNormalizationTransformation, isWebhookTransformation } from "area/connection/utils";
import { ConnectionValues, useCurrentWorkspace } from "core/api";
import {
  ActorDefinitionVersionRead,
  ConnectionScheduleData,
  ConnectionScheduleType,
  DestinationDefinitionSpecificationRead,
  DestinationSyncMode,
  Geography,
  NamespaceDefinitionType,
  NonBreakingChangesPreference,
  OperationCreate,
  OperationRead,
  OperatorType,
  SchemaChange,
  SyncMode,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { SyncSchema } from "core/domain/catalog";
import { SOURCE_NAMESPACE_TAG } from "core/domain/connector/source";
import { FeatureItem, useFeature } from "core/services/features";
import { ConnectionFormMode, ConnectionOrPartialConnection } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import calculateInitialCatalog from "./calculateInitialCatalog";
import { frequencyConfig } from "./frequencyConfig";
import { DbtOperationRead } from "../TransformationHookForm";

export interface FormikConnectionFormValues {
  name?: string;
  scheduleType?: ConnectionScheduleType | null;
  scheduleData?: ConnectionScheduleData | null;
  nonBreakingChangesPreference?: NonBreakingChangesPreference | null;
  prefix: string;
  syncCatalog: SyncSchema;
  namespaceDefinition?: NamespaceDefinitionType;
  namespaceFormat: string;
  transformations?: OperationRead[];
  normalization?: NormalizationType;
  geography: Geography;
}

export type ConnectionFormValues = ConnectionValues;

export const SUPPORTED_MODES: Array<[SyncMode, DestinationSyncMode]> = [
  [SyncMode.incremental, DestinationSyncMode.append_dedup],
  [SyncMode.full_refresh, DestinationSyncMode.overwrite],
  [SyncMode.incremental, DestinationSyncMode.append],
  [SyncMode.full_refresh, DestinationSyncMode.append],
];

const DEFAULT_SCHEDULE: ConnectionScheduleData = {
  basicSchedule: {
    units: 24,
    timeUnit: "hours",
  },
};

export function useDefaultTransformation(): OperationCreate {
  const workspace = useCurrentWorkspace();
  return {
    name: "My dbt transformations",
    workspaceId: workspace.workspaceId,
    operatorConfiguration: {
      operatorType: OperatorType.dbt,
      dbt: {
        gitRepoUrl: "", // TODO: Does this need a value?
        dockerImage: "fishtownanalytics/dbt:1.0.0",
        dbtArguments: "run",
      },
    },
  };
}

const createConnectionValidationSchema = (
  mode: ConnectionFormMode,
  allowSubOneHourCronExpressions: boolean,
  allowAutoDetectSchema: boolean
) => {
  return yup
    .object({
      // The connection name during Editing is handled separately from the form
      name: mode === "create" ? yup.string().required("form.empty.error") : yup.string().notRequired(),
      geography: yup.mixed<Geography>().oneOf(Object.values(Geography)),
      scheduleType: yup
        .string()
        .oneOf([ConnectionScheduleType.manual, ConnectionScheduleType.basic, ConnectionScheduleType.cron]),
      scheduleData: yup.mixed().when("scheduleType", (scheduleType) => {
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
                  return validation.isValid === true
                    ? true
                    : createError({
                        path,
                        message: validation.message ?? "form.cronExpression.invalid",
                      });
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
      }),
      nonBreakingChangesPreference: allowAutoDetectSchema
        ? yup.mixed().oneOf(Object.values(NonBreakingChangesPreference)).required("form.empty.error")
        : yup.mixed().notRequired(),
      namespaceDefinition: yup
        .string()
        .oneOf([
          NamespaceDefinitionType.destination,
          NamespaceDefinitionType.source,
          NamespaceDefinitionType.customformat,
        ])
        .required("form.empty.error"),
      namespaceFormat: yup.string().when("namespaceDefinition", {
        is: NamespaceDefinitionType.customformat,
        then: yup.string().trim().required("form.empty.error"),
      }),
      prefix: yup.string(),
      syncCatalog: yup.object({
        streams: yup
          .array()
          .of(
            yup.object({
              id: yup
                .string()
                // This is required to get rid of id fields we are using to detect stream for edition
                .when("$isRequest", (isRequest: boolean, schema: yup.StringSchema) =>
                  isRequest ? schema.strip(true) : schema
                ),
              stream: yup.object(),
              config: yup
                .object({
                  selected: yup.boolean(),
                  syncMode: yup.string(),
                  destinationSyncMode: yup.string(),
                  primaryKey: yup.array().of(yup.array().of(yup.string())),
                  cursorField: yup.array().of(yup.string()),
                })
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
                      DestinationSyncMode.append_dedup === value.destinationSyncMode &&
                      value.primaryKey?.length === 0
                    ) {
                      errors.push(
                        this.createError({
                          message: "connectionForm.primaryKey.required",
                          path: `${pathRoot}.streams[${this.parent.id}].config.primaryKey`,
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
                          path: `${pathRoot}.streams[${this.parent.id}].config.cursorField`,
                        })
                      );
                    }

                    return errors.length > 0 ? new yup.ValidationError(errors) : true;
                  },
                }),
            })
          )
          .test(
            "syncCatalog.streams.required",
            "connectionForm.streams.required",
            (streams) => streams?.some(({ config }) => !!config.selected) ?? false
          ),
      }),
    })
    .noUnknown();
};

interface CreateConnectionValidationSchemaArgs {
  mode: ConnectionFormMode;
}

export const useConnectionValidationSchema = ({ mode }: CreateConnectionValidationSchemaArgs) => {
  const allowSubOneHourCronExpressions = useFeature(FeatureItem.AllowSyncSubOneHourCronExpressions);
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  return useMemo(
    () => createConnectionValidationSchema(mode, allowSubOneHourCronExpressions, allowAutoDetectSchema),
    [allowAutoDetectSchema, allowSubOneHourCronExpressions, mode]
  );
};

export type ConnectionValidationSchema = ReturnType<typeof useConnectionValidationSchema>;

/**
 * Returns {@link Operation}[]
 *
 * Maps UI representation of Transformation and Normalization
 * into API's {@link Operation} representation.
 *
 * Always puts normalization as first operation
 * @param values
 * @param initialOperations
 * @param workspaceId
 */
// TODO: need to split this mapper for each type of operation(transformations, normalizations, webhooks)
export function mapFormPropsToOperation(
  values: {
    transformations?: OperationRead[];
    normalization?: NormalizationType;
  },
  initialOperations: OperationRead[] = [],
  workspaceId: string
): OperationCreate[] {
  const newOperations: OperationCreate[] = [];

  if (values.normalization && values.normalization !== NormalizationType.raw) {
    const normalizationOperation = initialOperations.find(isNormalizationTransformation);

    if (normalizationOperation) {
      newOperations.push(normalizationOperation);
    } else {
      newOperations.push({
        name: "Normalization",
        workspaceId,
        operatorConfiguration: {
          operatorType: OperatorType.normalization,
          normalization: {
            option: values.normalization,
          },
        },
      });
    }
  }

  if (values.transformations) {
    newOperations.push(...values.transformations);
  }

  // webhook operations (e.g. dbt Cloud jobs in the Airbyte Cloud integration) are managed
  // by separate sub-forms; they should not be ignored (which would cause accidental
  // deletions), but managing them should not be combined with this (already-confusing)
  // codepath, either.
  newOperations.push(...initialOperations.filter(isWebhookTransformation));

  return newOperations;
}

/**
 * get transformation operations only
 * @param operations
 */
export const getInitialTransformations = (operations: OperationRead[]): DbtOperationRead[] =>
  operations?.filter(isDbtTransformation) ?? [];

export const getInitialNormalization = (
  operations?: Array<OperationRead | OperationCreate>,
  isNotCreateMode?: boolean
): NormalizationType => {
  const initialNormalization =
    operations?.find(isNormalizationTransformation)?.operatorConfiguration?.normalization?.option;

  return initialNormalization
    ? NormalizationType[initialNormalization]
    : isNotCreateMode
    ? NormalizationType.raw
    : NormalizationType.basic;
};

export const useInitialValues = (
  connection: ConnectionOrPartialConnection,
  destDefinitionVersion: ActorDefinitionVersionRead,
  destDefinitionSpecification: DestinationDefinitionSpecificationRead,
  isNotCreateMode?: boolean
): FormikConnectionFormValues => {
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
      calculateInitialCatalog(
        connection.syncCatalog,
        destDefinitionSpecification?.supportedDestinationSyncModes || [],
        streamTransformsWithBreakingChange,
        isNotCreateMode,
        newStreamDescriptors
      ),
    [
      streamTransformsWithBreakingChange,
      connection.syncCatalog,
      destDefinitionSpecification?.supportedDestinationSyncModes,
      isNotCreateMode,
      newStreamDescriptors,
    ]
  );

  return useMemo(() => {
    const initialValues: FormikConnectionFormValues = {
      syncCatalog: initialSchema,
      scheduleType: connection.connectionId ? connection.scheduleType : ConnectionScheduleType.basic,
      scheduleData: connection.connectionId ? connection.scheduleData ?? null : DEFAULT_SCHEDULE,
      nonBreakingChangesPreference: connection.nonBreakingChangesPreference ?? defaultNonBreakingChangesPreference,
      prefix: connection.prefix || "",
      namespaceDefinition: connection.namespaceDefinition || NamespaceDefinitionType.destination,
      namespaceFormat: connection.namespaceFormat ?? SOURCE_NAMESPACE_TAG,
      geography: connection.geography || workspace.defaultGeography || "auto",
    };

    // Is Create Mode
    if (!isNotCreateMode) {
      initialValues.name = connection.name ?? `${connection.source.name} → ${connection.destination.name}`;
    }

    const operations = connection.operations ?? [];

    if (destDefinitionVersion.supportsDbt) {
      initialValues.transformations = getInitialTransformations(operations);
    }

    if (destDefinitionVersion.normalizationConfig?.supported) {
      initialValues.normalization = getInitialNormalization(operations, isNotCreateMode);
    }

    return initialValues;
  }, [
    connection.connectionId,
    connection.destination.name,
    connection.geography,
    connection.name,
    connection.namespaceDefinition,
    connection.namespaceFormat,
    connection.nonBreakingChangesPreference,
    connection.operations,
    connection.prefix,
    connection.scheduleData,
    connection.scheduleType,
    connection.source.name,
    defaultNonBreakingChangesPreference,
    destDefinitionVersion.supportsDbt,
    destDefinitionVersion.normalizationConfig,
    initialSchema,
    isNotCreateMode,
    workspace,
  ]);
};

export const useFrequencyDropdownData = (
  additionalFrequency: WebBackendConnectionRead["scheduleData"]
): DropDownOptionDataItem[] => {
  const { formatMessage } = useIntl();

  return useMemo(() => {
    const frequencies = [...frequencyConfig];
    if (additionalFrequency?.basicSchedule) {
      const additionalFreqAlreadyPresent = frequencies.some(
        (frequency) =>
          frequency?.timeUnit === additionalFrequency.basicSchedule?.timeUnit &&
          frequency?.units === additionalFrequency.basicSchedule?.units
      );
      if (!additionalFreqAlreadyPresent) {
        frequencies.push(additionalFrequency.basicSchedule);
      }
    }

    const basicFrequencies = frequencies.map((frequency) => ({
      value: frequency,
      label: formatMessage(
        {
          id: `form.every.${frequency.timeUnit}`,
        },
        { value: frequency.units }
      ),
    }));

    // Add Manual and Custom to the frequencies list
    const customFrequency = formatMessage({
      id: "frequency.cron",
    });
    const manualFrequency = formatMessage({
      id: "frequency.manual",
    });
    const otherFrequencies = [
      {
        label: manualFrequency,
        value: manualFrequency.toLowerCase(),
      },
      {
        label: customFrequency,
        value: customFrequency.toLowerCase(),
      },
    ];

    return [...otherFrequencies, ...basicFrequencies];
  }, [formatMessage, additionalFrequency]);
};
