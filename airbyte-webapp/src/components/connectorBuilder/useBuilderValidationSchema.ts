import { load } from "js-yaml";
import isObject from "lodash/isObject";
import { useCallback, useMemo } from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";
import { MixedSchema } from "yup/lib/mixed";
import { AnyObject } from "yup/lib/types";

import {
  ListPartitionRouter,
  RequestOptionInjectInto,
  SimpleRetrieverPartitionRouter,
  SimpleRetrieverPartitionRouterAnyOfItem,
} from "core/api/types/ConnectorManifest";
import { FORM_PATTERN_ERROR } from "core/form/types";

import {
  API_KEY_AUTHENTICATOR,
  BuilderStream,
  CURSOR_PAGINATION,
  CUSTOM_PARTITION_ROUTER,
  LIST_PARTITION_ROUTER,
  OAUTH_AUTHENTICATOR,
  PAGE_INCREMENT,
  SESSION_TOKEN_AUTHENTICATOR,
  SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
  SUBSTREAM_PARTITION_ROUTER,
  isYamlString,
} from "./types";

const INTERPOLATION_PATTERN = /^\{\{.+\}\}$/;
const REQUIRED_ERROR = "form.empty.error";
const strip = (schema: MixedSchema) => schema.strip();

export const useBuilderValidationSchema = () => {
  const { formatMessage } = useIntl();

  const validatePartitionRouterTypes = useCallback(
    (
      partitionRouter: SimpleRetrieverPartitionRouter,
      createError: (params?: yup.CreateErrorOptions | undefined) => yup.ValidationError,
      validTypes: Array<SimpleRetrieverPartitionRouterAnyOfItem["type"]>,
      additionalInvalidTypeMessage?: string
    ): boolean | yup.ValidationError => {
      if (Array.isArray(partitionRouter)) {
        for (const subRouter of partitionRouter) {
          const validationResult = validatePartitionRouterTypes(
            subRouter,
            createError,
            validTypes,
            additionalInvalidTypeMessage
          );
          if (validationResult !== true) {
            return validationResult;
          }
        }
        return true;
      }

      if (!partitionRouter.type) {
        return createError({ message: formatMessage({ id: "connectorBuilder.partitionRouter.missingTypeMessage" }) });
      }

      if (!validTypes.includes(partitionRouter.type)) {
        return createError({
          message: `${formatMessage(
            { id: "connectorBuilder.partitionRouter.invalidTypeMessage" },
            { inputType: partitionRouter.type, validTypes: validTypes.join(", ") }
          )}\n${additionalInvalidTypeMessage}`,
        });
      }

      return true;
    },
    [formatMessage]
  );

  const validateListPartitionRouterValues = useCallback(
    (
      partitionRouter: ListPartitionRouter | ListPartitionRouter[],
      createError: (params?: yup.CreateErrorOptions | undefined) => yup.ValidationError
    ): boolean | yup.ValidationError => {
      if (Array.isArray(partitionRouter)) {
        for (const subRouter of partitionRouter) {
          const validationResult = validateListPartitionRouterValues(subRouter, createError);
          if (validationResult !== true) {
            return validationResult;
          }
        }
        return true;
      }

      if (!Array.isArray(partitionRouter.values) && !partitionRouter.values.match(INTERPOLATION_PATTERN)) {
        return createError({ message: formatMessage({ id: "connectorBuilder.partitionRouter.valuesPattern" }) });
      }

      return true;
    },
    [formatMessage]
  );

  const globalSchema = useMemo(
    () =>
      yup.object().shape({
        urlBase: yup.string().required(REQUIRED_ERROR),
        authenticator: maybeYamlSchema(authenticatorSchema),
      }),
    []
  );

  const streamSchema = useMemo(
    () =>
      yup.object().shape({
        name: yup
          .string()
          .required(REQUIRED_ERROR)
          .test(
            "unique-stream-name",
            "connectorBuilder.unique",
            (value: string | undefined, testContext: AnyObject) => {
              const allStreamNames = testContext.from[1].value.streams.map((stream: BuilderStream) => stream.name);
              return allStreamNames.filter((name: string) => name === value).length === 1;
            }
          ),
        urlPath: yup.string().required(REQUIRED_ERROR),
        primaryKey: yup.array().of(yup.string()),
        httpMethod: httpMethodSchema,
        requestOptions: requestOptionsSchema,
        schema: jsonString,
        recordSelector: maybeYamlSchema(recordSelectorSchema),
        paginator: maybeYamlSchema(paginatorSchema),
        parentStreams: maybeYamlSchema(
          yup
            .array(
              yup.object().shape({
                parent_key: yup.string().required(REQUIRED_ERROR),
                parentStreamReference: yup.string().required(REQUIRED_ERROR),
                partition_field: yup.string().required(REQUIRED_ERROR),
                request_option: nonPathRequestOptionSchema,
                incremental_dependency: yup.boolean().default(false),
              })
            )
            .notRequired()
            .default(undefined),
          (parsedYaml, createError) =>
            validatePartitionRouterTypes(parsedYaml as SimpleRetrieverPartitionRouter, createError, [
              SUBSTREAM_PARTITION_ROUTER,
              CUSTOM_PARTITION_ROUTER,
            ])
        ),
        parameterizedRequests: maybeYamlSchema(
          yup
            .array(
              yup.object().shape({
                cursor_field: yup.string().required(REQUIRED_ERROR),
                values: yup.object().shape({
                  value: yup.mixed().when("type", {
                    is: "list",
                    then: yup.array().of(yup.string()).min(1, REQUIRED_ERROR),
                    otherwise: yup.string().required(REQUIRED_ERROR).matches(INTERPOLATION_PATTERN, FORM_PATTERN_ERROR),
                  }),
                }),
                request_option: nonPathRequestOptionSchema,
              })
            )
            .notRequired()
            .default(undefined),
          (parsedYaml, createError) => {
            const partitionRouter = parsedYaml as SimpleRetrieverPartitionRouter;

            const typeValidationResult = validatePartitionRouterTypes(
              partitionRouter,
              createError,
              [LIST_PARTITION_ROUTER],
              formatMessage({ id: "connectorBuilder.partitionRouter.customRouter" })
            );

            if (typeValidationResult !== true) {
              return typeValidationResult;
            }

            return validateListPartitionRouterValues(
              partitionRouter as ListPartitionRouter | ListPartitionRouter[],
              createError
            );
          }
        ),
        transformations: maybeYamlSchema(transformationsSchema),
        errorHandler: maybeYamlSchema(errorHandlerSchema),
        incrementalSync: maybeYamlSchema(incrementalSyncSchema),
        unknownFields: yamlSchema((parsedYaml, createError) => {
          if (Array.isArray(parsedYaml) || !isObject(parsedYaml)) {
            return createError({ message: formatMessage({ id: "connectorBuilder.unknownFields.nonObjectError" }) });
          }
          return true;
        }),
      }),
    [formatMessage, validateListPartitionRouterValues, validatePartitionRouterTypes]
  );

  const builderFormValidationSchema = useMemo(
    () =>
      yup.object().shape({
        global: globalSchema,
        streams: yup.array().min(1).of(streamSchema),
      }),
    [globalSchema, streamSchema]
  );

  const builderStateValidationSchema = useMemo(
    () =>
      yup.object().shape({
        name: yup.string().required(REQUIRED_ERROR).max(256, "connectorBuilder.maxLength"),
        mode: yup.mixed().oneOf(["ui", "yaml"]).required(REQUIRED_ERROR),
        formValues: builderFormValidationSchema.required(REQUIRED_ERROR),
        yaml: yup.string().required(REQUIRED_ERROR),
        view: yup
          .mixed()
          .test(
            "isValidView",
            'Must be "global", "inputs", or a number',
            (value) => typeof value === "number" || value === "global" || value === "inputs"
          ),
        testStreamIndex: yup.number().min(0).required(REQUIRED_ERROR),
      }),
    [builderFormValidationSchema]
  );

  return {
    globalSchema,
    streamSchema,
    builderFormValidationSchema,
    builderStateValidationSchema,
  };
};

export type InjectIntoValue = RequestOptionInjectInto | "path";
export const injectIntoOptions: Array<{ label: string; value: InjectIntoValue; fieldLabel?: string }> = [
  { label: "Query Parameter", value: "request_parameter", fieldLabel: "Parameter Name" },
  { label: "Header", value: "header", fieldLabel: "Header Name" },
  { label: "Path", value: "path" },
  { label: "Body data (urlencoded form)", value: "body_data", fieldLabel: "Key Name" },
  { label: "Body JSON payload", value: "body_json", fieldLabel: "Key Name" },
];

export const jsonString = yup.string().test({
  test: (val: string | undefined) => {
    if (!val) {
      return true;
    }
    try {
      JSON.parse(val);
      return true;
    } catch {
      return false;
    }
  },
  message: "connectorBuilder.invalidJSON",
});

const nonArrayJsonString = jsonString.test({
  test: (val: string | undefined) => {
    if (!val) {
      return true;
    }
    try {
      const parsedValue = JSON.parse(val);
      if (Array.isArray(parsedValue)) {
        return false;
      }
    } catch {
      return true;
    }
    return true;
  },
  message: "connectorBuilder.invalidArray",
});

const nonPathRequestOptionSchema = yup
  .object()
  .shape({
    inject_into: yup.mixed().oneOf(injectIntoOptions.map((option) => option.value).filter((val) => val !== "path")),
    field_name: yup.string().required(REQUIRED_ERROR),
  })
  .notRequired()
  .default(undefined);

const keyValueListSchema = yup.array().of(yup.array().of(yup.string().required(REQUIRED_ERROR)));

const yupNumberOrEmptyString = yup.number().transform((value) => (isNaN(value) ? undefined : value));

const schemaIfNotDataFeed = (schema: yup.AnySchema) =>
  yup.mixed().when("filter_mode", {
    is: (val: string) => val !== "no_filter",
    then: schema,
  });

const schemaIfRangeFilter = (schema: yup.AnySchema) =>
  yup.mixed().when("filter_mode", {
    is: (val: string) => val === "range",
    then: schema,
  });

type TestParsedYaml = (
  parsedYaml: unknown,
  createError: (params?: yup.CreateErrorOptions | undefined) => yup.ValidationError
) => boolean | yup.ValidationError;

const maybeYamlSchema = (schema: yup.BaseSchema, testParsedYaml?: TestParsedYaml) => {
  return yup.lazy((val) => (isYamlString(val) ? yamlSchema(testParsedYaml) : schema));
};

const yamlSchema = (testParsedYaml?: TestParsedYaml) =>
  yup.string().test("is-valid-yaml", "Invalid YAML", (value, { createError }) => {
    if (!value) {
      return true;
    }
    let parsedValue;
    try {
      parsedValue = load(value);
    } catch {
      return false;
    }
    return testParsedYaml ? testParsedYaml(parsedValue, createError) : true;
  });

const interpolationString = yup
  .string()
  .matches(INTERPOLATION_PATTERN, { message: FORM_PATTERN_ERROR, excludeEmptyString: true });

const requestOptionsSchema = yup.object().shape({
  requestParameters: keyValueListSchema,
  requestHeaders: keyValueListSchema,
  requestBody: yup.object().shape({
    values: yup.mixed().when("type", {
      is: (val: string) => val === "form_list" || val === "json_list",
      then: keyValueListSchema,
      otherwise: strip,
    }),
    value: yup
      .mixed()
      .when("type", {
        is: (val: string) => val === "json_freeform",
        then: nonArrayJsonString,
      })
      .when("type", {
        is: (val: string) => val === "string_freeform",
        then: yup.string(),
      }),
  }),
});

const apiKeyInjectIntoSchema = yup.mixed().when("type", {
  is: API_KEY_AUTHENTICATOR,
  then: nonPathRequestOptionSchema,
  otherwise: strip,
});

const httpMethodSchema = yup.mixed().oneOf(["GET", "POST"]);

const errorHandlerSchema = yup
  .array(
    yup.object().shape({
      max_retries: yupNumberOrEmptyString,
      backoff_strategy: yup
        .object()
        .shape({
          backoff_time_in_seconds: yup.mixed().when("type", {
            is: (val: string) => val === "ConstantBackoffStrategy",
            then: yupNumberOrEmptyString.required(REQUIRED_ERROR),
            otherwise: strip,
          }),
          factor: yup.mixed().when("type", {
            is: (val: string) => val === "ExponentialBackoffStrategy",
            then: yupNumberOrEmptyString,
            otherwise: strip,
          }),
          header: yup.mixed().when("type", {
            is: (val: string) => val === "WaitTimeFromHeader" || val === "WaitUntilTimeFromHeader",
            then: yup.string().required(REQUIRED_ERROR),
            otherwise: strip,
          }),
          regex: yup.mixed().when("type", {
            is: (val: string) => val === "WaitTimeFromHeader" || val === "WaitUntilTimeFromHeader",
            then: yup.string(),
            otherwise: strip,
          }),
          min_wait: yup.mixed().when("type", {
            is: (val: string) => val === "WaitUntilTimeFromHeader",
            then: yupNumberOrEmptyString,
            otherwise: strip,
          }),
        })
        .notRequired()
        .default(undefined),
      response_filter: yup
        .object()
        .shape({
          error_message_contains: yup.string(),
          predicate: interpolationString,
          http_codes: yup.array(yup.string()).notRequired().default(undefined),
          error_message: yup.string(),
        })
        .notRequired()
        .default(undefined),
    })
  )
  .notRequired()
  .default(undefined);

const authenticatorSchema = yup.object({
  type: yup.string().required(REQUIRED_ERROR),
  inject_into: apiKeyInjectIntoSchema,
  token_refresh_endpoint: yup.mixed().when("type", {
    is: OAUTH_AUTHENTICATOR,
    then: yup.string().required(REQUIRED_ERROR),
    otherwise: strip,
  }),
  refresh_token_updater: yup.mixed().when("type", {
    is: OAUTH_AUTHENTICATOR,
    then: yup
      .object()
      .shape({
        refresh_token_name: yup.string(),
      })
      .default(undefined),
    otherwise: strip,
  }),
  refresh_request_body: yup.mixed().when("type", {
    is: OAUTH_AUTHENTICATOR,
    then: keyValueListSchema,
    otherwise: strip,
  }),
  login_requester: yup.mixed().when("type", {
    is: SESSION_TOKEN_AUTHENTICATOR,
    then: yup.object().shape({
      url: yup.string().required(REQUIRED_ERROR),
      authenticator: yup.object({
        inject_into: apiKeyInjectIntoSchema,
      }),
      errorHandler: errorHandlerSchema,
      httpMethod: httpMethodSchema,
      requestOptions: requestOptionsSchema,
    }),
    otherwise: strip,
  }),
  session_token_path: yup.mixed().when("type", {
    is: SESSION_TOKEN_AUTHENTICATOR,
    then: yup.array().of(yup.string()).min(1, REQUIRED_ERROR).required(REQUIRED_ERROR),
    otherwise: strip,
  }),
  expiration_duration: yup.mixed().when("type", {
    is: SESSION_TOKEN_AUTHENTICATOR,
    then: yup.string(),
    otherwise: strip,
  }),
  request_authentication: yup.mixed().when("type", {
    is: SESSION_TOKEN_AUTHENTICATOR,
    then: yup.object().shape({
      inject_into: yup.mixed().when("type", {
        is: SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
        then: nonPathRequestOptionSchema,
        otherwise: strip,
      }),
    }),
    otherwise: strip,
  }),
});

const recordSelectorSchema = yup.object().shape({
  fieldPath: yup.array().of(yup.string()),
  filterCondition: interpolationString.notRequired().default(undefined),
  normalizeToSchema: yup.boolean().default(false),
});

const paginatorSchema = yup
  .object()
  .shape({
    pageSizeOption: yup.mixed().when("strategy.page_size", {
      is: (val: number) => Boolean(val),
      then: nonPathRequestOptionSchema,
      otherwise: strip,
    }),
    pageTokenOption: yup
      .object()
      .shape({
        inject_into: yup.mixed().oneOf(injectIntoOptions.map((option) => option.value)),
        field_name: yup.mixed().when("inject_into", {
          is: "path",
          then: strip,
          otherwise: yup.string().required(REQUIRED_ERROR),
        }),
      })
      .notRequired()
      .default(undefined),
    strategy: yup
      .object({
        page_size: yupNumberOrEmptyString,
        cursor: yup.mixed().when("type", {
          is: CURSOR_PAGINATION,
          then: yup.object().shape({
            cursor_value: yup.mixed().when("type", {
              is: "custom",
              then: yup.string().required(REQUIRED_ERROR),
              otherwise: strip,
            }),
            stop_condition: yup.mixed().when("type", {
              is: "custom",
              then: interpolationString,
              otherwise: strip,
            }),
            path: yup.mixed().when("type", {
              is: (val: string) => val !== "custom",
              then: yup.array().of(yup.string()).min(1, REQUIRED_ERROR),
              otherwise: strip,
            }),
          }),
          otherwise: strip,
        }),
        start_from_page: yup.mixed().when("type", {
          is: PAGE_INCREMENT,
          then: yupNumberOrEmptyString,
          otherwise: strip,
        }),
      })
      .notRequired()
      .default(undefined),
  })
  .notRequired()
  .default(undefined);

const incrementalSyncSchema = yup
  .object()
  .shape({
    cursor_field: yup.string().required(REQUIRED_ERROR),
    slicer: schemaIfNotDataFeed(
      yup
        .object()
        .shape({
          cursor_granularity: yup.string().required(REQUIRED_ERROR),
          step: yup.string().required(REQUIRED_ERROR),
        })
        .default(undefined)
    ),
    start_datetime: yup.object().shape({
      value: yup.mixed().when("type", {
        is: (val: string) => val === "custom" || val === "user_input",
        then: yup.string().required(REQUIRED_ERROR),
        otherwise: strip,
      }),
    }),
    end_datetime: schemaIfRangeFilter(
      yup.object().shape({
        value: yup.mixed().when("type", {
          is: (val: string) => val === "custom" || val === "user_input",
          then: yup.string().required(REQUIRED_ERROR),
          otherwise: strip,
        }),
      })
    ),
    datetime_format: yup.string().notRequired().default(undefined),
    cursor_datetime_formats: yup.array(yup.string()).min(1, REQUIRED_ERROR).required(REQUIRED_ERROR),
    start_time_option: schemaIfNotDataFeed(nonPathRequestOptionSchema),
    end_time_option: schemaIfRangeFilter(nonPathRequestOptionSchema),
    stream_state_field_start: yup.string(),
    stream_state_field_end: yup.string(),
    lookback_window: yup.string(),
  })
  .notRequired()
  .default(undefined);

const transformationsSchema = yup
  .array(
    yup.object().shape({
      path: yup.array(yup.string()).min(1, REQUIRED_ERROR),
      value: yup.mixed().when("type", {
        is: (val: string) => val === "add",
        then: yup.string().required(REQUIRED_ERROR),
        otherwise: strip,
      }),
    })
  )
  .notRequired()
  .default(undefined);
