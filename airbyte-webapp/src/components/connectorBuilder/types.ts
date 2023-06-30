import { JSONSchema7 } from "json-schema";
import merge from "lodash/merge";
import { FieldPath, useWatch } from "react-hook-form";
import semver from "semver";
import * as yup from "yup";

import { naturalComparator } from "utils/objects";

import { CDK_VERSION } from "./cdk";
import { formatJson } from "./utils";
import { FORM_PATTERN_ERROR } from "../../core/form/types";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";
import {
  ConnectorManifest,
  Spec,
  ApiKeyAuthenticator,
  BasicHttpAuthenticator,
  BearerAuthenticator,
  DeclarativeStream,
  NoAuth,
  RequestOption,
  OAuthAuthenticator,
  HttpRequesterAuthenticator,
  DeclarativeStreamSchemaLoader,
  PageIncrement,
  OffsetIncrement,
  CursorPagination,
  SimpleRetrieverPaginator,
  DefaultPaginatorPageTokenOption,
  DatetimeBasedCursor,
  ListPartitionRouter,
  SubstreamPartitionRouterType,
  SubstreamPartitionRouter,
  ListPartitionRouterType,
  ApiKeyAuthenticatorType,
  OAuthAuthenticatorType,
  CursorPaginationType,
  OffsetIncrementType,
  PageIncrementType,
  BearerAuthenticatorType,
  BasicHttpAuthenticatorType,
  DefaultErrorHandler,
  CompositeErrorHandler,
  DefaultErrorHandlerBackoffStrategiesItem,
  DeclarativeStreamTransformationsItem,
  HttpResponseFilter,
  DefaultPaginator,
  DeclarativeComponentSchemaMetadata,
} from "../../core/request/ConnectorManifest";

export type EditorView = "ui" | "yaml";

export interface BuilderFormInput {
  key: string;
  required: boolean;
  definition: AirbyteJSONSchema;
  as_config_path?: boolean;
}

export type BuilderFormAuthenticator = (
  | NoAuth
  | (Omit<OAuthAuthenticator, "refresh_request_body"> & {
      refresh_request_body: Array<[string, string]>;
    })
  | ApiKeyAuthenticator
  | BearerAuthenticator
  | BasicHttpAuthenticator
) & { type: string };

export interface BuilderFormValues {
  global: {
    connectorName: string;
    urlBase: string;
    authenticator: BuilderFormAuthenticator;
  };
  inputs: BuilderFormInput[];
  inferredInputOverrides: Record<string, Partial<AirbyteJSONSchema>>;
  inputOrder: string[];
  streams: BuilderStream[];
  checkStreams: string[];
  version: string;
}

export type RequestOptionOrPathInject = Omit<RequestOption, "type"> | { inject_into: "path" };

export interface BuilderCursorPagination extends Omit<CursorPagination, "cursor_value" | "stop_condition"> {
  cursor:
    | {
        type: "custom";
        cursor_value: string;
        stop_condition?: string;
      }
    | { type: "response"; path: string[] }
    | { type: "headers"; path: string[] };
}

export interface BuilderPaginator {
  strategy: PageIncrement | OffsetIncrement | BuilderCursorPagination;
  pageTokenOption: RequestOptionOrPathInject;
  pageSizeOption?: RequestOption;
}

export interface BuilderSubstreamPartitionRouter {
  type: SubstreamPartitionRouterType;
  parent_key: string;
  partition_field: string;
  parentStreamReference: string;
  request_option?: RequestOption;
}

export interface BuilderListPartitionRouter extends Omit<ListPartitionRouter, "values"> {
  values: { type: "list"; value: string[] } | { type: "variable"; value: string };
}

export type BuilderTransformation =
  | {
      type: "add";
      path: string[];
      value: string;
    }
  | {
      type: "remove";
      path: string[];
    };

interface BuilderResponseFilter extends Omit<HttpResponseFilter, "http_codes"> {
  // turn http codes into string so they can be edited in the form - still enforced to parse into a number
  http_codes?: string[];
}

interface BuilderErrorHandler extends Omit<DefaultErrorHandler, "backoff_strategies" | "response_filters"> {
  backoff_strategy?: DefaultErrorHandlerBackoffStrategiesItem;
  response_filter?: BuilderResponseFilter;
}

export interface BuilderIncrementalSync
  extends Pick<
    DatetimeBasedCursor,
    "cursor_field" | "datetime_format" | "end_time_option" | "start_time_option" | "lookback_window"
  > {
  end_datetime:
    | {
        type: "user_input";
      }
    | { type: "now" }
    | { type: "custom"; value: string; format?: string };
  start_datetime:
    | {
        type: "user_input";
      }
    | { type: "custom"; value: string; format?: string };
  slicer?: {
    step?: string;
    cursor_granularity?: string;
  };
}

export const INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ";

export type BuilderRequestBody =
  | {
      type: "json_list";
      values: Array<[string, string]>;
    }
  | {
      type: "json_freeform";
      value: string;
    }
  | {
      type: "form_list";
      values: Array<[string, string]>;
    }
  | {
      type: "string_freeform";
      value: string;
    };

export interface BuilderStream {
  id: string;
  name: string;
  urlPath: string;
  fieldPointer: string[];
  primaryKey: string[];
  httpMethod: "GET" | "POST";
  requestOptions: {
    requestParameters: Array<[string, string]>;
    requestHeaders: Array<[string, string]>;
    requestBody: BuilderRequestBody;
  };
  paginator?: BuilderPaginator;
  transformations?: BuilderTransformation[];
  incrementalSync?: BuilderIncrementalSync;
  partitionRouter?: Array<BuilderListPartitionRouter | BuilderSubstreamPartitionRouter>;
  errorHandler?: BuilderErrorHandler[];
  schema?: string;
  unsupportedFields?: Record<string, object>;
  autoImportSchema: boolean;
}

// 0.29.0 is the version where breaking changes got introduced - older states can't be supported
export const OLDEST_SUPPORTED_CDK_VERSION = "0.29.0";

export function versionSupported(version: string) {
  return semver.satisfies(version, `>= ${OLDEST_SUPPORTED_CDK_VERSION} <=${CDK_VERSION}`);
}

export const DEFAULT_CONNECTOR_NAME = "Untitled";

export const LARGE_DURATION_OPTIONS = [
  { value: "PT1H", description: "1 hour" },
  { value: "P1D", description: "1 day" },
  { value: "P1W", description: "1 week" },
  { value: "P1M", description: "1 month" },
  { value: "P1Y", description: "1 year" },
];

export const SMALL_DURATION_OPTIONS = [
  { value: "PT0.000001S", description: "1 microsecond" },
  { value: "PT0.001S", description: "1 millisecond" },
  { value: "PT1S", description: "1 second" },
  { value: "PT1M", description: "1 minute" },
  { value: "PT1H", description: "1 hour" },
  { value: "P1D", description: "1 day" },
];

export const DATETIME_FORMAT_OPTIONS = [
  { value: "%Y-%m-%d" },
  { value: "%Y-%m-%d %H:%M:%S" },
  { value: "%Y-%m-%d %H:%M:%S.%f+00:00" },
  { value: "%Y-%m-%dT%H:%M:%S.%f%z" },
];

export const DEFAULT_BUILDER_FORM_VALUES: BuilderFormValues = {
  global: {
    connectorName: DEFAULT_CONNECTOR_NAME,
    urlBase: "",
    authenticator: { type: "NoAuth" },
  },
  inputs: [],
  inferredInputOverrides: {},
  inputOrder: [],
  streams: [],
  checkStreams: [],
  version: CDK_VERSION,
};

export const DEFAULT_SCHEMA = formatJson(
  {
    $schema: "http://json-schema.org/draft-07/schema#",
    type: "object",
    properties: {},
    additionalProperties: true,
  },
  true
);

export const isEmptyOrDefault = (schema?: string) => {
  return !schema || schema === DEFAULT_SCHEMA;
};

export const DEFAULT_BUILDER_STREAM_VALUES: Omit<BuilderStream, "id"> = {
  name: "",
  urlPath: "",
  fieldPointer: [],
  primaryKey: [],
  httpMethod: "GET",
  schema: DEFAULT_SCHEMA,
  requestOptions: {
    requestParameters: [],
    requestHeaders: [],
    requestBody: {
      type: "json_list",
      values: [],
    },
  },
  autoImportSchema: true,
};

export const LIST_PARTITION_ROUTER: ListPartitionRouterType = "ListPartitionRouter";
export const SUBSTREAM_PARTITION_ROUTER: SubstreamPartitionRouterType = "SubstreamPartitionRouter";

export const API_KEY_AUTHENTICATOR: ApiKeyAuthenticatorType = "ApiKeyAuthenticator";
export const BEARER_AUTHENTICATOR: BearerAuthenticatorType = "BearerAuthenticator";
export const BASIC_AUTHENTICATOR: BasicHttpAuthenticatorType = "BasicHttpAuthenticator";
export const OAUTH_AUTHENTICATOR: OAuthAuthenticatorType = "OAuthAuthenticator";

export const CURSOR_PAGINATION: CursorPaginationType = "CursorPagination";
export const OFFSET_INCREMENT: OffsetIncrementType = "OffsetIncrement";
export const PAGE_INCREMENT: PageIncrementType = "PageIncrement";

export const incrementalSyncInferredInputs: Record<"start_date" | "end_date", BuilderFormInput> = {
  start_date: {
    key: "start_date",
    required: true,
    definition: {
      type: "string",
      title: "Start date",
      format: "date-time",
      pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    },
  },
  end_date: {
    key: "end_date",
    required: true,
    definition: {
      type: "string",
      title: "End date",
      format: "date-time",
      pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    },
  },
};

export const DEFAULT_INFERRED_INPUT_ORDER = [
  "api_key",
  "username",
  "password",
  "client_id",
  "client_secret",
  "client_refresh_token",
];

export const authTypeToKeyToInferredInput = (
  authenticator: BuilderFormAuthenticator | { type: BuilderFormAuthenticator["type"] }
): Record<string, BuilderFormInput> => {
  switch (authenticator.type) {
    case "NoAuth":
      return {};
    case API_KEY_AUTHENTICATOR:
      return {
        api_token: {
          key: "api_key",
          required: true,
          definition: {
            type: "string",
            title: "API Key",
            airbyte_secret: true,
          },
        },
      };
    case BEARER_AUTHENTICATOR:
      return {
        api_token: {
          key: "api_key",
          required: true,
          definition: {
            type: "string",
            title: "API Key",
            airbyte_secret: true,
          },
        },
      };
    case BASIC_AUTHENTICATOR:
      return {
        username: {
          key: "username",
          required: true,
          definition: {
            type: "string",
            title: "Username",
          },
        },
        password: {
          key: "password",
          required: false,
          definition: {
            type: "string",
            title: "Password",
            always_show: true,
            airbyte_secret: true,
          },
        },
      };
    case OAUTH_AUTHENTICATOR:
      const baseInputs: Record<string, BuilderFormInput> = {
        client_id: {
          key: "client_id",
          required: true,
          definition: {
            type: "string",
            title: "Client ID",
            airbyte_secret: true,
          },
        },
        client_secret: {
          key: "client_secret",
          required: true,
          definition: {
            type: "string",
            title: "Client secret",
            airbyte_secret: true,
          },
        },
      };
      if (!("grant_type" in authenticator) || authenticator.grant_type === "refresh_token") {
        baseInputs.refresh_token = {
          key: "client_refresh_token",
          required: true,
          definition: {
            type: "string",
            title: "Refresh token",
            airbyte_secret: true,
          },
        };
        if ("refresh_token_updater" in authenticator && authenticator.refresh_token_updater) {
          baseInputs.oauth_access_token = {
            key: "oauth_access_token",
            required: true,
            definition: {
              type: "string",
              title: "Access token",
              airbyte_secret: true,
              description:
                "The current access token. This field might be overridden by the connector based on the token refresh endpoint response.",
            },
            as_config_path: true,
          };
          baseInputs.oauth_token_expiry_date = {
            key: "oauth_token_expiry_date",
            required: true,
            definition: {
              type: "string",
              title: "Token expiry date",
              format: "date-time",
              description:
                "The date the current access token expires in. This field might be overridden by the connector based on the token refresh endpoint response.",
            },
            as_config_path: true,
          };
        }
      }
      return baseInputs;
  }
};

export const OAUTH_ACCESS_TOKEN_INPUT = "oauth_access_token";
export const OAUTH_TOKEN_EXPIRY_DATE_INPUT = "oauth_token_expiry_date";

export const inferredAuthValues = (type: BuilderFormAuthenticator["type"]): Record<string, string> => {
  return Object.fromEntries(
    Object.entries(authTypeToKeyToInferredInput({ type })).map(([authKey, inferredInput]) => {
      return [authKey, interpolateConfigKey(inferredInput.key)];
    })
  );
};

export function hasIncrementalSyncUserInput(
  streams: BuilderFormValues["streams"],
  key: "start_datetime" | "end_datetime"
) {
  return streams.some((stream) => stream.incrementalSync?.[key].type === "user_input");
}

export function getInferredInputList(
  global: BuilderFormValues["global"],
  inferredInputOverrides: BuilderFormValues["inferredInputOverrides"],
  startDateInput: boolean,
  endDateInput: boolean
): BuilderFormInput[] {
  const authKeyToInferredInput = authTypeToKeyToInferredInput(global.authenticator);
  const authKeys = Object.keys(authKeyToInferredInput);
  const inputs = authKeys.flatMap((authKey) => {
    if (
      authKeyToInferredInput[authKey].as_config_path ||
      extractInterpolatedConfigKey(Reflect.get(global.authenticator, authKey)) === authKeyToInferredInput[authKey].key
    ) {
      return [authKeyToInferredInput[authKey]];
    }
    return [];
  });

  if (startDateInput) {
    inputs.push(incrementalSyncInferredInputs.start_date);
  }

  if (endDateInput) {
    inputs.push(incrementalSyncInferredInputs.end_date);
  }

  return inputs.map((input) =>
    inferredInputOverrides[input.key]
      ? {
          ...input,
          definition: { ...input.definition, ...inferredInputOverrides[input.key] },
        }
      : input
  );
}

const interpolateConfigKey = (key: string): string => {
  return `{{ config['${key}'] }}`;
};

const interpolatedConfigValueRegex = /^{{config\[('|"+)(.+)('|"+)\]}}$/;

export function isInterpolatedConfigKey(str: string | undefined): boolean {
  if (str === undefined) {
    return false;
  }
  const noWhitespaceString = str.replace(/\s/g, "");
  return interpolatedConfigValueRegex.test(noWhitespaceString);
}

export function extractInterpolatedConfigKey(str: string | undefined): string | undefined {
  if (str === undefined) {
    return undefined;
  }
  const noWhitespaceString = str.replace(/\s/g, "");
  const regexResult = interpolatedConfigValueRegex.exec(noWhitespaceString);
  if (regexResult === null) {
    return undefined;
  }
  return regexResult[2];
}

const INTERPOLATION_PATTERN = /^\{\{.+\}\}$/;

export const injectIntoValues = ["request_parameter", "header", "path", "body_data", "body_json"];
const nonPathRequestOptionSchema = yup
  .object()
  .shape({
    inject_into: yup.mixed().oneOf(injectIntoValues.filter((val) => val !== "path")),
    field_name: yup.string().required("form.empty.error"),
  })
  .notRequired()
  .default(undefined);

const keyValueListSchema = yup.array().of(yup.array().of(yup.string().required("form.empty.error")));

const yupNumberOrEmptyString = yup.number().transform((value) => (isNaN(value) ? undefined : value));

const jsonString = yup.string().test({
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

export const builderFormValidationSchema = yup.object().shape({
  global: yup.object().shape({
    connectorName: yup.string().required("form.empty.error").max(256, "connectorBuilder.maxLength"),
    urlBase: yup.string().required("form.empty.error"),
    authenticator: yup.object({
      inject_into: yup.mixed().when("type", {
        is: (type: string) => type === API_KEY_AUTHENTICATOR,
        then: nonPathRequestOptionSchema,
        otherwise: (schema) => schema.strip(),
      }),
      token_refresh_endpoint: yup.mixed().when("type", {
        is: OAUTH_AUTHENTICATOR,
        then: yup.string().required("form.empty.error"),
        otherwise: (schema) => schema.strip(),
      }),
      refresh_token_updater: yup.mixed().when("type", {
        is: OAUTH_AUTHENTICATOR,
        then: yup
          .object()
          .shape({
            refresh_token_name: yup.string(),
          })
          .default(undefined),
        otherwise: (schema) => schema.strip(),
      }),
      refresh_request_body: yup.mixed().when("type", {
        is: OAUTH_AUTHENTICATOR,
        then: keyValueListSchema,
        otherwise: (schema) => schema.strip(),
      }),
    }),
  }),
  streams: yup
    .array()
    .min(1)
    .of(
      yup.object().shape({
        name: yup.string().required("form.empty.error"),
        urlPath: yup.string().required("form.empty.error"),
        fieldPointer: yup.array().of(yup.string()),
        primaryKey: yup.array().of(yup.string()),
        httpMethod: yup.mixed().oneOf(["GET", "POST"]),
        requestOptions: yup.object().shape({
          requestParameters: keyValueListSchema,
          requestHeaders: keyValueListSchema,
          requestBody: yup.object().shape({
            values: yup.mixed().when("type", {
              is: (val: string) => val === "form_list" || val === "json_list",
              then: keyValueListSchema,
              otherwise: (schema) => schema.strip(),
            }),
            value: yup
              .mixed()
              .when("type", {
                is: (val: string) => val === "json_freeform",
                then: jsonString,
              })
              .when("type", {
                is: (val: string) => val === "string_freeform",
                then: yup.string(),
              }),
          }),
        }),
        schema: jsonString,
        paginator: yup
          .object()
          .shape({
            pageSizeOption: yup.mixed().when("strategy.page_size", {
              is: (val: number) => Boolean(val),
              then: nonPathRequestOptionSchema,
              otherwise: (schema) => schema.strip(),
            }),
            pageTokenOption: yup.object().shape({
              inject_into: yup.mixed().oneOf(injectIntoValues),
              field_name: yup.mixed().when("inject_into", {
                is: "path",
                then: (schema) => schema.strip(),
                otherwise: yup.string().required("form.empty.error"),
              }),
            }),
            strategy: yup
              .object({
                page_size: yupNumberOrEmptyString,
                cursor: yup.mixed().when("type", {
                  is: CURSOR_PAGINATION,
                  then: yup.object().shape({
                    cursor_value: yup.mixed().when("type", {
                      is: "custom",
                      then: yup.string().required("form.empty.error"),
                      otherwise: (schema) => schema.strip(),
                    }),
                    stop_condition: yup.mixed().when("type", {
                      is: "custom",
                      then: yup.string(),
                      otherwise: (schema) => schema.strip(),
                    }),
                    path: yup.mixed().when("type", {
                      is: (val: string) => val !== "custom",
                      then: yup.array().of(yup.string()).min(1, "form.empty.error"),
                      otherwise: (schema) => schema.strip(),
                    }),
                  }),
                  otherwise: (schema) => schema.strip(),
                }),
                start_from_page: yup.mixed().when("type", {
                  is: PAGE_INCREMENT,
                  then: yupNumberOrEmptyString,
                  otherwise: (schema) => schema.strip(),
                }),
              })
              .notRequired()
              .default(undefined),
          })
          .notRequired()
          .default(undefined),
        partitionRouter: yup
          .array(
            yup.object().shape({
              cursor_field: yup.mixed().when("type", {
                is: (val: string) => val === LIST_PARTITION_ROUTER,
                then: yup.string().required("form.empty.error"),
                otherwise: (schema) => schema.strip(),
              }),
              values: yup.mixed().when("type", {
                is: LIST_PARTITION_ROUTER,
                then: yup.object().shape({
                  value: yup.mixed().when("type", {
                    is: "list",
                    then: yup.array().of(yup.string()).min(1, "form.empty.error"),
                    otherwise: yup
                      .string()
                      .required("form.empty.error")
                      .matches(INTERPOLATION_PATTERN, FORM_PATTERN_ERROR),
                  }),
                }),
                otherwise: (schema) => schema.strip(),
              }),
              request_option: nonPathRequestOptionSchema,
              parent_key: yup.mixed().when("type", {
                is: SUBSTREAM_PARTITION_ROUTER,
                then: yup.string().required("form.empty.error"),
                otherwise: (schema) => schema.strip(),
              }),
              parentStreamReference: yup.mixed().when("type", {
                is: SUBSTREAM_PARTITION_ROUTER,
                then: yup.string().required("form.empty.error"),
                otherwise: (schema) => schema.strip(),
              }),
              partition_field: yup.mixed().when("type", {
                is: SUBSTREAM_PARTITION_ROUTER,
                then: yup.string().required("form.empty.error"),
                otherwise: (schema) => schema.strip(),
              }),
            })
          )
          .notRequired()
          .default(undefined),
        transformations: yup
          .array(
            yup.object().shape({
              path: yup.array(yup.string()).min(1, "form.empty.error"),
              value: yup.mixed().when("type", {
                is: (val: string) => val === "add",
                then: yup.string().required("form.empty.error"),
                otherwise: (schema) => schema.strip(),
              }),
            })
          )
          .notRequired()
          .default(undefined),
        errorHandler: yup
          .array(
            yup.object().shape({
              max_retries: yupNumberOrEmptyString,
              backoff_strategy: yup
                .object()
                .shape({
                  backoff_time_in_seconds: yup.mixed().when("type", {
                    is: (val: string) => val === "ConstantBackoffStrategy",
                    then: yupNumberOrEmptyString.required("form.empty.error"),
                    otherwise: (schema) => schema.strip(),
                  }),
                  factor: yup.mixed().when("type", {
                    is: (val: string) => val === "ExponentialBackoffStrategy",
                    then: yupNumberOrEmptyString,
                    otherwise: (schema) => schema.strip(),
                  }),
                  header: yup.mixed().when("type", {
                    is: (val: string) => val === "WaitTimeFromHeader" || val === "WaitUntilTimeFromHeader",
                    then: yup.string().required("form.empty.error"),
                    otherwise: (schema) => schema.strip(),
                  }),
                  regex: yup.mixed().when("type", {
                    is: (val: string) => val === "WaitTimeFromHeader" || val === "WaitUntilTimeFromHeader",
                    then: yup.string(),
                    otherwise: (schema) => schema.strip(),
                  }),
                  min_wait: yup.mixed().when("type", {
                    is: (val: string) => val === "WaitUntilTimeFromHeader",
                    then: yup.string(),
                    otherwise: (schema) => schema.strip(),
                  }),
                })
                .notRequired()
                .default(undefined),
              response_filter: yup
                .object()
                .shape({
                  error_message_contains: yup.string(),
                  predicate: yup.string().matches(INTERPOLATION_PATTERN, FORM_PATTERN_ERROR),
                  http_codes: yup.array(yup.string()).notRequired().default(undefined),
                  error_message: yup.string(),
                })
                .notRequired()
                .default(undefined),
            })
          )
          .notRequired()
          .default(undefined),
        incrementalSync: yup
          .object()
          .shape({
            cursor_field: yup.string().required("form.empty.error"),
            slicer: yup
              .object()
              .shape({
                cursor_granularity: yup.string().required("form.empty.error"),
                step: yup.string().required("form.empty.error"),
              })
              .default(undefined),
            start_datetime: yup.object().shape({
              value: yup.mixed().when("type", {
                is: (val: string) => val === "custom",
                then: yup.string().required("form.empty.error"),
                otherwise: (schema) => schema.strip(),
              }),
            }),
            end_datetime: yup.object().shape({
              value: yup.mixed().when("type", {
                is: (val: string) => val === "custom",
                then: yup.string().required("form.empty.error"),
                otherwise: (schema) => schema.strip(),
              }),
            }),
            datetime_format: yup.string().required("form.empty.error"),
            start_time_option: nonPathRequestOptionSchema,
            end_time_option: nonPathRequestOptionSchema,
            stream_state_field_start: yup.string(),
            stream_state_field_end: yup.string(),
            lookback_window: yup.string(),
          })
          .notRequired()
          .default(undefined),
      })
    ),
});

function builderAuthenticatorToManifest(globalSettings: BuilderFormValues["global"]): HttpRequesterAuthenticator {
  if (globalSettings.authenticator.type === "OAuthAuthenticator") {
    return {
      ...globalSettings.authenticator,
      refresh_token:
        globalSettings.authenticator.grant_type === "client_credentials"
          ? undefined
          : globalSettings.authenticator.refresh_token,
      refresh_token_updater:
        globalSettings.authenticator.grant_type === "client_credentials"
          ? undefined
          : globalSettings.authenticator.refresh_token_updater,
      refresh_request_body: Object.fromEntries(globalSettings.authenticator.refresh_request_body),
    };
  }
  if (globalSettings.authenticator.type === "ApiKeyAuthenticator") {
    return {
      ...globalSettings.authenticator,
      header: undefined,
    };
  }
  return globalSettings.authenticator as HttpRequesterAuthenticator;
}

function pathToSafeJinjaAccess(path: string[]): string {
  return path
    .map((segment) => {
      const asNumber = Number(segment);
      if (!Number.isNaN(asNumber)) {
        return `[${asNumber}]`;
      }
      return `.get("${segment}", {})`;
    })
    .join("");
}

function builderPaginationStrategyToManifest(
  strategy: BuilderPaginator["strategy"]
): DefaultPaginator["pagination_strategy"] {
  if (strategy.type === "OffsetIncrement" || strategy.type === "PageIncrement") {
    return strategy;
  }
  const { cursor, ...rest } = strategy;

  return {
    ...rest,
    cursor_value:
      cursor.type === "custom" ? cursor.cursor_value : `{{ ${cursor.type}${pathToSafeJinjaAccess(cursor.path)} }}`,
    stop_condition:
      cursor.type === "custom"
        ? cursor.stop_condition
        : `{{ not ${cursor.type}${pathToSafeJinjaAccess(cursor.path)} }}`,
  };
}

function builderPaginatorToManifest(paginator: BuilderStream["paginator"]): SimpleRetrieverPaginator {
  if (!paginator) {
    return { type: "NoPagination" };
  }

  let pageTokenOption: DefaultPaginatorPageTokenOption;
  if (paginator?.pageTokenOption.inject_into === "path") {
    pageTokenOption = { type: "RequestPath" };
  } else {
    pageTokenOption = {
      type: "RequestOption",
      inject_into: paginator.pageTokenOption.inject_into,
      field_name: paginator.pageTokenOption.field_name,
    };
  }
  return {
    type: "DefaultPaginator",
    page_token_option: pageTokenOption,
    page_size_option: paginator.strategy.page_size ? paginator.pageSizeOption : undefined,
    pagination_strategy: builderPaginationStrategyToManifest(paginator.strategy),
  };
}

function builderIncrementalToManifest(formValues: BuilderStream["incrementalSync"]): DatetimeBasedCursor | undefined {
  if (!formValues) {
    return undefined;
  }

  const { start_datetime, end_datetime, slicer, ...regularFields } = formValues;
  return {
    type: "DatetimeBasedCursor",
    ...regularFields,
    start_datetime: {
      type: "MinMaxDatetime",
      datetime: start_datetime.type === "custom" ? start_datetime.value : `{{ config['start_date'] }}`,
      datetime_format:
        start_datetime.type === "custom" ? start_datetime.format : INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
    },
    end_datetime: {
      type: "MinMaxDatetime",
      datetime:
        end_datetime.type === "custom"
          ? end_datetime.value
          : end_datetime.type === "now"
          ? `{{ now_utc().strftime('${INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT}') }}`
          : `{{ config['end_date'] }}`,
      datetime_format: end_datetime.type === "custom" ? end_datetime.format : INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
    },
    step: slicer?.step,
    cursor_granularity: slicer?.cursor_granularity,
  };
}

function builderStreamPartitionRouterToManifest(
  values: BuilderFormValues,
  partitionRouter: BuilderStream["partitionRouter"],
  visitedStreams: string[]
): Array<ListPartitionRouter | SubstreamPartitionRouter> | undefined {
  if (!partitionRouter) {
    return undefined;
  }
  if (partitionRouter.length === 0) {
    return undefined;
  }
  return partitionRouter.map((subRouter) => {
    if (subRouter.type === "ListPartitionRouter") {
      return {
        ...subRouter,
        values: subRouter.values.value,
      };
    }
    const parentStream = values.streams.find(({ id }) => id === subRouter.parentStreamReference);
    if (!parentStream) {
      return {
        type: "SubstreamPartitionRouter",
        parent_stream_configs: [],
      };
    }
    if (visitedStreams.includes(parentStream.id)) {
      // circular dependency
      return {
        type: "SubstreamPartitionRouter",
        parent_stream_configs: [],
      };
    }
    return {
      type: "SubstreamPartitionRouter",
      parent_stream_configs: [
        {
          type: "ParentStreamConfig",
          parent_key: subRouter.parent_key,
          request_option: subRouter.request_option,
          partition_field: subRouter.partition_field,
          stream: builderStreamToDeclarativeSteam(values, parentStream, visitedStreams),
        },
      ],
    };
  });
}

function buildCompositeErrorHandler(errorHandlers: BuilderStream["errorHandler"]): CompositeErrorHandler | undefined {
  if (!errorHandlers || errorHandlers.length === 0) {
    return undefined;
  }
  return {
    type: "CompositeErrorHandler",
    error_handlers: errorHandlers.map((handler) => ({
      ...handler,
      backoff_strategies: handler.backoff_strategy ? [handler.backoff_strategy] : undefined,
      response_filters: handler.response_filter
        ? [{ ...handler.response_filter, http_codes: handler.response_filter.http_codes?.map(Number) }]
        : undefined,
      backoff_strategy: undefined,
      response_filter: undefined,
    })),
  };
}

function builderTransformationsToManifest(
  transformations: BuilderTransformation[] | undefined
): DeclarativeStreamTransformationsItem[] | undefined {
  if (!transformations) {
    return undefined;
  }
  if (transformations.length === 0) {
    return undefined;
  }
  return transformations.map((transformation) => {
    if (transformation.type === "add") {
      return {
        type: "AddFields",
        fields: [
          {
            path: transformation.path,
            value: transformation.value,
          },
        ],
      };
    }
    return {
      type: "RemoveFields",
      field_pointers: [transformation.path],
    };
  });
}

const EMPTY_SCHEMA = { type: "InlineSchemaLoader", schema: {} } as const;

function parseSchemaString(schema?: string): DeclarativeStreamSchemaLoader {
  if (!schema) {
    return EMPTY_SCHEMA;
  }
  try {
    return { type: "InlineSchemaLoader", schema: JSON.parse(schema) };
  } catch {
    return EMPTY_SCHEMA;
  }
}

function builderRequestBodyToStreamRequestBody(stream: BuilderStream) {
  try {
    return {
      request_body_json:
        stream.requestOptions.requestBody.type === "json_list"
          ? Object.fromEntries(stream.requestOptions.requestBody.values)
          : stream.requestOptions.requestBody.type === "json_freeform"
          ? JSON.parse(stream.requestOptions.requestBody.value)
          : undefined,
      request_body_data:
        stream.requestOptions.requestBody.type === "form_list"
          ? Object.fromEntries(stream.requestOptions.requestBody.values)
          : stream.requestOptions.requestBody.type === "string_freeform"
          ? stream.requestOptions.requestBody.value
          : undefined,
    };
  } catch {
    return {};
  }
}

function builderStreamToDeclarativeSteam(
  values: BuilderFormValues,
  stream: BuilderStream,
  visitedStreams: string[]
): DeclarativeStream {
  const declarativeStream: DeclarativeStream = {
    type: "DeclarativeStream",
    name: stream.name,
    primary_key: stream.primaryKey,
    schema_loader: parseSchemaString(stream.schema),
    retriever: {
      type: "SimpleRetriever",
      requester: {
        type: "HttpRequester",
        url_base: values.global?.urlBase?.trim(),
        path: stream.urlPath?.trim(),
        http_method: stream.httpMethod,
        request_parameters: Object.fromEntries(stream.requestOptions.requestParameters),
        request_headers: Object.fromEntries(stream.requestOptions.requestHeaders),
        authenticator: builderAuthenticatorToManifest(values.global),
        error_handler: buildCompositeErrorHandler(stream.errorHandler),
        ...builderRequestBodyToStreamRequestBody(stream),
      },
      record_selector: {
        type: "RecordSelector",
        extractor: {
          type: "DpathExtractor",
          field_path: stream.fieldPointer,
        },
      },
      paginator: builderPaginatorToManifest(stream.paginator),
      partition_router: builderStreamPartitionRouterToManifest(values, stream.partitionRouter, [
        ...visitedStreams,
        stream.id,
      ]),
    },
    transformations: builderTransformationsToManifest(stream.transformations),
    incremental_sync: builderIncrementalToManifest(stream.incrementalSync),
  };

  return merge({}, declarativeStream, stream.unsupportedFields);
}

export const orderInputs = (
  inputs: BuilderFormInput[],
  inferredInputs: BuilderFormInput[],
  storedInputOrder: string[]
) => {
  const keyToStoredOrder = storedInputOrder.reduce((map, key, index) => map.set(key, index), new Map<string, number>());

  return inferredInputs
    .map((input) => {
      return { input, isInferred: true, id: input.key };
    })
    .concat(
      inputs.map((input) => {
        return { input, isInferred: false, id: input.key };
      })
    )
    .sort((inputA, inputB) => {
      const storedIndexA = keyToStoredOrder.get(inputA.id);
      const storedIndexB = keyToStoredOrder.get(inputB.id);

      if (storedIndexA !== undefined && storedIndexB !== undefined) {
        return storedIndexA - storedIndexB;
      }
      if (storedIndexA !== undefined && storedIndexB === undefined) {
        return inputB.isInferred ? 1 : -1;
      }
      if (storedIndexA === undefined && storedIndexB !== undefined) {
        return inputA.isInferred ? -1 : 1;
      }
      // both indexes are undefined
      if (inputA.isInferred && inputB.isInferred) {
        return DEFAULT_INFERRED_INPUT_ORDER.indexOf(inputA.id) - DEFAULT_INFERRED_INPUT_ORDER.indexOf(inputB.id);
      }
      if (inputA.isInferred && !inputB.isInferred) {
        return -1;
      }
      if (!inputA.isInferred && inputB.isInferred) {
        return 1;
      }
      return naturalComparator(inputA.id, inputB.id);
    });
};

export const builderFormValuesToMetadata = (values: BuilderFormValues): DeclarativeComponentSchemaMetadata => {
  return {
    autoImportSchema: Object.fromEntries(values.streams.map((stream) => [stream.name, stream.autoImportSchema])),
  };
};

export const convertToManifest = (values: BuilderFormValues): ConnectorManifest => {
  const manifestStreams: DeclarativeStream[] = values.streams.map((stream) =>
    builderStreamToDeclarativeSteam(values, stream, [])
  );

  const orderedInputs = orderInputs(
    values.inputs,
    getInferredInputList(
      values.global,
      values.inferredInputOverrides,
      hasIncrementalSyncUserInput(values.streams, "start_datetime"),
      hasIncrementalSyncUserInput(values.streams, "end_datetime")
    ),
    values.inputOrder
  );
  const allInputs = orderedInputs.map((orderedInput) => orderedInput.input);

  const specSchema: JSONSchema7 = {
    $schema: "http://json-schema.org/draft-07/schema#",
    type: "object",
    required: allInputs.filter((input) => input.required).map((input) => input.key),
    properties: Object.fromEntries(allInputs.map((input, index) => [input.key, { ...input.definition, order: index }])),
    additionalProperties: true,
  };

  const spec: Spec = {
    connection_specification: specSchema,
    documentation_url: "https://example.org",
    type: "Spec",
  };

  const streamNames = values.streams.map((s) => s.name);
  const validCheckStreamNames = (values.checkStreams ?? []).filter((checkStream) => streamNames.includes(checkStream));
  const correctedCheckStreams =
    validCheckStreamNames.length > 0 ? validCheckStreamNames : streamNames.length > 0 ? [streamNames[0]] : [];

  return merge({
    version: CDK_VERSION,
    type: "DeclarativeSource",
    check: {
      type: "CheckStream",
      stream_names: correctedCheckStreams,
    },
    streams: manifestStreams,
    spec,
    metadata: builderFormValuesToMetadata(values),
  });
};

export const DEFAULT_JSON_MANIFEST_VALUES: ConnectorManifest = convertToManifest(DEFAULT_BUILDER_FORM_VALUES);

export const useBuilderWatch = <TPath extends FieldPath<BuilderFormValues>>(
  path: TPath,
  options?: { exact: boolean }
) => useWatch<BuilderFormValues, TPath>({ name: path, ...options });

export type StreamPathFn = <T extends string>(fieldPath: T) => `streams.${number}.${T}`;
