import { load } from "js-yaml";
import { JSONSchema7 } from "json-schema";
import isString from "lodash/isString";
import merge from "lodash/merge";
import { FieldPath, useWatch } from "react-hook-form";
import semver from "semver";
import { match } from "ts-pattern";
import * as yup from "yup";
import { MixedSchema } from "yup/lib/mixed";

import { ConnectorBuilderProjectTestingValues } from "core/api/types/AirbyteClient";
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
  SessionTokenAuthenticator,
  SessionTokenAuthenticatorType,
  SessionTokenRequestApiKeyAuthenticatorType,
  SessionTokenRequestBearerAuthenticatorType,
  RequestOptionInjectInto,
  NoAuthType,
  HttpRequester,
  OAuthAuthenticatorRefreshTokenUpdater,
} from "core/api/types/ConnectorManifest";

import { CDK_VERSION } from "./cdk";
import { formatJson } from "./utils";
import { FORM_PATTERN_ERROR } from "../../core/form/types";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";

export interface BuilderState {
  name: string;
  mode: "ui" | "yaml";
  formValues: BuilderFormValues;
  yaml: string;
  view: "global" | "inputs" | number;
  testStreamIndex: number;
  testingValues: ConnectorBuilderProjectTestingValues | undefined;
}

export interface BuilderFormInput {
  key: string;
  required: boolean;
  definition: AirbyteJSONSchema;
  isLocked?: boolean;
}

type BuilderHttpMethod = "GET" | "POST";

interface BuilderRequestOptions {
  requestParameters: Array<[string, string]>;
  requestHeaders: Array<[string, string]>;
  requestBody: BuilderRequestBody;
}

export type BuilderSessionTokenAuthenticator = Omit<SessionTokenAuthenticator, "login_requester"> & {
  login_requester: {
    url: string;
    authenticator: NoAuth | ApiKeyAuthenticator | BearerAuthenticator | BasicHttpAuthenticator;
    httpMethod: BuilderHttpMethod;
    requestOptions: BuilderRequestOptions;
    errorHandler?: BuilderErrorHandler[];
  };
};

export type BuilderFormAuthenticator =
  | NoAuth
  | BuilderFormOAuthAuthenticator
  | ApiKeyAuthenticator
  | BearerAuthenticator
  | BasicHttpAuthenticator
  | BuilderSessionTokenAuthenticator;

export type BuilderFormOAuthAuthenticator = Omit<
  OAuthAuthenticator,
  "refresh_request_body" | "refresh_token_updater"
> & {
  refresh_request_body: Array<[string, string]>;
  refresh_token_updater?: Omit<
    OAuthAuthenticatorRefreshTokenUpdater,
    "access_token_config_path" | "token_expiry_date_config_path" | "refresh_token_config_path"
  > & {
    access_token: string;
    token_expiry_date: string;
  };
};

export interface BuilderFormValues {
  global: {
    urlBase: string;
    authenticator: BuilderFormAuthenticator;
  };
  inputs: BuilderFormInput[];
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
  pageTokenOption?: RequestOptionOrPathInject;
  pageSizeOption?: RequestOption;
}

export interface BuilderParentStream {
  parent_key: string;
  partition_field: string;
  parentStreamReference: string;
  request_option?: RequestOption;
}

export interface BuilderParameterizedRequests extends Omit<ListPartitionRouter, "values"> {
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

export interface BuilderErrorHandler extends Omit<DefaultErrorHandler, "backoff_strategies" | "response_filters"> {
  backoff_strategy?: DefaultErrorHandlerBackoffStrategiesItem;
  response_filter?: BuilderResponseFilter;
}

export interface BuilderIncrementalSync
  extends Pick<DatetimeBasedCursor, "cursor_field" | "end_time_option" | "start_time_option" | "lookback_window"> {
  end_datetime:
    | {
        type: "user_input";
        value: string;
      }
    | { type: "now" }
    | { type: "custom"; value: string; format?: string };
  start_datetime:
    | {
        type: "user_input";
        value: string;
      }
    | { type: "custom"; value: string; format?: string };
  slicer?: {
    step?: string;
    cursor_granularity?: string;
  };
  filter_mode: "range" | "start" | "no_filter";
  cursor_datetime_formats: string[];
  datetime_format?: string;
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

export type YamlString = string;
export const isYamlString = (value: unknown): value is YamlString => isString(value);

export interface BuilderStream {
  id: string;
  name: string;
  urlPath: string;
  fieldPointer: string[];
  primaryKey: string[];
  httpMethod: BuilderHttpMethod;
  requestOptions: BuilderRequestOptions;
  paginator?: BuilderPaginator | YamlString;
  transformations?: BuilderTransformation[] | YamlString;
  incrementalSync?: BuilderIncrementalSync | YamlString;
  parentStreams?: BuilderParentStream[];
  parameterizedRequests?: BuilderParameterizedRequests[];
  errorHandler?: BuilderErrorHandler[] | YamlString;
  schema?: string;
  unsupportedFields?: Record<string, object>;
  autoImportSchema: boolean;
}

type StreamName = string;
// todo: add more component names to this type as more components support in YAML
export type YamlSupportedComponentName = "paginator" | "errorHandler" | "transformations" | "incrementalSync";

export interface BuilderMetadata {
  autoImportSchema: Record<StreamName, boolean>;
  yamlComponents?: {
    streams: Record<StreamName, YamlSupportedComponentName[]>;
  };
}

export type ManifestValuePerComponentPerStream = Record<StreamName, Record<YamlSupportedComponentName, unknown>>;
export const getManifestValuePerComponentPerStream = (
  manifest: ConnectorManifest
): ManifestValuePerComponentPerStream => {
  if (manifest.metadata === undefined) {
    return {};
  }
  const metadata = manifest.metadata as BuilderMetadata;
  if (metadata?.yamlComponents?.streams === undefined) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(metadata?.yamlComponents?.streams).map(([streamName, yamlComponentNames]) => {
      // this method is only called in UI mode, so we can assume full streams are found in definitions
      const stream = manifest.definitions?.streams?.[streamName];
      const manifestValuePerComponent = Object.fromEntries(
        yamlComponentNames.map((yamlComponentName) =>
          match(yamlComponentName)
            .with("paginator", (name) => [name, stream?.retriever?.paginator])
            .with("errorHandler", (name) => [name, stream?.retriever?.requester?.error_handler])
            .with("transformations", (name) => [name, stream?.transformations])
            .with("incrementalSync", (name) => [name, stream?.incremental_sync])
            .otherwise(() => [])
        )
      );
      return [streamName, manifestValuePerComponent];
    })
  );
};

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
  { value: "%Y-%m-%dT%H:%M:%S" },
  { value: "%Y-%m-%dT%H:%M:%SZ" },
  { value: "%Y-%m-%dT%H:%M:%S%z" },
  { value: "%Y-%m-%dT%H:%M:%S.%fZ" },
  { value: "%Y-%m-%dT%H:%M:%S.%f%z" },
  { value: "%Y-%m-%d %H:%M:%S.%f+00:00" },
  { value: "%s" },
  { value: "%ms" },
];

export const DEFAULT_BUILDER_FORM_VALUES: BuilderFormValues = {
  global: {
    urlBase: "",
    authenticator: { type: "NoAuth" },
  },
  inputs: [],
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

export const NO_AUTH: NoAuthType = "NoAuth";
export const API_KEY_AUTHENTICATOR: ApiKeyAuthenticatorType = "ApiKeyAuthenticator";
export const BEARER_AUTHENTICATOR: BearerAuthenticatorType = "BearerAuthenticator";
export const BASIC_AUTHENTICATOR: BasicHttpAuthenticatorType = "BasicHttpAuthenticator";
export const OAUTH_AUTHENTICATOR: OAuthAuthenticatorType = "OAuthAuthenticator";
export const SESSION_TOKEN_AUTHENTICATOR: SessionTokenAuthenticatorType = "SessionTokenAuthenticator";
export const SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR: SessionTokenRequestApiKeyAuthenticatorType = "ApiKey";
export const SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR: SessionTokenRequestBearerAuthenticatorType = "Bearer";

export const CURSOR_PAGINATION: CursorPaginationType = "CursorPagination";
export const OFFSET_INCREMENT: OffsetIncrementType = "OffsetIncrement";
export const PAGE_INCREMENT: PageIncrementType = "PageIncrement";

export const OAUTH_ACCESS_TOKEN_INPUT = "oauth_access_token";
export const OAUTH_TOKEN_EXPIRY_DATE_INPUT = "oauth_token_expiry_date";

export function hasIncrementalSyncUserInput(
  streams: BuilderFormValues["streams"],
  key: "start_datetime" | "end_datetime"
) {
  return streams.some(
    (stream) =>
      !isYamlString(stream.incrementalSync) &&
      stream.incrementalSync?.[key]?.type === "user_input" &&
      (key === "start_datetime" || stream.incrementalSync?.filter_mode === "range")
  );
}

export function interpolateConfigKey(key: string): string;
export function interpolateConfigKey(key: string | undefined): string | undefined;
export function interpolateConfigKey(key: string | undefined): string | undefined {
  return key ? `{{ config["${key}"] }}` : undefined;
}

const interpolatedConfigValueRegexBracket = /^\s*{{\s*config\[('|")+(\S+)('|")+\]\s*}}\s*$/;
const interpolatedConfigValueRegexDot = /^\s*{{\s*config\.(\S+)\s*}}\s*$/;

export function isInterpolatedConfigKey(str: string | undefined): boolean {
  if (str === undefined) {
    return false;
  }
  return interpolatedConfigValueRegexBracket.test(str) || interpolatedConfigValueRegexDot.test(str);
}

export function extractInterpolatedConfigKey(str: string): string;
export function extractInterpolatedConfigKey(str: string | undefined): string | undefined;
export function extractInterpolatedConfigKey(str: string | undefined): string | undefined {
  /**
   * This methods does not work for nested configs like `config["credentials"]["client_secret"]` as the interpolated config key would be
   * `credentials"]["client_secret`. Same for config.credentials.client_secret which would output `credentials.client_secret` as a key
   */
  if (str === undefined) {
    return undefined;
  }
  const regexBracketResult = interpolatedConfigValueRegexBracket.exec(str);
  if (regexBracketResult === null) {
    const regexDotResult = interpolatedConfigValueRegexDot.exec(str);
    if (regexDotResult === null) {
      return undefined;
    }
    return regexDotResult[1];
  }
  return regexBracketResult[2];
}

const INTERPOLATION_PATTERN = /^\{\{.+\}\}$/;

export type InjectIntoValue = RequestOptionInjectInto | "path";
export const injectIntoOptions: Array<{ label: string; value: InjectIntoValue; fieldLabel?: string }> = [
  { label: "Query Parameter", value: "request_parameter", fieldLabel: "Parameter Name" },
  { label: "Header", value: "header", fieldLabel: "Header Name" },
  { label: "Path", value: "path" },
  { label: "Body data (urlencoded form)", value: "body_data", fieldLabel: "Key Name" },
  { label: "Body JSON payload", value: "body_json", fieldLabel: "Key Name" },
];

const REQUIRED_ERROR = "form.empty.error";
const strip = (schema: MixedSchema) => schema.strip();

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
            then: yup.string(),
            otherwise: strip,
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
  .default(undefined);

const apiKeyInjectIntoSchema = yup.mixed().when("type", {
  is: API_KEY_AUTHENTICATOR,
  then: nonPathRequestOptionSchema,
  otherwise: strip,
});

const httpMethodSchema = yup.mixed().oneOf(["GET", "POST"]);

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
        then: jsonString,
      })
      .when("type", {
        is: (val: string) => val === "string_freeform",
        then: yup.string(),
      }),
  }),
});

export const authenticatorSchema = yup.object({
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

export const globalSchema = yup.object().shape({
  urlBase: yup.string().required(REQUIRED_ERROR),
  authenticator: authenticatorSchema,
});

const maybeYamlSchema = (schema: yup.BaseSchema) => {
  return yup.lazy((val) =>
    isYamlString(val)
      ? // eslint-disable-next-line no-template-curly-in-string
        yup.string().test("is-valid-yaml", "${path} is not valid YAML", (value) => {
          if (!value) {
            return true;
          }
          try {
            load(value);
            return true;
          } catch {
            return false;
          }
        })
      : schema
  );
};

export const streamSchema = yup.object().shape({
  name: yup.string().required(REQUIRED_ERROR),
  urlPath: yup.string().required(REQUIRED_ERROR),
  fieldPointer: yup.array().of(yup.string()),
  primaryKey: yup.array().of(yup.string()),
  httpMethod: httpMethodSchema,
  requestOptions: requestOptionsSchema,
  schema: jsonString,
  paginator: maybeYamlSchema(
    yup
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
                  then: yup.string(),
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
      .default(undefined)
  ),
  parentStreams: yup
    .array(
      yup.object().shape({
        parent_key: yup.string().required(REQUIRED_ERROR),
        parentStreamReference: yup.string().required(REQUIRED_ERROR),
        partition_field: yup.string().required(REQUIRED_ERROR),
        request_option: nonPathRequestOptionSchema,
      })
    )
    .notRequired()
    .default(undefined),
  parameterizedRequests: yup
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
  transformations: maybeYamlSchema(
    yup
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
      .default(undefined)
  ),
  errorHandler: maybeYamlSchema(errorHandlerSchema),
  incrementalSync: maybeYamlSchema(
    yup
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
      .default(undefined)
  ),
});

export const builderFormValidationSchema = yup.object().shape({
  global: globalSchema,
  streams: yup.array().min(1).of(streamSchema),
});

export const builderStateValidationSchema = yup.object().shape({
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
});

function splitUrl(url: string): { base: string; path: string } {
  const lastSlashIndex = url.lastIndexOf("/");

  if (lastSlashIndex === -1) {
    // return a "/" for the path to avoid setting path to an empty string, which breaks validation
    return { base: url, path: "/" };
  }

  const leftSide = url.substring(0, lastSlashIndex);
  const rightSide = url.substring(lastSlashIndex + 1);

  return { base: leftSide, path: rightSide || "/" };
}

function convertOrLoadYamlString<BuilderInput, ManifestOutput>(
  builderValue: BuilderInput | YamlString | undefined,
  convertFn: (builderValue: BuilderInput | undefined) => ManifestOutput | undefined
) {
  if (builderValue === undefined) {
    return undefined;
  }
  if (isYamlString(builderValue)) {
    return load(builderValue) as ManifestOutput;
  }
  return convertFn(builderValue);
}

function builderAuthenticatorToManifest(
  globalSettings: BuilderFormValues["global"]
): HttpRequesterAuthenticator | undefined {
  if (globalSettings.authenticator.type === "NoAuth") {
    return undefined;
  }
  if (globalSettings.authenticator.type === "OAuthAuthenticator") {
    const { access_token, token_expiry_date, ...refresh_token_updater } =
      globalSettings.authenticator.refresh_token_updater ?? {};
    return {
      ...globalSettings.authenticator,
      refresh_token:
        globalSettings.authenticator.grant_type === "client_credentials"
          ? undefined
          : globalSettings.authenticator.refresh_token,
      refresh_token_updater:
        globalSettings.authenticator.grant_type === "client_credentials" ||
        !globalSettings.authenticator.refresh_token_updater
          ? undefined
          : {
              ...refresh_token_updater,
              access_token_config_path: [
                extractInterpolatedConfigKey(globalSettings.authenticator.refresh_token_updater.access_token),
              ],
              token_expiry_date_config_path: [
                extractInterpolatedConfigKey(globalSettings.authenticator.refresh_token_updater.token_expiry_date),
              ],
              refresh_token_config_path: [extractInterpolatedConfigKey(globalSettings.authenticator.refresh_token!)],
            },
      refresh_request_body: Object.fromEntries(globalSettings.authenticator.refresh_request_body),
    };
  }
  if (globalSettings.authenticator.type === "ApiKeyAuthenticator") {
    return {
      ...globalSettings.authenticator,
      header: undefined,
      api_token: globalSettings.authenticator.api_token,
    };
  }
  if (globalSettings.authenticator.type === "BearerAuthenticator") {
    return {
      ...globalSettings.authenticator,
      api_token: globalSettings.authenticator.api_token,
    };
  }
  if (globalSettings.authenticator.type === "BasicHttpAuthenticator") {
    return {
      ...globalSettings.authenticator,
      username: globalSettings.authenticator.username,
      password: globalSettings.authenticator.password,
    };
  }
  if (globalSettings.authenticator.type === "SessionTokenAuthenticator") {
    const builderLoginRequester = globalSettings.authenticator.login_requester;
    const { base, path } = splitUrl(builderLoginRequester.url ?? "");
    return {
      ...globalSettings.authenticator,
      login_requester: {
        type: "HttpRequester",
        url_base: base,
        path,
        authenticator: builderLoginRequester.authenticator,
        error_handler: builderErrorHandlersToManifest(builderLoginRequester.errorHandler),
        http_method: builderLoginRequester.httpMethod,
        request_parameters: Object.fromEntries(builderLoginRequester.requestOptions.requestParameters),
        request_headers: Object.fromEntries(builderLoginRequester.requestOptions.requestHeaders),
        ...builderRequestBodyToStreamRequestBody(builderLoginRequester.requestOptions.requestBody),
      },
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
  const correctedStrategy = {
    ...strategy,
    // must manually convert page_size to a number if it exists, because RHF watch() treats all numeric values as strings
    page_size: strategy.page_size ? Number(strategy.page_size) : undefined,
  };

  if (correctedStrategy.type === "OffsetIncrement" || correctedStrategy.type === "PageIncrement") {
    return correctedStrategy;
  }

  const { cursor, ...rest } = correctedStrategy;

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

export function builderPaginatorToManifest(
  paginator: BuilderPaginator | undefined
): SimpleRetrieverPaginator | undefined {
  if (!paginator) {
    return undefined;
  }

  let pageTokenOption: DefaultPaginatorPageTokenOption | undefined;
  if (!paginator.pageTokenOption) {
    pageTokenOption = undefined;
  } else if (paginator?.pageTokenOption.inject_into === "path") {
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

export function builderIncrementalSyncToManifest(
  formValues: BuilderIncrementalSync | undefined
): DatetimeBasedCursor | undefined {
  if (!formValues) {
    return undefined;
  }

  const {
    start_datetime,
    end_datetime,
    slicer,
    start_time_option,
    end_time_option,
    filter_mode,
    cursor_datetime_formats,
    datetime_format,
    ...regularFields
  } = formValues;
  const startDatetime = {
    type: "MinMaxDatetime" as const,
    datetime: start_datetime.value,
    datetime_format: start_datetime.type === "custom" ? start_datetime.format : INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  };
  const manifestIncrementalSync = {
    type: "DatetimeBasedCursor" as const,
    ...regularFields,
    cursor_datetime_formats,
    datetime_format: datetime_format || cursor_datetime_formats[0],
    start_datetime: startDatetime,
  };

  if (filter_mode === "range") {
    return {
      ...manifestIncrementalSync,
      start_time_option,
      end_time_option,
      end_datetime: {
        type: "MinMaxDatetime",
        datetime:
          end_datetime.type === "now"
            ? `{{ now_utc().strftime('${INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT}') }}`
            : end_datetime.value,
        datetime_format: end_datetime.type === "custom" ? end_datetime.format : INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
      },
      step: slicer?.step,
      cursor_granularity: slicer?.cursor_granularity,
    };
  }
  if (filter_mode === "start") {
    return {
      ...manifestIncrementalSync,
      start_time_option,
    };
  }
  return {
    ...manifestIncrementalSync,
    is_data_feed: true,
  };
}

function builderStreamPartitionRouterToManifest(
  values: BuilderFormValues,
  parentStreams: BuilderStream["parentStreams"],
  parameterizedRequests: BuilderStream["parameterizedRequests"],
  visitedStreams: string[]
): Array<ListPartitionRouter | SubstreamPartitionRouter> | undefined {
  let substreamPartitionRouters: SubstreamPartitionRouter[] | undefined = undefined;
  if (parentStreams && parentStreams.length > 0) {
    substreamPartitionRouters = parentStreams.map((parentStreamConfiguration) => {
      const parentStream = values.streams.find(({ id }) => id === parentStreamConfiguration.parentStreamReference);
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
            parent_key: parentStreamConfiguration.parent_key,
            request_option: parentStreamConfiguration.request_option,
            partition_field: parentStreamConfiguration.partition_field,
            stream: streamRef(parentStream.name),
          },
        ],
      };
    });
  }

  let listPartitionRouters: ListPartitionRouter[] | undefined = undefined;
  if (parameterizedRequests && parameterizedRequests.length > 0) {
    listPartitionRouters = parameterizedRequests.map((parameterizedRequest) => {
      return {
        ...parameterizedRequest,
        values: parameterizedRequest.values.value,
      };
    });
  }

  const combinedPartitionRouters = [...(substreamPartitionRouters ?? []), ...(listPartitionRouters ?? [])];
  return combinedPartitionRouters.length > 0 ? combinedPartitionRouters : undefined;
}

export function builderErrorHandlersToManifest(
  errorHandlers: BuilderErrorHandler[] | undefined
): CompositeErrorHandler | undefined {
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

export function builderTransformationsToManifest(
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

function fromEntriesOrUndefined(...args: Parameters<typeof Object.fromEntries>) {
  const obj = Object.fromEntries(...args);
  return Object.keys(obj).length > 0 ? obj : undefined;
}

function builderRequestBodyToStreamRequestBody(builderRequestBody: BuilderRequestBody) {
  try {
    const requestBody = {
      request_body_json:
        builderRequestBody.type === "json_list"
          ? fromEntriesOrUndefined(builderRequestBody.values)
          : builderRequestBody.type === "json_freeform"
          ? ((parsedJson) => (Object.keys(parsedJson).length > 0 ? parsedJson : undefined))(
              JSON.parse(builderRequestBody.value)
            )
          : undefined,
      request_body_data:
        builderRequestBody.type === "form_list"
          ? fromEntriesOrUndefined(builderRequestBody.values)
          : builderRequestBody.type === "string_freeform"
          ? builderRequestBody.value
          : undefined,
    };
    return Object.keys(requestBody).length > 0 ? requestBody : undefined;
  } catch {
    return undefined;
  }
}

type BaseRequester = Pick<HttpRequester, "type" | "url_base" | "authenticator">;

function builderStreamToDeclarativeSteam(
  values: BuilderFormValues,
  stream: BuilderStream,
  visitedStreams: string[]
): DeclarativeStream {
  // cast to tell typescript which properties will be present after resolving the ref
  const requesterRef = {
    $ref: "#/definitions/base_requester",
  } as unknown as BaseRequester;

  const declarativeStream: DeclarativeStream = {
    type: "DeclarativeStream",
    name: stream.name,
    primary_key: stream.primaryKey.length > 0 ? stream.primaryKey : undefined,
    retriever: {
      type: "SimpleRetriever",
      requester: {
        ...requesterRef,
        path: stream.urlPath?.trim(),
        http_method: stream.httpMethod,
        request_parameters: fromEntriesOrUndefined(stream.requestOptions.requestParameters),
        request_headers: fromEntriesOrUndefined(stream.requestOptions.requestHeaders),
        ...builderRequestBodyToStreamRequestBody(stream.requestOptions.requestBody),
        error_handler: convertOrLoadYamlString(stream.errorHandler, builderErrorHandlersToManifest),
      },
      record_selector: {
        type: "RecordSelector",
        extractor: {
          type: "DpathExtractor",
          field_path: stream.fieldPointer,
        },
      },
      paginator: convertOrLoadYamlString(stream.paginator, builderPaginatorToManifest),
      partition_router: builderStreamPartitionRouterToManifest(
        values,
        stream.parentStreams,
        stream.parameterizedRequests,
        [...visitedStreams, stream.id]
      ),
    },
    incremental_sync: convertOrLoadYamlString(stream.incrementalSync, builderIncrementalSyncToManifest),
    transformations: convertOrLoadYamlString(stream.transformations, builderTransformationsToManifest),
    schema_loader: { type: "InlineSchemaLoader", schema: schemaRef(stream.name) },
  };

  return merge({}, declarativeStream, stream.unsupportedFields);
}

export const builderFormValuesToMetadata = (values: BuilderFormValues): BuilderMetadata => {
  const componentNameIfString = (componentName: YamlSupportedComponentName, value: unknown) =>
    isYamlString(value) ? [componentName] : [];

  const yamlComponentsPerStream = {} as Record<string, YamlSupportedComponentName[]>;
  values.streams.forEach((stream) => {
    const yamlComponents = [
      ...componentNameIfString("paginator", stream.paginator),
      ...componentNameIfString("errorHandler", stream.errorHandler),
      ...componentNameIfString("transformations", stream.transformations),
      ...componentNameIfString("incrementalSync", stream.incrementalSync),
    ];
    if (yamlComponents.length > 0) {
      yamlComponentsPerStream[stream.name] = yamlComponents;
    }
  });

  const hasYamlComponents = Object.keys(yamlComponentsPerStream).length > 0;

  return {
    autoImportSchema: Object.fromEntries(values.streams.map((stream) => [stream.name, stream.autoImportSchema])),
    ...(hasYamlComponents && {
      yamlComponents: {
        streams: yamlComponentsPerStream,
      },
    }),
  };
};

export const builderInputsToSpec = (inputs: BuilderFormInput[]): Spec => {
  const specSchema: JSONSchema7 = {
    $schema: "http://json-schema.org/draft-07/schema#",
    type: "object",
    required: inputs.filter((input) => input.required).map((input) => input.key),
    properties: Object.fromEntries(inputs.map((input, index) => [input.key, { ...input.definition, order: index }])),
    additionalProperties: true,
  };

  return {
    connection_specification: specSchema,
    type: "Spec",
  };
};

export const convertToManifest = (values: BuilderFormValues): ConnectorManifest => {
  const manifestStreams: DeclarativeStream[] = values.streams.map((stream) =>
    builderStreamToDeclarativeSteam(values, stream, [])
  );

  const streamNames = values.streams.map((s) => s.name);
  const validCheckStreamNames = (values.checkStreams ?? []).filter((checkStream) => streamNames.includes(checkStream));
  const correctedCheckStreams =
    validCheckStreamNames.length > 0 ? validCheckStreamNames : streamNames.length > 0 ? [streamNames[0]] : [];

  const streamNameToStream = Object.fromEntries(manifestStreams.map((stream) => [stream.name, stream]));
  const streamRefs = manifestStreams.map((stream) => streamRef(stream.name!));

  const streamNameToSchema = Object.fromEntries(
    values.streams.map((stream) => {
      const schema = stream.schema ? JSON.parse(stream.schema) : JSON.parse(DEFAULT_SCHEMA);
      schema.additionalProperties = true;
      return [stream.name, schema];
    })
  );

  const baseRequester: BaseRequester = {
    type: "HttpRequester",
    url_base: values.global?.urlBase?.trim(),
    authenticator: builderAuthenticatorToManifest(values.global),
  };

  return {
    version: CDK_VERSION,
    type: "DeclarativeSource",
    check: {
      type: "CheckStream",
      stream_names: correctedCheckStreams,
    },
    definitions: {
      base_requester: baseRequester,
      streams: streamNameToStream,
    },
    streams: streamRefs,
    schemas: streamNameToSchema,
    spec: builderInputsToSpec(values.inputs),
    metadata: builderFormValuesToMetadata(values),
  };
};

function streamRef(streamName: string) {
  // force cast to DeclarativeStream so that this still validates against the types
  return { $ref: `#/definitions/streams/${streamName}` } as unknown as DeclarativeStream;
}

function schemaRef(streamName: string) {
  return { $ref: `#/schemas/${streamName}` };
}

export const DEFAULT_JSON_MANIFEST_VALUES: ConnectorManifest = convertToManifest(DEFAULT_BUILDER_FORM_VALUES);

export const useBuilderWatch = <TPath extends FieldPath<BuilderState>>(path: TPath, options?: { exact: boolean }) =>
  useWatch<BuilderState, TPath>({ name: path, ...options });

export type BuilderPathFn = <TPath extends FieldPath<BuilderState>>(fieldPath: string) => TPath;

export type StreamPathFn = <T extends string>(fieldPath: T) => `formValues.streams.${number}.${T}`;

export const concatPath = <TBase extends string, TPath extends string>(base: TBase, path: TPath) =>
  `${base}.${path}` as const;
