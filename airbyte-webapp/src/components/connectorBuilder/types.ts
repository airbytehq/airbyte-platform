import { load } from "js-yaml";
import { JSONSchema7 } from "json-schema";
import isString from "lodash/isString";
import merge from "lodash/merge";
import omit from "lodash/omit";
import semver from "semver";
import { match } from "ts-pattern";

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
  DeclarativeStreamTransformationsItem,
  HttpResponseFilter,
  DefaultPaginator,
  SessionTokenAuthenticator,
  SessionTokenAuthenticatorType,
  SessionTokenRequestApiKeyAuthenticatorType,
  SessionTokenRequestBearerAuthenticatorType,
  NoAuthType,
  HttpRequester,
  OAuthAuthenticatorRefreshTokenUpdater,
  OAuthConfigSpecificationOauthConnectorInputSpecification,
  RecordSelector,
  SimpleRetrieverPartitionRouter,
  SimpleRetrieverPartitionRouterAnyOfItem,
  CustomPartitionRouterType,
  ConstantBackoffStrategy,
  ExponentialBackoffStrategy,
  WaitUntilTimeFromHeader,
  WaitTimeFromHeader,
  SimpleRetrieverDecoder,
  XmlDecoderType,
  JwtAuthenticator,
  JwtAuthenticatorType,
  RequestOptionType,
} from "core/api/types/ConnectorManifest";

import { DecoderTypeConfig } from "./Builder/DecoderConfig";
import { CDK_VERSION } from "./cdk";
import { filterPartitionRouterToType, formatJson, streamRef } from "./utils";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";

export interface BuilderState {
  name: string;
  mode: "ui" | "yaml";
  formValues: BuilderFormValues;
  previewValues?: BuilderFormValues;
  yaml: string;
  customComponentsCode?: string;
  view: "global" | "inputs" | "components" | number;
  testStreamIndex: number;
  testingValues: ConnectorBuilderProjectTestingValues | undefined;
}

export interface AssistData {
  docsUrl?: string;
  openapiSpecUrl?: string;
}

export interface BuilderFormInput {
  key: string;
  required: boolean;
  definition: AirbyteJSONSchema;
  isLocked?: boolean;
}

type BuilderHttpMethod = "GET" | "POST";

export const BUILDER_DECODER_TYPES = ["JSON", "XML", "JSON Lines", "Iterable", "CSV"] as const;

export type ManifestDecoderType = "JsonDecoder" | "XmlDecoder" | "JsonlDecoder" | "IterableDecoder" | "CsvDecoder";

interface JSONDecoderConfig {
  type: "JSON";
}

interface XMLDecoderConfig {
  type: "XML";
}

interface JSONLinesDecoderConfig {
  type: "JSON Lines";
}

interface IterableDecoderConfig {
  type: "Iterable";
}

interface CSVDecoderConfig {
  type: "CSV";
  delimiter?: string;
  encoding?: string;
}

export type BuilderDecoderConfig =
  | JSONDecoderConfig
  | XMLDecoderConfig
  | JSONLinesDecoderConfig
  | IterableDecoderConfig
  | CSVDecoderConfig;

// Intermediary mapping of builder -> manifest decoder types
// TODO: find a way to abstract/simplify the decoder manifest -> UI typing so we don't have so many places to update when adding new decoders
const DECODER_TYPE_MAP: Record<(typeof BUILDER_DECODER_TYPES)[number], ManifestDecoderType> = {
  JSON: "JsonDecoder",
  XML: "XmlDecoder",
  "JSON Lines": "JsonlDecoder",
  Iterable: "IterableDecoder",
  CSV: "CsvDecoder",
} as const;

// Registry of decoder configurations. Additional configurations can be added here as more decoders are supported.
export const DECODER_CONFIGS: Partial<Record<(typeof BUILDER_DECODER_TYPES)[number], DecoderTypeConfig>> = {
  CSV: {
    title: "connectorBuilder.decoder.csvDecoder.label",
    fields: [
      {
        key: "delimiter",
        type: "string",
        label: "connectorBuilder.decoder.csvDecoder.delimiter.label",
        tooltip: "connectorBuilder.decoder.csvDecoder.delimiter.tooltip",
        manifestPath: "CsvDecoder.properties.delimiter",
        placeholder: ",",
        optional: true,
      },
      {
        key: "encoding",
        type: "string",
        label: "connectorBuilder.decoder.csvDecoder.encoding.label",
        tooltip: "connectorBuilder.decoder.csvDecoder.encoding.tooltip",
        manifestPath: "CsvDecoder.properties.encoding",
        placeholder: "utf-8",
        optional: true,
      },
    ],
  },
};

interface BuilderRequestOptions {
  requestParameters: Array<[string, string]>;
  requestHeaders: Array<[string, string]>;
  requestBody: BuilderRequestBody;
}

export const BUILDER_SESSION_TOKEN_AUTH_DECODER_TYPES = ["JSON", "XML"] as const;
export type BuilderSessionTokenAuthenticator = Omit<SessionTokenAuthenticator, "login_requester" | "decoder"> & {
  login_requester: {
    url: string;
    authenticator: NoAuth | ApiKeyAuthenticator | BearerAuthenticator | BasicHttpAuthenticator;
    httpMethod: BuilderHttpMethod;
    requestOptions: BuilderRequestOptions;
    errorHandler?: BuilderErrorHandler[];
  };
  decoder: (typeof BUILDER_SESSION_TOKEN_AUTH_DECODER_TYPES)[number];
};

export type BuilderFormAuthenticator =
  | NoAuth
  | BuilderFormOAuthAuthenticator
  | BuilderFormDeclarativeOAuthAuthenticator
  | ApiKeyAuthenticator
  | BearerAuthenticator
  | BasicHttpAuthenticator
  | BuilderJwtAuthenticator
  | BuilderSessionTokenAuthenticator;

export type BuilderJwtAuthenticator = Omit<JwtAuthenticator, "additional_jwt_headers" | "additional_jwt_headers"> & {
  additional_jwt_headers?: Array<[string, string]>;
  additional_jwt_payload?: Array<[string, string]>;
};

export const DeclarativeOAuthAuthenticatorType = "DeclarativeOAuthAuthenticator" as const;

export type BuilderFormDeclarativeOAuthAuthenticator = Omit<BuilderFormOAuthAuthenticator, "type"> & {
  type: typeof DeclarativeOAuthAuthenticatorType;
  declarative: Omit<OAuthConfigSpecificationOauthConnectorInputSpecification, "extract_output"> & {
    access_token_key: string; // extract_output is partially generated from this
  };
};

const isAuthenticatorDeclarativeOAuth = (
  authenticator: BuilderFormValues["global"]["authenticator"]
): authenticator is BuilderFormDeclarativeOAuthAuthenticator =>
  typeof authenticator === "object" && authenticator.type === DeclarativeOAuthAuthenticatorType;

export type BuilderFormOAuthAuthenticator = Omit<
  OAuthAuthenticator,
  "type" | "refresh_request_body" | "refresh_token_updater"
> & {
  type: OAuthAuthenticatorType | typeof DeclarativeOAuthAuthenticatorType;
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
    authenticator: BuilderFormAuthenticator | YamlString;
  };
  assist: AssistData;
  inputs: BuilderFormInput[];
  streams: BuilderStream[];
  checkStreams: string[];
  version: string;
  description?: string;
}

export interface StreamTestResults {
  streamHash: string | null;
  hasResponse?: boolean;
  responsesAreSuccessful?: boolean;
  hasRecords?: boolean;
  primaryKeysArePresent?: boolean;
  primaryKeysAreUnique?: boolean;
}

type TestedStreams = Record<string, StreamTestResults>;

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
  incremental_dependency?: boolean;
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
  backoff_strategy?:
    | (Omit<ConstantBackoffStrategy, "backoff_time_in_seconds"> & { backoff_time_in_seconds: number })
    | (Omit<ExponentialBackoffStrategy, "factor"> & { factor?: number })
    | (Omit<WaitUntilTimeFromHeader, "min_wait"> & { min_wait?: number })
    | WaitTimeFromHeader;
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
    }
  | {
      type: "graphql";
      value: string;
    };

export interface BuilderRecordSelector {
  fieldPath: string[];
  filterCondition?: string;
  normalizeToSchema: boolean;
}

export type YamlString = string;
export const isYamlString = (value: unknown): value is YamlString => isString(value);

export interface BuilderStream {
  id: string;
  name: string;
  urlPath: string;
  primaryKey: string[];
  httpMethod: BuilderHttpMethod;
  decoder: BuilderDecoderConfig;
  requestOptions: BuilderRequestOptions;
  recordSelector?: BuilderRecordSelector | YamlString;
  paginator?: BuilderPaginator | YamlString;
  transformations?: BuilderTransformation[] | YamlString;
  incrementalSync?: BuilderIncrementalSync | YamlString;
  parentStreams?: BuilderParentStream[] | YamlString;
  parameterizedRequests?: BuilderParameterizedRequests[] | YamlString;
  errorHandler?: BuilderErrorHandler[] | YamlString;
  schema?: string;
  autoImportSchema: boolean;
  unknownFields?: YamlString;
  testResults?: StreamTestResults;
}

type StreamName = string;

export interface YamlSupportedComponentName {
  stream:
    | "paginator"
    | "errorHandler"
    | "transformations"
    | "incrementalSync"
    | "recordSelector"
    | "parameterizedRequests"
    | "parentStreams";
  global: "authenticator";
}

export interface BuilderMetadata {
  autoImportSchema: Record<StreamName, boolean>;
  yamlComponents?: {
    streams?: Record<StreamName, Array<YamlSupportedComponentName["stream"]>>;
    global?: Array<YamlSupportedComponentName["global"]>;
  };
  testedStreams?: TestedStreams;
  assist?: AssistData;
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
  assist: {
    docsUrl: "",
    openapiSpecUrl: "",
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
  primaryKey: [],
  httpMethod: "GET",
  decoder: { type: "JSON" },
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
  unknownFields: undefined,
};

export const BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE = "manifest-only";

export const LIST_PARTITION_ROUTER: ListPartitionRouterType = "ListPartitionRouter";
export const SUBSTREAM_PARTITION_ROUTER: SubstreamPartitionRouterType = "SubstreamPartitionRouter";
export const CUSTOM_PARTITION_ROUTER: CustomPartitionRouterType = "CustomPartitionRouter";

export const NO_AUTH: NoAuthType = "NoAuth";
export const API_KEY_AUTHENTICATOR: ApiKeyAuthenticatorType = "ApiKeyAuthenticator";
export const BEARER_AUTHENTICATOR: BearerAuthenticatorType = "BearerAuthenticator";
export const BASIC_AUTHENTICATOR: BasicHttpAuthenticatorType = "BasicHttpAuthenticator";
export const JWT_AUTHENTICATOR: JwtAuthenticatorType = "JwtAuthenticator";
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
  builderValue: BuilderInput | YamlString,
  convertFn: (builderValue: BuilderInput) => ManifestOutput
): ManifestOutput {
  if (isYamlString(builderValue)) {
    return load(builderValue) as ManifestOutput;
  }
  return convertFn(builderValue);
}

export function builderAuthenticatorToManifest(
  authenticator: BuilderFormAuthenticator
): HttpRequesterAuthenticator | undefined {
  if (authenticator.type === "NoAuth") {
    return undefined;
  }
  if (authenticator.type === OAUTH_AUTHENTICATOR || authenticator.type === DeclarativeOAuthAuthenticatorType) {
    const isRefreshTokenFlowEnabled = !!authenticator.refresh_token_updater;
    const { access_token, token_expiry_date, ...refresh_token_updater } = authenticator.refresh_token_updater ?? {};
    return {
      ...omit(authenticator, "declarative", "type"),
      type: OAUTH_AUTHENTICATOR,
      refresh_token: authenticator.grant_type === "client_credentials" ? undefined : authenticator.refresh_token,
      refresh_token_updater:
        authenticator.grant_type === "client_credentials" || !authenticator.refresh_token_updater
          ? undefined
          : authenticator.type === DeclarativeOAuthAuthenticatorType && isRefreshTokenFlowEnabled
          ? {
              ...refresh_token_updater,
              refresh_token_config_path: [extractInterpolatedConfigKey(authenticator.refresh_token!)],
            }
          : {
              ...refresh_token_updater,
              access_token_config_path: [
                extractInterpolatedConfigKey(authenticator.refresh_token_updater.access_token),
              ],
              token_expiry_date_config_path: [
                extractInterpolatedConfigKey(authenticator.refresh_token_updater.token_expiry_date),
              ],
              refresh_token_config_path: [extractInterpolatedConfigKey(authenticator.refresh_token!)],
            },
      refresh_request_body: Object.fromEntries(authenticator.refresh_request_body),
    } satisfies OAuthAuthenticator;
  }
  if (authenticator.type === "ApiKeyAuthenticator") {
    const convertedInjectInto = authenticator.inject_into
      ? convertRequestOptionFieldPathToLegacyFieldName(authenticator.inject_into)
      : undefined;

    return {
      ...authenticator,
      header: undefined,
      api_token: authenticator.api_token,
      inject_into: convertedInjectInto,
    };
  }
  if (authenticator.type === "BearerAuthenticator") {
    return {
      ...authenticator,
      api_token: authenticator.api_token,
    };
  }
  if (authenticator.type === "BasicHttpAuthenticator") {
    return {
      ...authenticator,
      username: authenticator.username,
      password: authenticator.password,
    };
  }
  if (authenticator.type === "JwtAuthenticator") {
    return {
      ...authenticator,
      secret_key: authenticator.secret_key,
      additional_jwt_headers: fromEntriesOrUndefined(authenticator.additional_jwt_headers ?? []),
      additional_jwt_payload: fromEntriesOrUndefined(authenticator.additional_jwt_payload ?? []),
    };
  }
  if (authenticator.type === "SessionTokenAuthenticator") {
    const builderLoginRequester = authenticator.login_requester;
    const { base, path } = splitUrl(builderLoginRequester.url ?? "");
    return {
      ...authenticator,
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
      decoder: authenticator.decoder === "XML" ? { type: XmlDecoderType.XmlDecoder } : undefined,
    };
  }
  return authenticator as HttpRequesterAuthenticator;
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
    pageTokenOption = convertRequestOptionFieldPathToLegacyFieldName(paginator.pageTokenOption);
  }

  let pageSizeOption: RequestOption | undefined;
  if (paginator.strategy.page_size && paginator.pageSizeOption) {
    pageSizeOption = convertRequestOptionFieldPathToLegacyFieldName(paginator.pageSizeOption);
  }

  return {
    type: "DefaultPaginator",
    page_token_option: pageTokenOption,
    page_size_option: pageSizeOption,
    pagination_strategy: builderPaginationStrategyToManifest(paginator.strategy),
  };
}

function convertRequestOptionFieldPathToLegacyFieldName(option: RequestOptionOrPathInject): RequestOption {
  // We are introducing a field_path field (type: string[]) to the RequestOption type, to support nested field injection.
  // Eventually, we should deprecate the existing field_name field (type: string), since field_path is more flexible.
  // However, because existing builder projects already use field_name and we trigger stream change warnings on any schema change,
  // we need to support both fields for now to avoid triggering unnecessary and potentially confusing warnings.

  // This function converts a single-element field_path in the UI to field_name in the YAML manifest:
  // RequestOption.field_path: ['page_size'] -> RequestOption.field_name: 'page_size'
  // TODO: Remove this function once we are ready to fully deprecate RequestOption.field_name
  if ("inject_into" in option && option.inject_into === "path") {
    throw new Error("Cannot convert path injection to manifest");
  }

  if (option.field_path && option.field_path.length === 1) {
    return {
      type: RequestOptionType.RequestOption,
      inject_into: option.inject_into,
      field_name: option.field_path[0],
    };
  }
  return {
    type: RequestOptionType.RequestOption,
    ...option,
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

  const startTimeOption = start_time_option
    ? convertRequestOptionFieldPathToLegacyFieldName(start_time_option)
    : undefined;
  const endTimeOption = end_time_option ? convertRequestOptionFieldPathToLegacyFieldName(end_time_option) : undefined;

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
      start_time_option: startTimeOption,
      end_time_option: endTimeOption,
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
      start_time_option: startTimeOption,
    };
  }
  return {
    ...manifestIncrementalSync,
    is_data_feed: true,
  };
}

export function builderParameterizedRequestsToManifest(
  parameterizedRequests: BuilderParameterizedRequests[] | undefined
): ListPartitionRouter[] {
  if (!parameterizedRequests || parameterizedRequests.length === 0) {
    return [];
  }

  return parameterizedRequests.map((parameterizedRequest) => {
    const request = {
      ...parameterizedRequest,
      values: parameterizedRequest.values.value,
    };

    if (request.request_option) {
      request.request_option = convertRequestOptionFieldPathToLegacyFieldName(request.request_option);
    }

    return request;
  });
}

export function builderParentStreamsToManifest(
  parentStreams: BuilderParentStream[] | undefined,
  builderStreams: BuilderStream[]
): SubstreamPartitionRouter[] {
  if (!parentStreams || parentStreams.length === 0) {
    return [];
  }

  const substreamPartitionRouters: SubstreamPartitionRouter[] = parentStreams
    .map((parentStreamConfiguration): SubstreamPartitionRouter | undefined => {
      const parentStream = builderStreams.find(({ id }) => id === parentStreamConfiguration.parentStreamReference);
      if (!parentStream) {
        return undefined;
      }
      return {
        type: "SubstreamPartitionRouter",
        parent_stream_configs: [
          {
            type: "ParentStreamConfig",
            parent_key: parentStreamConfiguration.parent_key,
            request_option: parentStreamConfiguration.request_option
              ? convertRequestOptionFieldPathToLegacyFieldName(parentStreamConfiguration.request_option)
              : undefined,
            partition_field: parentStreamConfiguration.partition_field,
            stream: streamRef(parentStream.name),
            incremental_dependency: parentStreamConfiguration.incremental_dependency ? true : undefined,
          },
        ],
      };
    })
    .filter(
      (substreamPartitionRouter): substreamPartitionRouter is SubstreamPartitionRouter =>
        substreamPartitionRouter !== undefined
    );

  return substreamPartitionRouters;
}

function combinePartitionRouters(
  ...partitionRouters: SimpleRetrieverPartitionRouterAnyOfItem[]
): SimpleRetrieverPartitionRouter | undefined {
  // filter out anything that doesn't have a supported type
  const filteredPartitionRouters = filterPartitionRouterToType(partitionRouters, [
    "ListPartitionRouter",
    "SubstreamPartitionRouter",
    "CustomPartitionRouter",
  ]);
  if (!filteredPartitionRouters || filteredPartitionRouters.length === 0) {
    return undefined;
  }
  if (filteredPartitionRouters.length === 1) {
    return filteredPartitionRouters[0];
  }
  return filteredPartitionRouters;
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
      max_retries: handler.max_retries ? Number(handler.max_retries) : undefined,
      backoff_strategies: match(handler.backoff_strategy)
        .with(undefined, () => undefined)
        // must explicitly cast fields that have "number | string" type in the declarative schema
        // to "number", or else they will end up as strings when dumping YAML
        .with({ type: "ConstantBackoffStrategy" }, (strategy) => [
          {
            ...strategy,
            backoff_time_in_seconds: Number(strategy.backoff_time_in_seconds),
          },
        ])
        .with({ type: "ExponentialBackoffStrategy" }, (strategy) => [
          {
            ...strategy,
            factor: strategy.factor ? Number(strategy.factor) : undefined,
          },
        ])
        .with({ type: "WaitUntilTimeFromHeader" }, (strategy) => [
          {
            ...strategy,
            min_wait: strategy.min_wait ? Number(strategy.min_wait) : undefined,
          },
        ])
        .otherwise((strategy) => [strategy]),
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
    const parsedJson = builderRequestBody.type === "json_freeform" ? JSON.parse(builderRequestBody.value) : undefined;

    const requestBody = {
      request_body_json:
        builderRequestBody.type === "json_list"
          ? fromEntriesOrUndefined(builderRequestBody.values)
          : builderRequestBody.type === "json_freeform"
          ? isString(parsedJson)
            ? undefined
            : Object.keys(parsedJson).length > 0
            ? parsedJson
            : undefined
          : builderRequestBody.type === "graphql"
          ? { query: builderRequestBody.value.replace(/\s+/g, " ").trim() }
          : undefined,
      request_body_data:
        builderRequestBody.type === "form_list"
          ? fromEntriesOrUndefined(builderRequestBody.values)
          : builderRequestBody.type === "string_freeform"
          ? builderRequestBody.value
          : builderRequestBody.type === "json_freeform" && isString(parsedJson)
          ? parsedJson
          : undefined,
    };
    return Object.keys(requestBody).length > 0 ? requestBody : undefined;
  } catch {
    return undefined;
  }
}

export function builderRecordSelectorToManifest(recordSelector: BuilderRecordSelector | undefined): RecordSelector {
  const defaultRecordSelector: RecordSelector = {
    type: "RecordSelector",
    extractor: {
      type: "DpathExtractor",
      field_path: [],
    },
  };
  if (!recordSelector) {
    return defaultRecordSelector;
  }
  return merge(defaultRecordSelector, {
    extractor: {
      field_path: recordSelector.fieldPath,
    },
    record_filter: recordSelector.filterCondition
      ? {
          type: "RecordFilter",
          condition: recordSelector.filterCondition,
        }
      : undefined,
    schema_normalization: recordSelector.normalizeToSchema ? "Default" : undefined,
  });
}

const builderDecoderToManifest = (decoder: BuilderDecoderConfig): SimpleRetrieverDecoder | undefined => {
  // No decoder is specified for JSON responses
  if (decoder.type === "JSON") {
    return undefined;
  }

  if (decoder.type === "CSV") {
    const result: SimpleRetrieverDecoder = {
      type: "CsvDecoder" as const,
    };

    if (decoder.delimiter) {
      result.delimiter = decoder.delimiter;
    }

    if (decoder.encoding) {
      result.encoding = decoder.encoding;
    }

    return result;
  }

  return { type: DECODER_TYPE_MAP[decoder.type] };
};

type BaseRequester = Pick<HttpRequester, "type" | "url_base" | "authenticator">;

function builderStreamToDeclarativeSteam(stream: BuilderStream, allStreams: BuilderStream[]): DeclarativeStream {
  // cast to tell typescript which properties will be present after resolving the ref
  const requesterRef = {
    $ref: "#/definitions/base_requester",
  } as unknown as BaseRequester;

  const parentStreamsToManifest = (parentStreams: BuilderParentStream[] | undefined) =>
    builderParentStreamsToManifest(parentStreams, allStreams);

  return merge({} as DeclarativeStream, stream.unknownFields ? load(stream.unknownFields) : {}, {
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
      record_selector: convertOrLoadYamlString(stream.recordSelector, builderRecordSelectorToManifest),
      paginator: convertOrLoadYamlString(stream.paginator, builderPaginatorToManifest),
      partition_router: combinePartitionRouters(
        ...convertOrLoadYamlString(stream.parameterizedRequests, builderParameterizedRequestsToManifest),
        ...convertOrLoadYamlString(stream.parentStreams, parentStreamsToManifest)
      ),
      decoder: builderDecoderToManifest(stream.decoder),
    },
    incremental_sync: convertOrLoadYamlString(stream.incrementalSync, builderIncrementalSyncToManifest),
    transformations: convertOrLoadYamlString(stream.transformations, builderTransformationsToManifest),
    schema_loader: { type: "InlineSchemaLoader", schema: schemaRef(stream.name) },
  });
}

export const builderFormValuesToMetadata = (values: BuilderFormValues): BuilderMetadata => {
  const componentNameIfString = <
    ComponentName extends YamlSupportedComponentName["stream"] | YamlSupportedComponentName["global"],
  >(
    componentName: ComponentName,
    value: unknown
  ) => (isYamlString(value) ? [componentName] : []);

  const yamlComponentsPerStream = {} as Record<string, Array<YamlSupportedComponentName["stream"]>>;
  const testedStreams = {} as TestedStreams;
  values.streams.forEach((stream) => {
    const yamlComponents = [
      ...componentNameIfString("paginator", stream.paginator),
      ...componentNameIfString("errorHandler", stream.errorHandler),
      ...componentNameIfString("transformations", stream.transformations),
      ...componentNameIfString("incrementalSync", stream.incrementalSync),
      ...componentNameIfString("recordSelector", stream.recordSelector),
      ...componentNameIfString("parameterizedRequests", stream.parameterizedRequests),
      ...componentNameIfString("parentStreams", stream.parentStreams),
    ];
    if (yamlComponents.length > 0) {
      yamlComponentsPerStream[stream.name] = yamlComponents;
    }

    if (stream.testResults) {
      testedStreams[stream.name] = stream.testResults;
    }
  });
  const hasStreamYamlComponents = Object.keys(yamlComponentsPerStream).length > 0;

  const globalYamlComponents = [...componentNameIfString("authenticator", values.global.authenticator)];
  const hasGlobalYamlComponents = globalYamlComponents.length > 0;

  const assistData = values.assist ?? {};

  return {
    autoImportSchema: Object.fromEntries(values.streams.map((stream) => [stream.name, stream.autoImportSchema])),
    ...((hasStreamYamlComponents || hasGlobalYamlComponents) && {
      yamlComponents: {
        ...(hasStreamYamlComponents && {
          streams: yamlComponentsPerStream,
        }),
        ...(hasGlobalYamlComponents && {
          global: globalYamlComponents,
        }),
      },
    }),
    testedStreams,
    assist: assistData,
  };
};

export const addDeclarativeOAuthAuthenticatorToSpec = (
  spec: Spec,
  authenticator: BuilderFormDeclarativeOAuthAuthenticator
): Spec => {
  const updatedSpec = structuredClone(spec);

  const isRefreshTokenFlowEnabled = !!authenticator.refresh_token_updater;
  const accessTokenKey = authenticator.declarative.access_token_key;

  const accessTokenConfigPath = extractInterpolatedConfigKey(authenticator.access_token_value);
  const refreshTokenConfigPath = extractInterpolatedConfigKey(authenticator.refresh_token);

  updatedSpec.advanced_auth = {
    auth_flow_type: "oauth2.0",
    oauth_config_specification: {
      oauth_connector_input_specification: {
        ...omit(authenticator.declarative, [
          "access_token_key",
          "access_token_headers",
          "access_token_params",
          "state",
        ]),
        extract_output: isRefreshTokenFlowEnabled
          ? [authenticator.declarative.access_token_key, "refresh_token"]
          : [authenticator.declarative.access_token_key],
        state: authenticator.declarative.state ? JSON.parse(authenticator.declarative.state) : undefined,

        access_token_headers: authenticator.declarative.access_token_headers
          ? Object.fromEntries(authenticator.declarative.access_token_headers)
          : undefined,

        access_token_params: authenticator.declarative.access_token_params
          ? Object.fromEntries(authenticator.declarative.access_token_params)
          : undefined,
      } as OAuthConfigSpecificationOauthConnectorInputSpecification,
      complete_oauth_output_specification: {
        required: isRefreshTokenFlowEnabled ? [accessTokenKey, "refresh_token"] : [accessTokenKey],
        properties: {
          [accessTokenKey]: {
            type: "string",
            path_in_connector_config: [accessTokenConfigPath],
          },
          ...(isRefreshTokenFlowEnabled
            ? {
                refresh_token: {
                  type: "string",
                  path_in_connector_config: [refreshTokenConfigPath],
                },
              }
            : {}),
        },
      },
      complete_oauth_server_input_specification: {
        required: ["client_id", "client_secret"],
        properties: {
          client_id: {
            type: "string",
          },
          client_secret: {
            type: "string",
          },
        },
      },
      complete_oauth_server_output_specification: {
        required: ["client_id", "client_secret"],
        properties: {
          client_id: {
            type: "string",
            path_in_connector_config: ["client_id"],
          },
          client_secret: {
            type: "string",
            path_in_connector_config: ["client_secret"],
          },
        },
      },
    },
  };

  return updatedSpec;
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
    builderStreamToDeclarativeSteam(stream, values.streams)
  );

  const streamNames = values.streams.map((s) => s.name);
  const validCheckStreamNames = (values.checkStreams ?? []).filter((checkStream) => streamNames.includes(checkStream));
  const correctedCheckStreams =
    validCheckStreamNames.length > 0 ? validCheckStreamNames : streamNames.length > 0 ? [streamNames[0]] : [];

  const streamNameToStream = Object.fromEntries(manifestStreams.map((stream) => [stream.name, stream]));
  const streamRefs = manifestStreams.map((stream) => streamRef(stream.name!));

  const streamNameToSchema = Object.fromEntries(
    values.streams.map((stream) => {
      let schema;
      if (stream.schema) {
        try {
          schema = JSON.parse(stream.schema);
        } catch (e) {
          schema = JSON.parse(DEFAULT_SCHEMA);
        }
      } else {
        schema = JSON.parse(DEFAULT_SCHEMA);
      }
      schema.additionalProperties = true;
      return [stream.name, schema];
    })
  );

  const baseRequester: BaseRequester = {
    type: "HttpRequester",
    url_base: values.global?.urlBase?.trim(),
    authenticator: convertOrLoadYamlString(values.global.authenticator, builderAuthenticatorToManifest),
  };

  let spec = builderInputsToSpec(values.inputs);
  if (isAuthenticatorDeclarativeOAuth(values.global.authenticator)) {
    spec = addDeclarativeOAuthAuthenticatorToSpec(spec, values.global.authenticator);
  }

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
    spec,
    metadata: builderFormValuesToMetadata(values),
    description: values.description,
  };
};

function schemaRef(streamName: string) {
  return { $ref: `#/schemas/${streamName}` };
}

export const DEFAULT_JSON_MANIFEST_VALUES: ConnectorManifest = convertToManifest(DEFAULT_BUILDER_FORM_VALUES);
export const DEFAULT_JSON_MANIFEST_STREAM: DeclarativeStream = {
  type: "DeclarativeStream",
  retriever: {
    type: "SimpleRetriever",
    record_selector: {
      type: "RecordSelector",
      extractor: {
        type: "DpathExtractor",
        field_path: [],
      },
    },
    requester: {
      type: "HttpRequester",
      url_base: "",
      authenticator: undefined,
      path: "",
      http_method: "GET",
    },
    paginator: undefined,
  },
  primary_key: undefined,
};

export type StreamPathFn = <T extends string>(fieldPath: T) => `formValues.streams.${number}.${T}`;

export const concatPath = <TBase extends string, TPath extends string>(base: TBase, path: TPath) =>
  `${base}.${path}` as const;
