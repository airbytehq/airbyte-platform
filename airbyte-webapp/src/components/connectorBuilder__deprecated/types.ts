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
  DynamicStreamCheckConfig,
  DynamicStreamCheckConfigType,
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
  XmlDecoderType,
  JwtAuthenticator,
  JwtAuthenticatorType,
  RequestOptionType,
  GzipDecoderDecoder,
  ZipfileDecoderDecoder,
  JsonDecoderType,
  JsonlDecoderType,
  IterableDecoderType,
  AddedFieldDefinitionType,
  AsyncRetrieverType,
  DeclarativeStreamType,
  SimpleRetrieverType,
  InlineSchemaLoaderType,
  SimpleRetrieverDecoder,
  HttpRequesterType,
  AsyncJobStatusMap,
  DpathExtractorType,
  DpathExtractor,
  SimpleRetriever,
  DynamicDeclarativeStream,
  DynamicDeclarativeStreamType,
} from "core/api/types/ConnectorManifest";

import { DecoderTypeConfig } from "./Builder/DecoderConfig";
import { CDK_VERSION } from "./cdk";
import { filterPartitionRouterToType, formatJson, streamRef } from "./utils";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";

export interface GeneratedStreamId {
  type: "generated_stream";
  index: number;
  dynamicStreamName: string;
}

export interface StaticStreamId {
  type: "stream";
  index: number;
}

export interface DynamicStreamId {
  type: "dynamic_stream";
  index: number;
}

export type StreamId = StaticStreamId | GeneratedStreamId | DynamicStreamId;

export interface BuilderState {
  name: string;
  mode: "ui" | "yaml";
  formValues: BuilderFormValues;
  previewValues?: BuilderFormValues;
  yaml: string;
  customComponentsCode?: string;
  view: { type: "global" } | { type: "inputs" } | { type: "components" } | StreamId;
  streamTab: BuilderStreamTab;
  testStreamId: StreamId;
  testingValues: ConnectorBuilderProjectTestingValues | undefined;
  manifest: ConnectorManifest | null;
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

export type BuilderStreamTab = "requester" | "schema" | "polling" | "download";

type BuilderHttpMethod = "GET" | "POST";

export const BUILDER_DECODER_TYPES = ["JSON", "XML", "JSON Lines", "Iterable", "CSV", "gzip", "ZIP file"] as const;

export type ManifestDecoderType =
  | "JsonDecoder"
  | "XmlDecoder"
  | "JsonlDecoder"
  | "IterableDecoder"
  | "CsvDecoder"
  | "GzipDecoder"
  | "ZipfileDecoder";

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

interface GzipDecoderConfig {
  type: "gzip";
  decoder: BuilderNestedDecoderConfig;
}

interface ZipfileDecoderConfig {
  type: "ZIP file";
  decoder: BuilderNestedDecoderConfig;
}

export type BuilderDecoderConfig =
  | JSONDecoderConfig
  | XMLDecoderConfig
  | JSONLinesDecoderConfig
  | IterableDecoderConfig
  | CSVDecoderConfig
  | GzipDecoderConfig
  | ZipfileDecoderConfig;

export type BuilderNestedDecoderConfig =
  | JSONDecoderConfig
  | CSVDecoderConfig
  | GzipDecoderConfig
  | JSONLinesDecoderConfig;

// Intermediary mapping of builder -> manifest decoder types
// TODO: find a way to abstract/simplify the decoder manifest -> UI typing so we don't have so many places to update when adding new decoders
const DECODER_TYPE_MAP: Record<(typeof BUILDER_DECODER_TYPES)[number], ManifestDecoderType> = {
  JSON: "JsonDecoder",
  XML: "XmlDecoder",
  "JSON Lines": "JsonlDecoder",
  Iterable: "IterableDecoder",
  CSV: "CsvDecoder",
  gzip: "GzipDecoder",
  "ZIP file": "ZipfileDecoder",
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
  gzip: {
    title: "connectorBuilder.decoder.gzipDecoder.label",
    fields: [
      {
        key: "decoder",
        type: "string",
        label: "connectorBuilder.decoder.nestedDecoder.label",
        tooltip: "connectorBuilder.decoder.nestedDecoder.tooltip",
        optional: false,
      },
    ],
  },
  "ZIP file": {
    title: "connectorBuilder.decoder.zipfileDecoder.label",
    fields: [
      {
        key: "decoder",
        type: "string",
        label: "connectorBuilder.decoder.nestedDecoder.label",
        tooltip: "connectorBuilder.decoder.nestedDecoder.tooltip",
        optional: false,
      },
    ],
  },
};

export interface BuilderRequestOptions {
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
    urlBase?: string;
    authenticator: BuilderFormAuthenticator | YamlString;
  };
  assist: AssistData;
  inputs: BuilderFormInput[];
  streams: BuilderStream[];
  dynamicStreams: BuilderDynamicStream[];
  generatedStreams: Record<string, GeneratedBuilderStream[]>;
  checkStreams: string[];
  dynamicStreamCheckConfigs: DynamicStreamCheckConfig[];
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

export interface BuilderDpathExtractor {
  type: DpathExtractorType;
  field_path: string[];
}

export type YamlString = string;
export const isYamlString = (value: unknown): value is YamlString => isString(value);

export type BuilderPollingTimeout =
  | {
      type: "number";
      value: number;
    }
  | {
      type: "custom";
      value: string;
    };

interface BuilderHttpComponentsResolver {
  type: "HttpComponentsResolver";
  retriever: SimpleRetriever;
}
export type BuilderComponentsResolver = BuilderHttpComponentsResolver;

export interface BuilderDynamicStream {
  streamTemplate: BuilderStream;
  dynamicStreamName: string;
  componentsResolver: BuilderComponentsResolver;
}

export type BuilderStream = {
  id: string;
  name: string;
  schema?: string;
  autoImportSchema: boolean;
  unknownFields?: YamlString;
  testResults?: StreamTestResults;
  dynamicStreamName?: string;
} & (
  | {
      requestType: "sync";
      urlPath?: string;
      httpMethod: BuilderHttpMethod;
      decoder: BuilderDecoderConfig;
      primaryKey: string[];
      requestOptions: BuilderRequestOptions;
      incrementalSync?: BuilderIncrementalSync | YamlString;
      transformations?: BuilderTransformation[] | YamlString;
      recordSelector?: BuilderRecordSelector | YamlString;
      paginator?: BuilderPaginator | YamlString;
      parentStreams?: BuilderParentStream[] | YamlString;
      parameterizedRequests?: BuilderParameterizedRequests[] | YamlString;
      errorHandler?: BuilderErrorHandler[] | YamlString;
    }
  | {
      requestType: "async";
      creationRequester: BuilderBaseRequester & {
        incrementalSync?: BuilderIncrementalSync | YamlString;
        parentStreams?: BuilderParentStream[] | YamlString;
        parameterizedRequests?: BuilderParameterizedRequests[] | YamlString;
        decoder: BuilderDecoderConfig;
      };
      pollingRequester: BuilderBaseRequester & {
        statusExtractor: BuilderDpathExtractor;
        statusMapping: AsyncJobStatusMap;
        downloadTargetExtractor: BuilderDpathExtractor;
        pollingTimeout: BuilderPollingTimeout;
      };
      downloadRequester: BuilderBaseRequester & {
        decoder: BuilderDecoderConfig;
        primaryKey: string[];
        transformations?: BuilderTransformation[] | YamlString;
        recordSelector?: BuilderRecordSelector | YamlString;
        paginator?: BuilderPaginator | YamlString;
        downloadExtractor?: BuilderDpathExtractor;
      };
      // TODO(async): add support for abort/delete/url requesters
      // abortRequester: BuilderBaseRequester;
      // deleteRequester: BuilderBaseRequester;
      // urlRequester: BuilderBaseRequester;
    }
);

export type GeneratedDeclarativeStream = DeclarativeStream & {
  dynamic_stream_name: string;
};

export type GeneratedBuilderStream = BuilderStream & {
  dynamicStreamName: string;
  declarativeStream: GeneratedDeclarativeStream;
};

export function declarativeStreamIsGenerated(stream: DeclarativeStream): stream is GeneratedDeclarativeStream {
  return "dynamic_stream_name" in stream;
}

export interface BuilderBaseRequester {
  url: string;
  httpMethod: BuilderHttpMethod;
  requestOptions: BuilderRequestOptions;
  errorHandler?: BuilderErrorHandler[] | YamlString;
  authenticator: BuilderFormAuthenticator | YamlString;
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
    | "parentStreams"
    | "authenticator";
  global: "authenticator";
}

export interface BuilderMetadata {
  autoImportSchema: Record<StreamName, boolean>;
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
    authenticator: { type: "NoAuth" },
  },
  assist: {
    docsUrl: "",
    openapiSpecUrl: "",
  },
  inputs: [],
  streams: [],
  dynamicStreams: [],
  generatedStreams: {},
  checkStreams: [],
  dynamicStreamCheckConfigs: [],
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

const DEFAULT_REQUEST_OPTIONS: BuilderRequestOptions = {
  requestParameters: [],
  requestHeaders: [],
  requestBody: {
    type: "json_list",
    values: [],
  },
};

export const DEFAULT_BUILDER_STREAM_VALUES: BuilderStream = {
  requestType: "sync" as const,
  id: "",
  name: "",
  urlPath: "",
  primaryKey: [],
  httpMethod: "GET",
  decoder: { type: "JSON" },
  schema: DEFAULT_SCHEMA,
  requestOptions: DEFAULT_REQUEST_OPTIONS,
  autoImportSchema: true,
  unknownFields: undefined,
};

export const DEFAULT_BUILDER_ASYNC_STREAM_VALUES: BuilderStream = {
  requestType: "async" as const,
  id: "",
  name: "",
  autoImportSchema: true,
  unknownFields: undefined,
  creationRequester: {
    url: "",
    httpMethod: "POST",
    requestOptions: DEFAULT_REQUEST_OPTIONS,
    authenticator: { type: "NoAuth" },
    decoder: { type: "JSON" },
  },
  pollingRequester: {
    url: "",
    httpMethod: "GET",
    requestOptions: DEFAULT_REQUEST_OPTIONS,
    authenticator: { type: "NoAuth" },
    statusExtractor: {
      type: DpathExtractorType.DpathExtractor,
      field_path: [],
    },
    statusMapping: {
      completed: [],
      failed: [],
      running: [],
      timeout: [],
    },
    downloadTargetExtractor: {
      type: DpathExtractorType.DpathExtractor,
      field_path: [],
    },
    pollingTimeout: { type: "number", value: 15 },
  },
  downloadRequester: {
    url: "",
    httpMethod: "GET",
    requestOptions: DEFAULT_REQUEST_OPTIONS,
    authenticator: { type: "NoAuth" },
    decoder: { type: "JSON" },
    primaryKey: [],
  },
};

export const URL_BASE_PLACEHOLDER = "/";

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
  return streams.some((stream) => {
    const incrementalSync =
      stream.requestType === "sync" ? stream.incrementalSync : stream.creationRequester.incrementalSync;
    return (
      !isYamlString(incrementalSync) &&
      incrementalSync?.[key]?.type === "user_input" &&
      (key === "start_datetime" || incrementalSync?.filter_mode === "range")
    );
  });
}

export function isStreamDynamicStream(streamId: StreamId): boolean {
  return streamId.type === "dynamic_stream";
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

    const usesRefreshToken = authenticator.type !== DeclarativeOAuthAuthenticatorType || isRefreshTokenFlowEnabled;

    return {
      ...omit(authenticator, "declarative", "type", "grant_type"),
      type: OAUTH_AUTHENTICATOR,
      grant_type: usesRefreshToken ? authenticator.grant_type : "client_credentials",
      refresh_token:
        authenticator.grant_type === "client_credentials" || !usesRefreshToken
          ? undefined
          : authenticator.refresh_token,
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
      refresh_request_body: !usesRefreshToken ? undefined : Object.fromEntries(authenticator.refresh_request_body),
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
    return {
      ...authenticator,
      login_requester: {
        type: "HttpRequester",
        url_base: builderLoginRequester.url,
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
  // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
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
            type: AddedFieldDefinitionType.AddedFieldDefinition,
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

  if (decoder.type === "gzip") {
    const result: SimpleRetrieverDecoder = {
      type: "GzipDecoder" as const,
      decoder: builderDecoderToManifest(decoder.decoder) as GzipDecoderDecoder,
    };

    return result;
  }

  if (decoder.type === "ZIP file") {
    const result: SimpleRetrieverDecoder = {
      type: "ZipfileDecoder" as const,
      decoder: builderDecoderToManifest(decoder.decoder) as ZipfileDecoderDecoder,
    };

    return result;
  }

  return {
    type: DECODER_TYPE_MAP[decoder.type] as JsonDecoderType | XmlDecoderType | JsonlDecoderType | IterableDecoderType,
  };
};

type BaseManifestRequester = Pick<HttpRequester, "type" | "url_base" | "authenticator">;

const builderBaseRequesterToManifest = (builderRequester: BuilderBaseRequester): HttpRequester => {
  return {
    type: HttpRequesterType.HttpRequester,
    url_base: builderRequester.url?.trim(),
    authenticator: convertOrLoadYamlString(builderRequester.authenticator, builderAuthenticatorToManifest),
    http_method: builderRequester.httpMethod,
    request_parameters: fromEntriesOrUndefined(builderRequester.requestOptions.requestParameters),
    request_headers: fromEntriesOrUndefined(builderRequester.requestOptions.requestHeaders),
    ...builderRequestBodyToStreamRequestBody(builderRequester.requestOptions.requestBody),
    error_handler: convertOrLoadYamlString(builderRequester.errorHandler, builderErrorHandlersToManifest),
  };
};

const builderDpathExtractorToManifest = (extractor: BuilderDpathExtractor): DpathExtractor => {
  return {
    type: DpathExtractorType.DpathExtractor,
    field_path: extractor.field_path,
  };
};

function builderStreamToDeclarativeStream(stream: BuilderStream, allStreams: BuilderStream[]): DeclarativeStream {
  // cast to tell typescript which properties will be present after resolving the ref
  const requesterRef = {
    $ref: "#/definitions/base_requester",
  } as unknown as BaseManifestRequester;

  const parentStreamsToManifest = (parentStreams: BuilderParentStream[] | undefined) =>
    builderParentStreamsToManifest(parentStreams, allStreams);

  let declarativeStream: DeclarativeStream;
  if (stream.requestType === "sync") {
    declarativeStream = {
      type: DeclarativeStreamType.DeclarativeStream,
      name: stream.name,
      primary_key: stream.primaryKey.length > 0 ? stream.primaryKey : undefined,
      retriever: {
        type: SimpleRetrieverType.SimpleRetriever,
        requester: {
          ...requesterRef,
          path: stream.urlPath?.trim() || undefined,
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
      schema_loader: { type: InlineSchemaLoaderType.InlineSchemaLoader, schema: schemaRef(stream.name) },
    };
  } else {
    declarativeStream = {
      type: DeclarativeStreamType.DeclarativeStream,
      name: stream.name,
      primary_key: stream.downloadRequester.primaryKey.length > 0 ? stream.downloadRequester.primaryKey : undefined,
      retriever: {
        type: AsyncRetrieverType.AsyncRetriever,
        record_selector: convertOrLoadYamlString(
          stream.downloadRequester.recordSelector,
          builderRecordSelectorToManifest
        ),
        status_mapping: stream.pollingRequester.statusMapping,
        status_extractor: builderDpathExtractorToManifest(stream.pollingRequester.statusExtractor),
        download_target_extractor: builderDpathExtractorToManifest(stream.pollingRequester.downloadTargetExtractor),
        partition_router: combinePartitionRouters(
          ...convertOrLoadYamlString(
            stream.creationRequester.parameterizedRequests,
            builderParameterizedRequestsToManifest
          ),
          ...convertOrLoadYamlString(stream.creationRequester.parentStreams, parentStreamsToManifest)
        ),
        decoder: builderDecoderToManifest(stream.creationRequester.decoder),
        creation_requester: builderBaseRequesterToManifest(stream.creationRequester),
        polling_requester: builderBaseRequesterToManifest(stream.pollingRequester),
        polling_job_timeout:
          stream.pollingRequester.pollingTimeout.value === 15
            ? undefined
            : stream.pollingRequester.pollingTimeout.value,
        download_requester: builderBaseRequesterToManifest(stream.downloadRequester),
        download_decoder: builderDecoderToManifest(stream.downloadRequester.decoder),
        download_paginator: convertOrLoadYamlString(stream.downloadRequester.paginator, builderPaginatorToManifest),
        download_extractor: stream.downloadRequester.downloadExtractor?.field_path
          ? stream.downloadRequester.downloadExtractor.field_path.length > 0
            ? builderDpathExtractorToManifest(stream.downloadRequester.downloadExtractor)
            : undefined
          : undefined,
        // abort_requester - TODO(async)
        // delete_requester - TODO(async)
        // url_requester - TODO(async)
      },
      incremental_sync: convertOrLoadYamlString(
        stream.creationRequester.incrementalSync,
        builderIncrementalSyncToManifest
      ),
      transformations: convertOrLoadYamlString(
        stream.downloadRequester.transformations,
        builderTransformationsToManifest
      ),
      schema_loader: { type: InlineSchemaLoaderType.InlineSchemaLoader, schema: schemaRef(stream.name) },
    };
  }
  return merge({} as DeclarativeStream, stream.unknownFields ? load(stream.unknownFields) : {}, declarativeStream);
}

function builderDynamicStreamToDeclarativeStream(dynamicStream: BuilderDynamicStream): DynamicDeclarativeStream {
  // the user operates on the streamTemplate, including component-mapped fields using `record`
  // we need to extract the component-mapped fields and add them to the components_mapping array

  const declarativeStream: DeclarativeStream = {
    ...builderStreamToDeclarativeStream(dynamicStream.streamTemplate, []),
    schema_loader: {
      type: InlineSchemaLoaderType.InlineSchemaLoader,
      schema: schemaRef(`${dynamicStream.dynamicStreamName}_stream_template`),
    },
  };

  const streamTemplateEntries = getRecursiveObjectEntries(declarativeStream);
  const { templateEntries, mappedEntries } = streamTemplateEntries.reduce<{
    templateEntries: Array<[string, unknown]>;
    mappedEntries: Array<[string, unknown]>;
  }>(
    (acc, entry) => {
      const isMapped = typeof entry[1] === "string" && entry[1].includes("components_values");
      if (isMapped) {
        acc.mappedEntries.push(entry);
        acc.templateEntries.push([entry[0], "placeholder"]); // keep a populated string here to ensure validation
      } else {
        acc.templateEntries.push(entry);
      }
      return acc;
    },
    { templateEntries: [], mappedEntries: [] }
  );

  const unmappedStreamTemplate = objectFromRecursiveEntries(templateEntries) as DeclarativeStream;

  const declarativeDynamicStream: DynamicDeclarativeStream = {
    type: DynamicDeclarativeStreamType.DynamicDeclarativeStream,
    name: dynamicStream.dynamicStreamName,
    components_resolver: {
      ...dynamicStream.componentsResolver,
      components_mapping: mappedEntries.map(([key, value]) => ({
        type: "ComponentMappingDefinition" as const,
        field_path: key.split("."),
        value: value as string,
      })),
    },
    stream_template: unmappedStreamTemplate,
  };

  // if there isn't a record filter condition, remove the filter component
  if (
    !(declarativeDynamicStream.components_resolver as BuilderComponentsResolver).retriever.record_selector.record_filter
      ?.condition
  ) {
    delete (declarativeDynamicStream.components_resolver as BuilderComponentsResolver).retriever.record_selector
      .record_filter;
  }

  return declarativeDynamicStream;
}

export const builderFormValuesToMetadata = (values: BuilderFormValues): BuilderMetadata => {
  const testedStreams = {} as TestedStreams;
  values.streams.forEach((stream) => {
    if (stream.testResults) {
      testedStreams[stream.name] = stream.testResults;
    }
  });

  Object.values(values.generatedStreams)
    .flat()
    .forEach((generatedStream) => {
      if (generatedStream.testResults) {
        testedStreams[generatedStream.name] = generatedStream.testResults;
      }
    });

  const assistData = values.assist ?? {};

  return {
    autoImportSchema: {
      ...Object.fromEntries(values.streams.map((stream) => [stream.name, stream.autoImportSchema])),
      ...Object.fromEntries(
        values.dynamicStreams.map((dynamicStream) => [
          dynamicStream.dynamicStreamName,
          dynamicStream.streamTemplate.autoImportSchema,
        ])
      ),
    },
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
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        state: authenticator.declarative.state ? JSON.parse(authenticator.declarative.state) : undefined,

        access_token_headers: authenticator.declarative.access_token_headers
          ? // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
            Object.fromEntries(authenticator.declarative.access_token_headers)
          : undefined,

        access_token_params: authenticator.declarative.access_token_params
          ? // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
            Object.fromEntries(authenticator.declarative.access_token_params)
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
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    connection_specification: specSchema,
    type: "Spec",
  };
};

export const convertToManifest = (values: BuilderFormValues): ConnectorManifest => {
  const manifestStreams: DeclarativeStream[] = values.streams.map((stream) =>
    builderStreamToDeclarativeStream(stream, values.streams)
  );

  const manifestDynamicStreams = values.dynamicStreams.map((dynamicStream) =>
    builderDynamicStreamToDeclarativeStream(dynamicStream)
  );

  const streamNames = values.streams.map((s) => s.name);
  const validCheckStreamNames = (values.checkStreams ?? []).filter((checkStream) => streamNames.includes(checkStream));
  const correctedCheckStreams =
    validCheckStreamNames.length > 0 ? validCheckStreamNames : streamNames.length > 0 ? [streamNames[0]] : [];

  const dynamicStreamNames = values.dynamicStreams.map((s) => s.dynamicStreamName);
  const validCheckDynamicStream = (values.dynamicStreamCheckConfigs ?? []).filter((dynamicStreamCheckConfig) =>
    dynamicStreamNames.includes(dynamicStreamCheckConfig.dynamic_stream_name)
  );
  const correctedCheckDynamicStreams =
    validCheckDynamicStream.length > 0
      ? validCheckDynamicStream
      : dynamicStreamNames.length > 0
      ? [
          {
            type: DynamicStreamCheckConfigType.DynamicStreamCheckConfig,
            dynamic_stream_name: dynamicStreamNames[0],
            stream_count: 1,
          },
        ]
      : [];

  const streamNameToStream = Object.fromEntries(manifestStreams.map((stream) => [stream.name, stream]));
  const streamRefs = manifestStreams.map((stream) => streamRef(stream.name!));

  const dynamicStreamNameToSchema = Object.fromEntries(
    values.dynamicStreams.map((dynamicStream) => {
      let schema;
      if (dynamicStream.streamTemplate.schema) {
        try {
          schema = JSON.parse(dynamicStream.streamTemplate.schema);
        } catch (e) {
          schema = JSON.parse(DEFAULT_SCHEMA);
        }
      } else {
        schema = JSON.parse(DEFAULT_SCHEMA);
      }
      schema.additionalProperties = true;
      return [`${dynamicStream.dynamicStreamName}_stream_template`, schema];
    })
  );

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

  const baseRequester: BaseManifestRequester = {
    type: "HttpRequester",
    url_base: values.global?.urlBase?.trim() ?? "/",
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
      ...(correctedCheckStreams.length > 0 ? { stream_names: correctedCheckStreams } : {}),
      ...(correctedCheckDynamicStreams.length > 0
        ? { dynamic_streams_check_configs: correctedCheckDynamicStreams }
        : {}),
    },
    definitions: {
      base_requester: baseRequester,
      streams: streamNameToStream,
    },
    streams: streamRefs,
    ...(manifestDynamicStreams.length > 0 ? { dynamic_streams: manifestDynamicStreams } : {}),
    schemas: { ...streamNameToSchema, ...dynamicStreamNameToSchema },
    spec,
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    metadata: builderFormValuesToMetadata(values),
    description: values.description,
  };
};

function schemaRef(streamName: string) {
  return { $ref: `#/schemas/${streamName}` };
}

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
      http_method: "GET",
    },
  },
};
export const DEFAULT_JSON_MANIFEST_VALUES: ConnectorManifest = {
  type: "DeclarativeSource",
  version: CDK_VERSION,
  check: {
    type: "CheckStream",
    stream_names: [],
  },
  streams: [],
  spec: {
    type: "Spec",
    connection_specification: {
      type: "object",
      properties: {},
    },
  },
};

export const DEFAULT_JSON_MANIFEST_STREAM_WITH_URL_BASE: DeclarativeStream = {
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
      http_method: "GET",
    },
  },
};
export const DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM: ConnectorManifest = {
  ...DEFAULT_JSON_MANIFEST_VALUES,
  streams: [DEFAULT_JSON_MANIFEST_STREAM_WITH_URL_BASE],
};

export type StreamPathFn = <T extends string>(fieldPath: T) => `formValues.streams.${number}.${T}`;

export type DynamicStreamStreamTemplatePathFn = <T extends string>(
  fieldPath: T
) => `formValues.dynamicStreams.${number}.streamTemplate.${T}`;

export type GeneratedStreamStreamTemplatePathFn = <T extends string>(
  fieldPath: T
) => `formValues.generatedStreams.${string}.${number}.${T}`;

export type AnyDeclarativeStreamPathFn =
  | StreamPathFn
  | DynamicStreamStreamTemplatePathFn
  | GeneratedStreamStreamTemplatePathFn;
export type DynamicStreamPathFn = <T extends string>(fieldPath: T) => `formValues.dynamicStreams.${number}.${T}`;
export type CreationRequesterPathFn = <T extends string>(
  fieldPath: T
) => `formValues.streams.${number}.creationRequester.${T}`;
export type PollingRequesterPathFn = <T extends string>(
  fieldPath: T
) => `formValues.streams.${number}.pollingRequester.${T}`;
export type DownloadRequesterPathFn = <T extends string>(
  fieldPath: T
) => `formValues.streams.${number}.downloadRequester.${T}`;

export const concatPath = <TBase extends string, TPath extends string>(base: TBase, path: TPath) =>
  `${base}.${path}` as const;

type StreamIdToFieldPath<T extends "stream" | "dynamic_stream", K extends string> = T extends "stream"
  ? `formValues.streams.${number}.${K}`
  : `formValues.dynamicStreams.${number}.streamTemplate.${K}`;
export function getStreamFieldPath<T extends "stream" | "dynamic_stream", K extends string>(
  streamId: StreamId,
  fieldPath: K
): StreamIdToFieldPath<T, K> {
  if (streamId.type === "stream") {
    return `formValues.streams.${streamId.index}.${fieldPath}` as StreamIdToFieldPath<T, K>;
  } else if (streamId.type === "generated_stream") {
    return `formValues.generatedStreams.${streamId.dynamicStreamName}.${streamId.index}.${fieldPath}` as StreamIdToFieldPath<
      T,
      K
    >;
  }
  return `formValues.dynamicStreams.${streamId.index}.streamTemplate.${fieldPath}` as StreamIdToFieldPath<T, K>;
}

function getRecursiveObjectEntries(obj: object, prefix: string = ""): Array<[string, unknown]> {
  return Object.entries(obj).flatMap(([key, value]) => {
    const cleanedPrefix = prefix ? `${prefix}.` : "";
    const valueIsEmptyArray = Array.isArray(value) && value.length === 0;
    if (typeof value === "object" && value !== null && !valueIsEmptyArray) {
      return getRecursiveObjectEntries(value as Record<string, unknown>, `${cleanedPrefix}${key}`);
    }
    return [[`${cleanedPrefix}${key}`, value]];
  });
}

function objectFromRecursiveEntries(entries: Array<[string, unknown]>) {
  return entries.reduce<Record<string, unknown>>((acc, [key, value]) => {
    const pathParts = key.split(".");
    const finalKey = pathParts.at(-1)!;
    let current = acc;

    while (pathParts.length > 1) {
      const next = pathParts.shift()!;
      const peek = pathParts.at(0);
      const peekIsIndex = peek && peek.match(/^\d+$/);

      current[next] = current[next] || (peekIsIndex ? [] : {});
      current = current[next] as Record<string, unknown>;
    }

    current[finalKey] = value;
    return acc;
  }, {});
}
