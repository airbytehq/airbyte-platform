import { dump } from "js-yaml";
import cloneDeep from "lodash/cloneDeep";
import get from "lodash/get";
import isArray from "lodash/isArray";
import isEqual from "lodash/isEqual";
import isString from "lodash/isString";
import pick from "lodash/pick";
import { match } from "ts-pattern";

import {
  ConnectorManifest,
  DeclarativeStream,
  DeclarativeStreamIncrementalSync,
  DeclarativeStreamSchemaLoader,
  DefaultErrorHandler,
  DeclarativeStreamTransformationsItem,
  DpathExtractor,
  HttpRequester,
  HttpRequesterAuthenticator,
  SimpleRetriever,
  SimpleRetrieverPaginator,
  SimpleRetrieverPartitionRouter,
  SimpleRetrieverPartitionRouterAnyOfItem,
  Spec,
  DatetimeBasedCursorEndDatetime,
  DatetimeBasedCursorStartDatetime,
  ApiKeyAuthenticator,
  BasicHttpAuthenticator,
  BearerAuthenticator,
  OAuthAuthenticator,
  DefaultPaginator,
  DeclarativeComponentSchemaMetadata,
  HttpRequesterErrorHandler,
  NoAuth,
  SessionTokenAuthenticator,
  DatetimeBasedCursorType,
} from "core/api/types/ConnectorManifest";
import { removeEmptyProperties } from "core/utils/form";

import {
  API_KEY_AUTHENTICATOR,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  BuilderErrorHandler,
  BuilderFormAuthenticator,
  BuilderFormInput,
  BuilderIncrementalSync,
  BuilderPaginator,
  BuilderRequestBody,
  BuilderStream,
  BuilderTransformation,
  DEFAULT_BUILDER_FORM_VALUES,
  DEFAULT_BUILDER_STREAM_VALUES,
  extractInterpolatedConfigKey,
  INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  interpolateConfigKey,
  isInterpolatedConfigKey,
  NO_AUTH,
  OAUTH_AUTHENTICATOR,
  RequestOptionOrPathInject,
  SESSION_TOKEN_AUTHENTICATOR,
  YamlString,
  YamlSupportedComponentName,
} from "./types";
import {
  getKeyToDesiredLockedInput,
  LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE,
  LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME,
} from "./useLockedInputs";
import { formatJson } from "./utils";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";

export const convertToBuilderFormValuesSync = (resolvedManifest: ConnectorManifest) => {
  const builderFormValues = cloneDeep(DEFAULT_BUILDER_FORM_VALUES);
  builderFormValues.checkStreams = resolvedManifest.check.stream_names;

  const streams = resolvedManifest.streams;
  if (streams === undefined || streams.length === 0) {
    builderFormValues.inputs = manifestSpecToBuilderInputs(resolvedManifest.spec, { type: NO_AUTH }, []);
    return removeEmptyProperties(builderFormValues);
  }

  assertType<SimpleRetriever>(streams[0].retriever, "SimpleRetriever", streams[0].name);
  assertType<HttpRequester>(streams[0].retriever.requester, "HttpRequester", streams[0].name);
  builderFormValues.global.urlBase = streams[0].retriever.requester.url_base;

  const serializedStreamToIndex = Object.fromEntries(streams.map((stream, index) => [formatJson(stream, true), index]));
  builderFormValues.streams = streams.map((stream, index) =>
    manifestStreamToBuilder(
      stream,
      index,
      serializedStreamToIndex,
      streams[0].retriever.requester.url_base,
      streams[0].retriever.requester.authenticator,
      resolvedManifest.metadata,
      resolvedManifest.spec
    )
  );

  builderFormValues.global.authenticator = manifestAuthenticatorToBuilder(
    streams[0].retriever.requester.authenticator,
    resolvedManifest.spec
  );
  builderFormValues.inputs = manifestSpecToBuilderInputs(
    resolvedManifest.spec,
    builderFormValues.global.authenticator,
    builderFormValues.streams
  );

  return removeEmptyProperties(builderFormValues);
};

const RELEVANT_AUTHENTICATOR_KEYS = [
  "type",
  "api_token",
  "header",
  "username",
  "password",
  "client_id",
  "client_secret",
  "refresh_token",
  "token_refresh_endpoint",
  "access_token_name",
  "expires_in_name",
  "grant_type",
  "refresh_request_body",
  "scopes",
  "token_expiry_date",
  "token_expiry_date_format",
  "refresh_token_updater",
  "inject_into",
] as const;

// This type is a union of all keys of the supported authenticators
type RelevantAuthenticatorKeysType = Exclude<
  keyof ApiKeyAuthenticator | keyof BasicHttpAuthenticator | keyof BearerAuthenticator | keyof OAuthAuthenticator,
  "$parameters"
>;

// Re-assign to make sure RELEVANT_AUTHENTICATOR_KEYS is listing all keys of all supported authenticators
// If a key is not listed above, it will cause a typescript error
const authenticatorKeysToCheck: Readonly<Array<(typeof RELEVANT_AUTHENTICATOR_KEYS)[number]>> =
  RELEVANT_AUTHENTICATOR_KEYS as Readonly<RelevantAuthenticatorKeysType[]>;

const manifestStreamToBuilder = (
  stream: DeclarativeStream,
  index: number,
  serializedStreamToIndex: Record<string, number>,
  firstStreamUrlBase: string,
  firstStreamAuthenticator?: HttpRequesterAuthenticator,
  metadata?: DeclarativeComponentSchemaMetadata,
  spec?: Spec
): BuilderStream => {
  assertType<SimpleRetriever>(stream.retriever, "SimpleRetriever", stream.name);
  const retriever = stream.retriever;

  assertType<HttpRequester>(retriever.requester, "HttpRequester", stream.name);
  const requester = retriever.requester;
  const cleanedAuthenticator = pick(retriever.requester.authenticator, authenticatorKeysToCheck);
  const cleanedFirstStreamAuthenticator = pick(firstStreamAuthenticator, authenticatorKeysToCheck);

  if (
    !firstStreamAuthenticator || firstStreamAuthenticator.type === "NoAuth"
      ? requester.authenticator && requester.authenticator.type !== "NoAuth"
      : !isEqual(cleanedAuthenticator, cleanedFirstStreamAuthenticator)
  ) {
    console.log("authenticator", cleanedAuthenticator, cleanedFirstStreamAuthenticator);
    throw new ManifestCompatibilityError(stream.name, "authenticator does not match the first stream's");
  }

  if (retriever.requester.url_base !== firstStreamUrlBase) {
    throw new ManifestCompatibilityError(stream.name, "url_base does not match the first stream's");
  }

  if (![undefined, "GET", "POST"].includes(requester.http_method)) {
    throw new ManifestCompatibilityError(stream.name, "http_method is not GET or POST");
  }

  assertType<DpathExtractor>(retriever.record_selector.extractor, "DpathExtractor", stream.name);

  const { parentStreams, parameterizedRequests } = manifestPartitionRouterToBuilder(
    retriever.partition_router,
    serializedStreamToIndex,
    stream.name
  );

  return {
    ...DEFAULT_BUILDER_STREAM_VALUES,
    id: index.toString(),
    name: stream.name ?? `stream_${index}`,
    urlPath: requester.path,
    httpMethod: requester.http_method === "POST" ? "POST" : "GET",
    fieldPointer: retriever.record_selector.extractor.field_path as string[],
    requestOptions: {
      requestParameters: Object.entries(requester.request_parameters ?? {}),
      requestHeaders: Object.entries(requester.request_headers ?? {}),
      requestBody: requesterToRequestBody(requester),
    },
    primaryKey: manifestPrimaryKeyToBuilder(stream),
    paginator: convertOrDumpAsString(
      retriever.paginator,
      manifestPaginatorToBuilder,
      "paginator",
      stream.name,
      metadata
    ),
    incrementalSync: convertOrDumpAsString(
      stream.incremental_sync,
      manifestIncrementalSyncToBuilder,
      "incrementalSync",
      stream.name,
      metadata,
      spec
    ),
    parentStreams,
    parameterizedRequests,
    schema: manifestSchemaLoaderToBuilderSchema(stream.schema_loader),
    errorHandler: convertOrDumpAsString(
      requester.error_handler,
      manifestErrorHandlerToBuilder,
      "errorHandler",
      stream.name,
      metadata
    ),
    transformations: convertOrDumpAsString(
      stream.transformations,
      manifestTransformationsToBuilder,
      "transformations",
      stream.name,
      metadata
    ),
    unsupportedFields: {
      retriever: {
        record_selector: {
          record_filter: stream.retriever.record_selector.record_filter,
        },
      },
    },
    autoImportSchema: metadata?.autoImportSchema?.[stream.name ?? ""] === true,
  };
};

function requesterToRequestBody(requester: HttpRequester): BuilderRequestBody {
  if (requester.request_body_data && typeof requester.request_body_data === "object") {
    return { type: "form_list", values: Object.entries(requester.request_body_data) };
  }
  if (requester.request_body_data && isString(requester.request_body_data)) {
    return { type: "string_freeform", value: requester.request_body_data };
  }
  if (!requester.request_body_json) {
    return { type: "json_list", values: [] };
  }
  const allStringValues = Object.values(requester.request_body_json).every((value) => isString(value));
  if (allStringValues) {
    return { type: "json_list", values: Object.entries(requester.request_body_json) };
  }
  return {
    type: "json_freeform",
    value: isString(requester.request_body_json)
      ? requester.request_body_json
      : formatJson(requester.request_body_json),
  };
}

function manifestPartitionRouterToBuilder(
  partitionRouter: SimpleRetrieverPartitionRouter | SimpleRetrieverPartitionRouterAnyOfItem | undefined,
  serializedStreamToIndex: Record<string, number>,
  streamName?: string
): { parentStreams: BuilderStream["parentStreams"]; parameterizedRequests: BuilderStream["parameterizedRequests"] } {
  if (partitionRouter === undefined) {
    return { parentStreams: undefined, parameterizedRequests: undefined };
  }

  if (Array.isArray(partitionRouter)) {
    const convertedPartitionRouters = partitionRouter.map((subRouter) =>
      manifestPartitionRouterToBuilder(subRouter, serializedStreamToIndex, streamName)
    );
    const parentStreams = convertedPartitionRouters.flatMap(
      (convertedPartitionRouter) => convertedPartitionRouter.parentStreams ?? []
    );
    const parameterizedRequests = convertedPartitionRouters.flatMap(
      (convertedPartitionRouter) => convertedPartitionRouter.parameterizedRequests ?? []
    );
    return {
      parentStreams: parentStreams.length === 0 ? undefined : parentStreams,
      parameterizedRequests: parameterizedRequests.length === 0 ? undefined : parameterizedRequests,
    };
  }

  if (partitionRouter.type === undefined) {
    throw new ManifestCompatibilityError(streamName, "partition_router has no type");
  }

  if (partitionRouter.type === "CustomPartitionRouter") {
    throw new ManifestCompatibilityError(streamName, "partition_router contains a CustomPartitionRouter");
  }

  if (partitionRouter.type === "ListPartitionRouter") {
    return {
      parentStreams: undefined,
      parameterizedRequests: [
        {
          ...partitionRouter,
          values: isString(partitionRouter.values)
            ? {
                value: partitionRouter.values,
                type: "variable" as const,
              }
            : {
                value: partitionRouter.values,
                type: "list" as const,
              },
        },
      ],
    };
  }

  if (partitionRouter.type === "SubstreamPartitionRouter") {
    const manifestSubstreamPartitionRouter = partitionRouter;

    if (manifestSubstreamPartitionRouter.parent_stream_configs.length > 1) {
      throw new ManifestCompatibilityError(streamName, "SubstreamPartitionRouter has more than one parent stream");
    }
    const parentStreamConfig = manifestSubstreamPartitionRouter.parent_stream_configs[0];

    const matchingStreamIndex = serializedStreamToIndex[formatJson(parentStreamConfig.stream, true)];
    if (matchingStreamIndex === undefined) {
      throw new ManifestCompatibilityError(
        streamName,
        "SubstreamPartitionRouter's parent stream doesn't match any other stream"
      );
    }

    return {
      parameterizedRequests: undefined,
      parentStreams: [
        {
          parent_key: parentStreamConfig.parent_key,
          partition_field: parentStreamConfig.partition_field,
          parentStreamReference: matchingStreamIndex.toString(),
          request_option: parentStreamConfig.request_option,
        },
      ],
    };
  }

  throw new ManifestCompatibilityError(streamName, "partition_router type is unsupported");
}

export function manifestErrorHandlerToBuilder(
  errorHandler: HttpRequesterErrorHandler | undefined,
  streamName?: string
): BuilderErrorHandler[] | undefined {
  if (!errorHandler) {
    return undefined;
  }
  const handlers: HttpRequesterErrorHandler[] =
    errorHandler.type === "CompositeErrorHandler" ? errorHandler.error_handlers : [errorHandler];

  handlers.forEach((handler) => {
    match(handler.type)
      .with("DefaultErrorHandler", () => {})
      .with("CustomErrorHandler", () => {
        throw new ManifestCompatibilityError(streamName, "custom error handler used");
      })
      .with("CompositeErrorHandler", () => {
        throw new ManifestCompatibilityError(streamName, "nested composite error handler used");
      })
      .otherwise(() => {
        throw new ManifestCompatibilityError(
          streamName,
          "error handler type is unsupported; only CompositeErrorHandler and DefaultErrorHandler are supported"
        );
      });
  });

  const defaultHandlers = handlers as DefaultErrorHandler[];
  return defaultHandlers.map((handler) => {
    if (handler.backoff_strategies && handler.backoff_strategies.length > 1) {
      throw new ManifestCompatibilityError(streamName, "more than one backoff strategy per handler");
    }
    const backoffStrategy = handler.backoff_strategies?.[0];
    if (backoffStrategy?.type === "CustomBackoffStrategy") {
      throw new ManifestCompatibilityError(streamName, "custom backoff strategy");
    }
    if (handler.response_filters && handler.response_filters.length > 1) {
      throw new ManifestCompatibilityError(streamName, "more than one response filter per handler");
    }
    const responseHandler = handler.response_filters?.[0];
    return {
      ...handler,
      response_filters: undefined,
      response_filter: responseHandler
        ? { ...responseHandler, http_codes: responseHandler.http_codes?.map((code) => String(code)) }
        : undefined,
      backoff_strategies: undefined,
      backoff_strategy: backoffStrategy,
    };
  });
}

function manifestPrimaryKeyToBuilder(manifestStream: DeclarativeStream): BuilderStream["primaryKey"] {
  if (manifestStream.primary_key === undefined) {
    return [];
  } else if (Array.isArray(manifestStream.primary_key)) {
    if (manifestStream.primary_key.length > 0 && Array.isArray(manifestStream.primary_key[0])) {
      throw new ManifestCompatibilityError(manifestStream.name, "primary_key contains nested arrays");
    } else {
      return manifestStream.primary_key as string[];
    }
  } else {
    return [manifestStream.primary_key];
  }
}

export function manifestTransformationsToBuilder(
  transformations: DeclarativeStreamTransformationsItem[] | undefined,
  streamName?: string
): BuilderTransformation[] | undefined {
  if (!transformations) {
    return undefined;
  }
  const builderTransformations: BuilderTransformation[] = [];

  transformations.forEach((transformation) => {
    if (transformation.type === "CustomTransformation") {
      throw new ManifestCompatibilityError(streamName, "custom transformation used");
    }
    if (transformation.type === "AddFields") {
      builderTransformations.push(
        ...(transformation.fields as Array<{ value: string; path: string[] }>).map((field) => ({
          type: "add" as const,
          value: field.value,
          path: field.path,
        }))
      );
    }
    if (transformation.type === "RemoveFields") {
      builderTransformations.push(...transformation.field_pointers.map((path) => ({ type: "remove" as const, path })));
    }
  });

  if (builderTransformations.length === 0) {
    return undefined;
  }
  return builderTransformations;
}

function getFormat(
  manifestCursorDatetime: DatetimeBasedCursorStartDatetime | DatetimeBasedCursorEndDatetime,
  manifestIncrementalSync: DeclarativeStreamIncrementalSync
) {
  if (isString(manifestCursorDatetime) || !manifestCursorDatetime.datetime_format) {
    return manifestIncrementalSync.datetime_format;
  }
  return manifestCursorDatetime.datetime_format;
}

function isFormatSupported(
  manifestCursorDatetime: DatetimeBasedCursorStartDatetime | DatetimeBasedCursorEndDatetime,
  manifestIncrementalSync: DeclarativeStreamIncrementalSync
) {
  return getFormat(manifestCursorDatetime, manifestIncrementalSync) === INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT;
}

export function manifestIncrementalSyncToBuilder(
  manifestIncrementalSync: DeclarativeStreamIncrementalSync | undefined,
  streamName?: string,
  spec?: Spec
): BuilderStream["incrementalSync"] | undefined {
  if (!manifestIncrementalSync) {
    return undefined;
  }
  if (manifestIncrementalSync.type === "CustomIncrementalSync") {
    throw new ManifestCompatibilityError(streamName, "incremental sync uses a custom implementation");
  }
  assertType(manifestIncrementalSync, "DatetimeBasedCursor", streamName);

  if (manifestIncrementalSync.partition_field_start || manifestIncrementalSync.partition_field_end) {
    throw new ManifestCompatibilityError(
      streamName,
      "Custom partition_field_start and partition_field_end are not supported"
    );
  }

  const {
    cursor_datetime_formats,
    datetime_format,
    partition_field_end,
    partition_field_start,
    end_datetime: manifestEndDateTime,
    start_datetime: manifestStartDateTime,
    step,
    cursor_granularity,
    is_data_feed,
    type,
    $parameters,
    ...regularFields
  } = manifestIncrementalSync;

  if (
    (manifestStartDateTime &&
      typeof manifestStartDateTime !== "string" &&
      (manifestStartDateTime.max_datetime || manifestStartDateTime.min_datetime)) ||
    (manifestEndDateTime &&
      typeof manifestEndDateTime !== "string" &&
      (manifestEndDateTime.max_datetime || manifestEndDateTime.min_datetime))
  ) {
    throw new ManifestCompatibilityError(
      streamName,
      "DatetimeBasedCursor max_datetime and min_datetime are not supported"
    );
  }

  let start_datetime: BuilderIncrementalSync["start_datetime"] = {
    type: "custom",
    value: isString(manifestStartDateTime) ? manifestStartDateTime : manifestStartDateTime.datetime,
    format: getFormat(manifestStartDateTime, manifestIncrementalSync),
  };
  let end_datetime: BuilderIncrementalSync["end_datetime"] = {
    type: "custom",
    value: isString(manifestEndDateTime) ? manifestEndDateTime : manifestEndDateTime?.datetime || "",
    format: manifestEndDateTime ? getFormat(manifestEndDateTime, manifestIncrementalSync) : undefined,
  };

  const startDateSpecKey = tryExtractAndValidateIncrementalKey(
    ["start_datetime"],
    start_datetime.value,
    spec,
    streamName
  );
  if (startDateSpecKey && isFormatSupported(manifestStartDateTime, manifestIncrementalSync)) {
    start_datetime = { type: "user_input", value: interpolateConfigKey(startDateSpecKey) };
  }

  const endDateSpecKey = tryExtractAndValidateIncrementalKey(["end_datetime"], end_datetime.value, spec, streamName);
  if (manifestEndDateTime && endDateSpecKey && isFormatSupported(manifestEndDateTime, manifestIncrementalSync)) {
    end_datetime = { type: "user_input", value: interpolateConfigKey(endDateSpecKey) };
  } else if (
    !manifestEndDateTime ||
    end_datetime.value === `{{ now_utc().strftime('${INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT}') }}`
  ) {
    end_datetime = { type: "now" };
  }

  return {
    ...regularFields,
    cursor_datetime_formats: cursor_datetime_formats ?? [datetime_format],
    datetime_format:
      cursor_datetime_formats && datetime_format === cursor_datetime_formats[0] ? undefined : datetime_format,
    end_datetime,
    start_datetime,
    slicer: step && cursor_granularity ? { step, cursor_granularity } : undefined,
    filter_mode: is_data_feed ? "no_filter" : manifestEndDateTime ? "range" : "start",
  };
}

function safeJinjaAccessToPath(expression: string, stopCondition: string): string[] | undefined {
  const matchesSafeJinjaAccess = expression.match(
    /\{\{ (response|headers)((\.get\("(.+?)", \{\}\))|(\[-?\d+\]))+ \}\}/
  );
  const matchesSafeJinjaCondition = stopCondition.match(
    /\{\{ not (response|headers)((\.get\("(.+?)", \{\}\))|(\[-?\d+\]))+ \}\}/
  );
  if (
    !matchesSafeJinjaAccess ||
    !matchesSafeJinjaCondition ||
    matchesSafeJinjaAccess[1] !== matchesSafeJinjaCondition[1]
  ) {
    return undefined;
  }

  const segmentRegex = /\.get\("(.+?)", {}\)|\[(-?\d+)\]/g;
  const segments = [...expression.matchAll(segmentRegex)].map((match) => match[1] || match[2]);
  const conditionSegments = [...stopCondition.matchAll(segmentRegex)].map((match) => match[1] || match[2]);

  if (!isEqual(segments, conditionSegments)) {
    return undefined;
  }

  return [matchesSafeJinjaAccess[1], ...segments];
}

function manifestPaginatorStrategyToBuilder(
  strategy: DefaultPaginator["pagination_strategy"]
): BuilderPaginator["strategy"] {
  if (strategy.type === "OffsetIncrement" || strategy.type === "PageIncrement") {
    return strategy;
  }

  if (strategy.type !== "CursorPagination") {
    throw new ManifestCompatibilityError(undefined, "paginator.pagination_strategy uses an unsupported type");
  }

  const { cursor_value, stop_condition, ...rest } = strategy;

  const path = safeJinjaAccessToPath(cursor_value, stop_condition || "");

  return {
    ...rest,
    cursor: path
      ? { type: path[0] as "response" | "headers", path: path.slice(1) }
      : {
          type: "custom",
          cursor_value,
          stop_condition,
        },
  };
}

export function manifestPaginatorToBuilder(
  manifestPaginator: SimpleRetrieverPaginator | undefined,
  streamName?: string
): BuilderPaginator | undefined {
  if (manifestPaginator === undefined || manifestPaginator.type === "NoPagination") {
    return undefined;
  }
  assertType(manifestPaginator, "DefaultPaginator", streamName);

  if (manifestPaginator.pagination_strategy.type === "CustomPaginationStrategy") {
    throw new ManifestCompatibilityError(streamName, "paginator.pagination_strategy uses a CustomPaginationStrategy");
  }

  let pageTokenOption: RequestOptionOrPathInject | undefined = undefined;

  if (manifestPaginator.page_token_option?.type === "RequestPath") {
    pageTokenOption = { inject_into: "path" };
  } else if (manifestPaginator.page_token_option?.type === "RequestOption") {
    pageTokenOption = {
      inject_into: manifestPaginator.page_token_option.inject_into,
      field_name: manifestPaginator.page_token_option.field_name,
    };
  }

  return {
    strategy: manifestPaginatorStrategyToBuilder(manifestPaginator.pagination_strategy),
    pageTokenOption,
    pageSizeOption: manifestPaginator.page_size_option,
  };
}

function manifestSchemaLoaderToBuilderSchema(
  manifestSchemaLoader: DeclarativeStreamSchemaLoader | undefined
): BuilderStream["schema"] {
  if (manifestSchemaLoader === undefined) {
    return undefined;
  }

  if (manifestSchemaLoader.type === "InlineSchemaLoader") {
    const inlineSchemaLoader = manifestSchemaLoader;
    return inlineSchemaLoader.schema ? formatJson(inlineSchemaLoader.schema, true) : undefined;
  }

  // Return undefined if schema loader is not inline.
  // In this case, users can copy-paste the schema into the Builder, or they can re-infer it
  return undefined;
}

function removeLeadingSlashes(path: string) {
  return path.replace(/^\/+/, "");
}

function removeTrailingSlashes(path: string) {
  return path.replace(/\/+$/, "");
}

type SupportedAuthenticator =
  | ApiKeyAuthenticator
  | BasicHttpAuthenticator
  | BearerAuthenticator
  | OAuthAuthenticator
  | NoAuth
  | SessionTokenAuthenticator;

function isSupportedAuthenticator(authenticator: HttpRequesterAuthenticator): authenticator is SupportedAuthenticator {
  const supportedAuthTypes: string[] = [
    NO_AUTH,
    API_KEY_AUTHENTICATOR,
    BEARER_AUTHENTICATOR,
    BASIC_AUTHENTICATOR,
    OAUTH_AUTHENTICATOR,
    SESSION_TOKEN_AUTHENTICATOR,
  ];
  return supportedAuthTypes.includes(authenticator.type);
}

function manifestAuthenticatorToBuilder(
  authenticator: HttpRequesterAuthenticator | undefined,
  spec: Spec | undefined,
  streamName?: string
): BuilderFormAuthenticator {
  if (authenticator === undefined) {
    return {
      type: NO_AUTH,
    };
  } else if (authenticator.type === undefined) {
    throw new ManifestCompatibilityError(streamName, "Authenticator has no type");
  } else if (!isSupportedAuthenticator(authenticator)) {
    throw new ManifestCompatibilityError(streamName, `Unsupported authenticator type: ${authenticator.type}`);
  }

  switch (authenticator.type) {
    case NO_AUTH: {
      return {
        type: NO_AUTH,
      };
    }

    case API_KEY_AUTHENTICATOR: {
      return {
        ...authenticator,
        inject_into: authenticator.inject_into ?? {
          type: "RequestOption",
          field_name: authenticator.header || "",
          inject_into: "header",
        },
        api_token: interpolateConfigKey(extractAndValidateAuthKey(["api_token"], authenticator, spec, streamName)),
      };
    }

    case BEARER_AUTHENTICATOR: {
      return {
        ...authenticator,
        api_token: interpolateConfigKey(extractAndValidateAuthKey(["api_token"], authenticator, spec, streamName)),
      };
    }

    case BASIC_AUTHENTICATOR: {
      return {
        ...authenticator,
        username: interpolateConfigKey(extractAndValidateAuthKey(["username"], authenticator, spec, streamName)),
        password: interpolateConfigKey(extractAndValidateAuthKey(["password"], authenticator, spec, streamName)),
      };
    }

    case OAUTH_AUTHENTICATOR: {
      if (
        Object.values(authenticator.refresh_request_body ?? {}).filter((value) => typeof value !== "string").length > 0
      ) {
        throw new ManifestCompatibilityError(
          streamName,
          "OAuthAuthenticator contains a refresh_request_body with non-string values"
        );
      }
      if (
        authenticator.grant_type &&
        authenticator.grant_type !== "refresh_token" &&
        authenticator.grant_type !== "client_credentials"
      ) {
        throw new ManifestCompatibilityError(
          streamName,
          "OAuthAuthenticator sets custom grant_type, but it must be one of 'refresh_token' or 'client_credentials'"
        );
      }

      let builderAuthenticator: BuilderFormAuthenticator = {
        ...authenticator,
        refresh_request_body: Object.entries(authenticator.refresh_request_body ?? {}),
        grant_type: authenticator.grant_type ?? "refresh_token",
        refresh_token_updater: undefined,
        client_id: interpolateConfigKey(extractAndValidateAuthKey(["client_id"], authenticator, spec, streamName)),
        client_secret: interpolateConfigKey(
          extractAndValidateAuthKey(["client_secret"], authenticator, spec, streamName)
        ),
      };

      if (!authenticator.grant_type || authenticator.grant_type === "refresh_token") {
        const refreshTokenSpecKey = extractAndValidateAuthKey(["refresh_token"], authenticator, spec, streamName);
        builderAuthenticator = {
          ...builderAuthenticator,
          refresh_token: interpolateConfigKey(refreshTokenSpecKey),
        };

        if (authenticator.refresh_token_updater) {
          if (!isEqual(authenticator.refresh_token_updater?.refresh_token_config_path, [refreshTokenSpecKey])) {
            throw new ManifestCompatibilityError(
              streamName,
              "OAuthAuthenticator refresh_token_config_path needs to match the config path used for refresh_token"
            );
          }
          const {
            access_token_config_path,
            token_expiry_date_config_path,
            refresh_token_config_path,
            ...refresh_token_updater
          } = authenticator.refresh_token_updater;
          builderAuthenticator = {
            ...builderAuthenticator,
            refresh_token_updater: {
              ...refresh_token_updater,
              access_token: interpolateConfigKey(
                extractAndValidateAuthKey(
                  ["refresh_token_updater", "access_token_config_path"],
                  authenticator,
                  spec,
                  streamName
                )
              ),
              token_expiry_date: interpolateConfigKey(
                extractAndValidateAuthKey(
                  ["refresh_token_updater", "token_expiry_date_config_path"],
                  authenticator,
                  spec,
                  streamName
                )
              ),
            },
          };
        }
      }

      return builderAuthenticator;
    }

    case SESSION_TOKEN_AUTHENTICATOR: {
      const manifestLoginRequester = authenticator.login_requester;
      if (
        manifestLoginRequester.authenticator &&
        manifestLoginRequester.authenticator?.type !== NO_AUTH &&
        manifestLoginRequester.authenticator?.type !== API_KEY_AUTHENTICATOR &&
        manifestLoginRequester.authenticator?.type !== BEARER_AUTHENTICATOR &&
        manifestLoginRequester.authenticator?.type !== BASIC_AUTHENTICATOR
      ) {
        throw new ManifestCompatibilityError(
          streamName,
          `SessionTokenAuthenticator login_requester.authenticator must have one of the following types: ${NO_AUTH}, ${API_KEY_AUTHENTICATOR}, ${BEARER_AUTHENTICATOR}, ${BASIC_AUTHENTICATOR}`
        );
      }
      const builderLoginRequesterAuthenticator = manifestAuthenticatorToBuilder(
        manifestLoginRequester.authenticator,
        spec,
        streamName
      );

      return {
        ...authenticator,
        login_requester: {
          url: `${removeTrailingSlashes(manifestLoginRequester.url_base)}/${removeLeadingSlashes(
            manifestLoginRequester.path
          )}`,
          authenticator: builderLoginRequesterAuthenticator as
            | ApiKeyAuthenticator
            | BearerAuthenticator
            | BasicHttpAuthenticator,
          httpMethod: manifestLoginRequester.http_method === "GET" ? "GET" : "POST",
          requestOptions: {
            requestParameters: Object.entries(manifestLoginRequester.request_parameters ?? {}),
            requestHeaders: Object.entries(manifestLoginRequester.request_headers ?? {}),
            requestBody: requesterToRequestBody(manifestLoginRequester),
          },
          errorHandler: manifestErrorHandlerToBuilder(manifestLoginRequester.error_handler),
        },
      };
    }
  }
}

function manifestSpecToBuilderInputs(
  manifestSpec: Spec | undefined,
  authenticator: BuilderFormAuthenticator,
  streams: BuilderStream[]
) {
  if (manifestSpec === undefined) {
    return [];
  }

  const lockedInputKeys = Object.keys(getKeyToDesiredLockedInput(authenticator, streams));

  const required = manifestSpec.connection_specification.required as string[] | undefined;

  return Object.entries(manifestSpec.connection_specification.properties as Record<string, AirbyteJSONSchema>)
    .sort(([_keyA, valueA], [_keyB, valueB]) => {
      if (valueA.order !== undefined && valueB.order !== undefined) {
        return valueA.order - valueB.order;
      }
      if (valueA.order !== undefined && valueB.order === undefined) {
        return -1;
      }
      if (valueA.order === undefined && valueB.order !== undefined) {
        return 1;
      }
      return 0;
    })
    .map(([specKey, specDefinition]) => {
      return {
        key: specKey,
        definition: specDefinition,
        required: required?.includes(specKey) || false,
        isLocked: lockedInputKeys.includes(specKey),
      };
    });
}

function assertType<T extends { type: string }>(
  object: { type: string },
  typeString: string,
  streamName: string | undefined
): asserts object is T {
  if (object.type !== typeString) {
    throw new ManifestCompatibilityError(streamName, `doesn't use a ${typeString}`);
  }
}

export class ManifestCompatibilityError extends Error {
  __type = "connectorBuilder.manifestCompatibility";

  constructor(
    public streamName: string | undefined,
    public message: string
  ) {
    const errorMessage = `${streamName ? `Stream ${streamName}: ` : ""}${message}`;
    super(errorMessage);
    this.message = errorMessage;
  }
}

export function isManifestCompatibilityError(error: { __type?: string }): error is ManifestCompatibilityError {
  return error.__type === "connectorBuilder.manifestCompatibility";
}

function convertOrDumpAsString<ManifestInput, BuilderOutput>(
  manifestValue: ManifestInput,
  convertFn: (manifestValue: ManifestInput, streamName?: string, spec?: Spec) => BuilderOutput | undefined,
  componentName: YamlSupportedComponentName,
  streamName?: string | undefined,
  metadata?: DeclarativeComponentSchemaMetadata,
  spec?: Spec
): BuilderOutput | YamlString | undefined {
  if (streamName && metadata?.yamlComponents?.streams?.[streamName]?.includes(componentName)) {
    return dump(manifestValue);
  }

  try {
    return convertFn(manifestValue, streamName, spec);
  } catch (e) {
    if (isManifestCompatibilityError(e)) {
      return dump(manifestValue);
    }
    throw e;
  }
}

const extractAndValidateAuthKey = (
  path: string[],
  authenticator: SupportedAuthenticator,
  manifestSpec: Spec | undefined,
  streamName?: string
) => {
  return extractAndValidateSpecKey(
    path,
    get(authenticator, path),
    get(LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[authenticator.type], path),
    authenticator.type,
    manifestSpec,
    streamName
  );
};

const tryExtractAndValidateIncrementalKey = (
  path: string[],
  value: string,
  manifestSpec: Spec | undefined,
  streamName?: string
) => {
  try {
    return extractAndValidateSpecKey(
      path,
      value,
      get(LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME, path),
      DatetimeBasedCursorType.DatetimeBasedCursor,
      manifestSpec,
      streamName
    );
  } catch (e) {
    if (isManifestCompatibilityError(e)) {
      // if the manifest value doesn't point to the expected input in the spec, just treat it as custom
      return undefined;
    }
    throw e;
  }
};

const extractAndValidateSpecKey = (
  path: string[],
  value: string | string[] | undefined,
  lockedInput: BuilderFormInput,
  componentName: string,
  spec: Spec | undefined,
  streamName?: string
): string => {
  const manifestPath = `${componentName}.${path.join(".")}`;

  let specKey: string | undefined = undefined;
  if (isArray(value)) {
    if (value.length < 1) {
      throw new ManifestCompatibilityError(
        streamName,
        `${manifestPath} has an empty path, but a non-empty path is required.`
      );
    }
    if (value.length > 1) {
      throw new ManifestCompatibilityError(
        streamName,
        `${manifestPath} points to a nested config path, but only top-level config fields are supported.`
      );
    }
    [specKey] = value;
  }
  if (isString(value)) {
    if (!isInterpolatedConfigKey(value)) {
      throw new ManifestCompatibilityError(streamName, `${manifestPath} must be of the form {{ config["key"] }}`);
    }
    specKey = extractInterpolatedConfigKey(value);
  }
  if (!specKey) {
    throw new ManifestCompatibilityError(streamName, `${manifestPath} must point to a config field`);
  }

  const specDefinition = specKey ? spec?.connection_specification?.properties?.[specKey] : undefined;
  if (!specDefinition) {
    throw new ManifestCompatibilityError(
      streamName,
      `${manifestPath} references spec key "${specKey}", which must appear in the spec`
    );
  }
  if (lockedInput.required && !spec?.connection_specification?.required?.includes(specKey)) {
    throw new ManifestCompatibilityError(
      streamName,
      `${manifestPath} references spec key "${specKey}", which must be required in the spec`
    );
  }
  if (specDefinition.type !== "string") {
    throw new ManifestCompatibilityError(
      streamName,
      `${manifestPath} references spec key "${specKey}", which must be of type string`
    );
  }
  if (lockedInput.definition.airbyte_secret && !specDefinition.airbyte_secret) {
    throw new ManifestCompatibilityError(
      streamName,
      `${manifestPath} references spec key "${specKey}", which must have airbyte_secret set to true`
    );
  }
  if (lockedInput.definition.pattern && specDefinition.pattern !== lockedInput.definition.pattern) {
    throw new ManifestCompatibilityError(
      streamName,
      `${manifestPath} references spec key "${specKey}", which must have pattern "${lockedInput.definition.pattern}"`
    );
  }

  return specKey;
};
