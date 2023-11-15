import cloneDeep from "lodash/cloneDeep";
import isEqual from "lodash/isEqual";
import pick from "lodash/pick";

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
  CursorPagination,
  DeclarativeComponentSchemaMetadata,
  HttpRequesterErrorHandler,
} from "core/api/types/ConnectorManifest";
import { removeEmptyProperties } from "core/utils/form";

import {
  API_KEY_AUTHENTICATOR,
  authTypeToKeyToInferredInput,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  BuilderFormAuthenticator,
  BuilderFormValues,
  BuilderIncrementalSync,
  BuilderPaginator,
  BuilderRequestBody,
  BuilderStream,
  BuilderTransformation,
  DEFAULT_BUILDER_FORM_VALUES,
  DEFAULT_BUILDER_STREAM_VALUES,
  extractInterpolatedConfigKey,
  getInferredAuthValue,
  hasIncrementalSyncUserInput,
  INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  incrementalSyncInferredInputs,
  isInterpolatedConfigKey,
  NO_AUTH,
  OAUTH_ACCESS_TOKEN_INPUT,
  OAUTH_TOKEN_EXPIRY_DATE_INPUT,
  RequestOptionOrPathInject,
} from "./types";
import { formatJson } from "./utils";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";

export const convertToBuilderFormValuesSync = (resolvedManifest: ConnectorManifest) => {
  const builderFormValues = cloneDeep(DEFAULT_BUILDER_FORM_VALUES);
  builderFormValues.checkStreams = resolvedManifest.check.stream_names;

  const streams = resolvedManifest.streams;
  if (streams === undefined || streams.length === 0) {
    const { inputs, inferredInputOverrides, inputOrder } = manifestSpecAndAuthToBuilder(
      resolvedManifest.spec,
      undefined,
      undefined
    );
    builderFormValues.inputs = inputs;
    builderFormValues.inferredInputOverrides = inferredInputOverrides;
    builderFormValues.inputOrder = inputOrder;

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
      resolvedManifest.metadata
    )
  );

  const { inputs, inferredInputOverrides, auth, inputOrder } = manifestSpecAndAuthToBuilder(
    resolvedManifest.spec,
    streams[0].retriever.requester.authenticator,
    builderFormValues.streams
  );
  builderFormValues.inputs = inputs;
  builderFormValues.inferredInputOverrides = inferredInputOverrides;
  builderFormValues.global.authenticator = auth;
  builderFormValues.inputOrder = inputOrder;

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
  metadata?: DeclarativeComponentSchemaMetadata
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
    name: stream.name ?? "",
    urlPath: requester.path,
    httpMethod: requester.http_method === "POST" ? "POST" : "GET",
    fieldPointer: retriever.record_selector.extractor.field_path as string[],
    requestOptions: {
      requestParameters: Object.entries(requester.request_parameters ?? {}),
      requestHeaders: Object.entries(requester.request_headers ?? {}),
      requestBody: requesterToRequestBody(requester),
    },
    primaryKey: manifestPrimaryKeyToBuilder(stream),
    paginator: manifestPaginatorToBuilder(retriever.paginator, stream.name),
    incrementalSync: manifestIncrementalSyncToBuilder(stream.incremental_sync, stream.name),
    parentStreams,
    parameterizedRequests,
    schema: manifestSchemaLoaderToBuilderSchema(stream.schema_loader),
    errorHandler: manifestErrorHandlerToBuilder(stream.name, requester.error_handler),
    transformations: manifestTransformationsToBuilder(stream.name, stream.transformations),
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
  if (requester.request_body_data && typeof requester.request_body_data === "string") {
    return { type: "string_freeform", value: requester.request_body_data };
  }
  if (!requester.request_body_json) {
    return { type: "json_list", values: [] };
  }
  const allStringValues = Object.values(requester.request_body_json).every((value) => typeof value === "string");
  if (allStringValues) {
    return { type: "json_list", values: Object.entries(requester.request_body_json) };
  }
  return {
    type: "json_freeform",
    value:
      typeof requester.request_body_json === "string"
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
          values:
            typeof partitionRouter.values === "string"
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

function manifestErrorHandlerToBuilder(
  streamName: string | undefined,
  errorHandler: HttpRequesterErrorHandler | undefined
): BuilderStream["errorHandler"] {
  if (!errorHandler) {
    return undefined;
  }
  const handlers = errorHandler.type === "CompositeErrorHandler" ? errorHandler.error_handlers : [errorHandler];
  if (handlers.some((handler) => handler.type === "CustomErrorHandler")) {
    throw new ManifestCompatibilityError(streamName, "custom error handler used");
  }
  if (handlers.some((handler) => handler.type === "CompositeErrorHandler")) {
    throw new ManifestCompatibilityError(streamName, "nested composite error handler used");
  }
  const defaultHandlers = handlers as DefaultErrorHandler[];
  return defaultHandlers.map((handler) => {
    if (handler.backoff_strategies && handler.backoff_strategies.length > 1) {
      throw new ManifestCompatibilityError(streamName, "more than one backoff strategy");
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

function manifestTransformationsToBuilder(
  name: string | undefined,
  transformations: DeclarativeStreamTransformationsItem[] | undefined
): BuilderTransformation[] | undefined {
  if (!transformations) {
    return undefined;
  }
  const builderTransformations: BuilderTransformation[] = [];

  transformations.forEach((transformation) => {
    if (transformation.type === "CustomTransformation") {
      throw new ManifestCompatibilityError(name, "custom transformation used");
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
  format: DatetimeBasedCursorStartDatetime | DatetimeBasedCursorEndDatetime,
  manifestIncrementalSync: DeclarativeStreamIncrementalSync
) {
  if (typeof format === "string" || !format.datetime_format) {
    return manifestIncrementalSync.datetime_format;
  }
  return format.datetime_format;
}

function isFormatSupported(
  format: DatetimeBasedCursorStartDatetime | DatetimeBasedCursorEndDatetime,
  manifestIncrementalSync: DeclarativeStreamIncrementalSync
) {
  return getFormat(format, manifestIncrementalSync) === INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT;
}

function manifestIncrementalSyncToBuilder(
  manifestIncrementalSync: DeclarativeStreamIncrementalSync | undefined,
  streamName?: string
): BuilderStream["incrementalSync"] | undefined {
  if (!manifestIncrementalSync) {
    return undefined;
  }
  if (manifestIncrementalSync.type === "CustomIncrementalSync") {
    throw new ManifestCompatibilityError(streamName, "incremental sync uses a custom implementation");
  }

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

  let start_datetime: BuilderIncrementalSync["start_datetime"] = {
    type: "custom",
    value: typeof manifestStartDateTime === "string" ? manifestStartDateTime : manifestStartDateTime.datetime,
    format: getFormat(manifestStartDateTime, manifestIncrementalSync),
  };
  let end_datetime: BuilderIncrementalSync["end_datetime"] = {
    type: "custom",
    value: typeof manifestEndDateTime === "string" ? manifestEndDateTime : manifestEndDateTime?.datetime || "",
    format: manifestEndDateTime ? getFormat(manifestEndDateTime, manifestIncrementalSync) : undefined,
  };

  if (
    start_datetime.value === "{{ config['start_date'] }}" &&
    isFormatSupported(manifestStartDateTime, manifestIncrementalSync)
  ) {
    start_datetime = { type: "user_input" };
  }

  if (
    end_datetime.value === "{{ config['end_date'] }}" &&
    manifestEndDateTime &&
    isFormatSupported(manifestEndDateTime, manifestIncrementalSync)
  ) {
    end_datetime = { type: "user_input" };
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
  const { cursor_value, stop_condition, ...rest } = strategy as CursorPagination;

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

function manifestPaginatorToBuilder(
  manifestPaginator: SimpleRetrieverPaginator | undefined,
  streamName: string | undefined
): BuilderPaginator | undefined {
  if (manifestPaginator === undefined || manifestPaginator.type === "NoPagination") {
    return undefined;
  }

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

function manifestAuthenticatorToBuilder(
  manifestAuthenticator: HttpRequesterAuthenticator | undefined,
  streamName?: string
): BuilderFormAuthenticator {
  let builderAuthenticator: BuilderFormAuthenticator;
  if (manifestAuthenticator === undefined) {
    builderAuthenticator = {
      type: "NoAuth",
    };
  } else if (manifestAuthenticator.type === undefined) {
    throw new ManifestCompatibilityError(streamName, "authenticator has no type");
  } else if (manifestAuthenticator.type === "CustomAuthenticator") {
    throw new ManifestCompatibilityError(streamName, "uses a CustomAuthenticator");
  } else if (manifestAuthenticator.type === "LegacySessionTokenAuthenticator") {
    throw new ManifestCompatibilityError(streamName, "uses a LegacySessionTokenAuthenticator");
  } else if (manifestAuthenticator.type === "ApiKeyAuthenticator") {
    builderAuthenticator = {
      ...manifestAuthenticator,
      inject_into: manifestAuthenticator.inject_into ?? {
        type: "RequestOption",
        field_name: manifestAuthenticator.header || "",
        inject_into: "header",
      },
    };
  } else if (manifestAuthenticator.type === "OAuthAuthenticator") {
    if (
      Object.values(manifestAuthenticator.refresh_request_body ?? {}).filter((value) => typeof value !== "string")
        .length > 0
    ) {
      throw new ManifestCompatibilityError(
        streamName,
        "OAuthAuthenticator contains a refresh_request_body with non-string values"
      );
    }

    const refreshTokenUpdater = manifestAuthenticator.refresh_token_updater;
    if (refreshTokenUpdater) {
      if (!isEqual(refreshTokenUpdater?.access_token_config_path, [OAUTH_ACCESS_TOKEN_INPUT])) {
        throw new ManifestCompatibilityError(
          streamName,
          `OAuthAuthenticator access token config path needs to be [${OAUTH_ACCESS_TOKEN_INPUT}]`
        );
      }
      if (!isEqual(refreshTokenUpdater?.token_expiry_date_config_path, [OAUTH_TOKEN_EXPIRY_DATE_INPUT])) {
        throw new ManifestCompatibilityError(
          streamName,
          `OAuthAuthenticator token expiry date config path needs to be [${OAUTH_TOKEN_EXPIRY_DATE_INPUT}]`
        );
      }
      if (
        !isEqual(refreshTokenUpdater?.refresh_token_config_path, [
          extractInterpolatedConfigKey(manifestAuthenticator.refresh_token),
        ])
      ) {
        throw new ManifestCompatibilityError(
          streamName,
          "OAuthAuthenticator refresh_token_config_path needs to match the config value used for refresh_token"
        );
      }
    }
    if (
      manifestAuthenticator.grant_type &&
      manifestAuthenticator.grant_type !== "refresh_token" &&
      manifestAuthenticator.grant_type !== "client_credentials"
    ) {
      throw new ManifestCompatibilityError(streamName, "OAuthAuthenticator sets custom grant_type");
    }

    builderAuthenticator = {
      ...manifestAuthenticator,
      refresh_request_body: Object.entries(manifestAuthenticator.refresh_request_body ?? {}),
      grant_type: manifestAuthenticator.grant_type ?? "refresh_token",
    };
  } else if (manifestAuthenticator.type === "SessionTokenAuthenticator") {
    const manifestLoginRequester = manifestAuthenticator.login_requester;
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
    builderAuthenticator = {
      ...manifestAuthenticator,
      login_requester: {
        url: `${removeTrailingSlashes(manifestLoginRequester.url_base)}/${removeLeadingSlashes(
          manifestLoginRequester.path
        )}`,
        authenticator: manifestLoginRequester.authenticator ?? { type: NO_AUTH },
        httpMethod: manifestLoginRequester.http_method === "GET" ? "GET" : "POST",
        requestOptions: {
          requestParameters: Object.entries(manifestLoginRequester.request_parameters ?? {}),
          requestHeaders: Object.entries(manifestLoginRequester.request_headers ?? {}),
          requestBody: requesterToRequestBody(manifestLoginRequester),
        },
        errorHandler: manifestErrorHandlerToBuilder(undefined, manifestLoginRequester.error_handler),
      },
    };
  } else {
    builderAuthenticator = manifestAuthenticator;
  }

  // verify that all auth keys which require a user input have a {{config[]}} value

  const inferredInputs = authTypeToKeyToInferredInput(builderAuthenticator);
  const userInputAuthKeys = Object.keys(inferredInputs);

  for (const userInputAuthKey of userInputAuthKeys) {
    if (
      !inferredInputs[userInputAuthKey].as_config_path &&
      !isInterpolatedConfigKey(getInferredAuthValue(builderAuthenticator, userInputAuthKey))
    ) {
      throw new ManifestCompatibilityError(
        undefined,
        `Authenticator's ${userInputAuthKey} value must be of the form {{ config['key'] }}`
      );
    }
  }

  return builderAuthenticator;
}

function manifestSpecAndAuthToBuilder(
  manifestSpec: Spec | undefined,
  manifestAuthenticator: HttpRequesterAuthenticator | undefined,
  streams: BuilderStream[] | undefined
) {
  const result: {
    inputs: BuilderFormValues["inputs"];
    inferredInputOverrides: BuilderFormValues["inferredInputOverrides"];
    auth: BuilderFormAuthenticator;
    inputOrder: string[];
  } = {
    inputs: [],
    inferredInputOverrides: {},
    auth: manifestAuthenticatorToBuilder(manifestAuthenticator),
    inputOrder: [],
  };

  if (manifestSpec === undefined) {
    return result;
  }

  const required = manifestSpec.connection_specification.required as string[] | undefined;

  Object.entries(manifestSpec.connection_specification.properties as Record<string, AirbyteJSONSchema>)
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
    .forEach(([specKey, specDefinition]) => {
      const matchingInferredInput = getMatchingInferredInput(result.auth, streams, specKey);
      if (matchingInferredInput) {
        result.inferredInputOverrides[matchingInferredInput.key] = specDefinition;
      } else {
        result.inputs.push({
          key: specKey,
          definition: specDefinition,
          required: required?.includes(specKey) || false,
        });
      }
      if (specDefinition.order !== undefined) {
        result.inputOrder.push(specKey);
      }
    });

  return result;
}

function getMatchingInferredInput(
  auth: BuilderFormAuthenticator,
  streams: BuilderStream[] | undefined,
  specKey: string
) {
  if (streams && specKey === "start_date" && hasIncrementalSyncUserInput(streams, "start_datetime")) {
    return incrementalSyncInferredInputs.start_date;
  }
  if (streams && specKey === "end_date" && hasIncrementalSyncUserInput(streams, "end_datetime")) {
    return incrementalSyncInferredInputs.end_date;
  }
  return Object.values(authTypeToKeyToInferredInput(auth)).find((input) => input.key === specKey);
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
