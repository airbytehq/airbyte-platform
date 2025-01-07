import { dump } from "js-yaml";
import cloneDeep from "lodash/cloneDeep";
import get from "lodash/get";
import isArray from "lodash/isArray";
import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import isObject from "lodash/isObject";
import isString from "lodash/isString";
import pick from "lodash/pick";
import { match } from "ts-pattern";

import {
  ConnectorManifest,
  DeclarativeStream,
  DeclarativeStreamIncrementalSync,
  DeclarativeStreamSchemaLoader,
  DeclarativeStreamTransformationsItem,
  HttpRequester,
  HttpRequesterAuthenticator,
  SimpleRetriever,
  SimpleRetrieverPaginator,
  SimpleRetrieverPartitionRouter,
  Spec,
  DatetimeBasedCursorEndDatetime,
  DatetimeBasedCursorStartDatetime,
  ApiKeyAuthenticator,
  BasicHttpAuthenticator,
  BearerAuthenticator,
  OAuthAuthenticator,
  DefaultPaginator,
  HttpRequesterErrorHandler,
  NoAuth,
  SessionTokenAuthenticator,
  DatetimeBasedCursorType,
  RecordSelector,
  SimpleRetrieverPartitionRouterAnyOfItem,
  RequestOption,
  PrimaryKey,
  DatetimeBasedCursor,
  DefaultErrorHandler,
  JsonDecoderType,
  JsonlDecoderType,
  XmlDecoderType,
  IterableDecoderType,
  SimpleRetrieverDecoder,
  GzipJsonDecoderType,
} from "core/api/types/ConnectorManifest";

import {
  API_KEY_AUTHENTICATOR,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  BuilderDecoder,
  BuilderErrorHandler,
  BuilderFormAuthenticator,
  BuilderFormInput,
  BuilderFormValues,
  BuilderIncrementalSync,
  BuilderMetadata,
  BuilderPaginator,
  BuilderParameterizedRequests,
  BuilderParentStream,
  BuilderRecordSelector,
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
  SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR,
  SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR,
  YamlString,
  YamlSupportedComponentName,
} from "./types";
import {
  getKeyToDesiredLockedInput,
  LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE,
  LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME,
} from "./useLockedInputs";
import { filterPartitionRouterToType, formatJson, streamNameOrDefault, streamRef } from "./utils";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";

export const convertToBuilderFormValuesSync = (resolvedManifest: ConnectorManifest) => {
  const builderFormValues = cloneDeep(DEFAULT_BUILDER_FORM_VALUES);
  builderFormValues.checkStreams = resolvedManifest.check.stream_names;
  builderFormValues.description = resolvedManifest.description;

  const streams = resolvedManifest.streams;
  if (streams === undefined || streams.length === 0) {
    builderFormValues.inputs = manifestSpecToBuilderInputs(resolvedManifest.spec, { type: NO_AUTH }, []);
    return builderFormValues;
  }

  assertType<SimpleRetriever>(streams[0].retriever, "SimpleRetriever", streams[0].name);
  const firstStreamRetriever: SimpleRetriever = streams[0].retriever;
  assertType<HttpRequester>(firstStreamRetriever.requester, "HttpRequester", streams[0].name);
  builderFormValues.global.urlBase = firstStreamRetriever.requester.url_base;

  const builderMetadata = resolvedManifest.metadata ? (resolvedManifest.metadata as BuilderMetadata) : undefined;

  const getStreamName = (stream: DeclarativeStream, index: number) => streamNameOrDefault(stream.name, index);

  const streamNameToIndex = streams.reduce((acc, stream, index) => {
    const streamName = getStreamName(stream, index);
    return { ...acc, [streamName]: index.toString() };
  }, {});

  const serializedStreamToName = Object.fromEntries(
    streams.map((stream, index) => [formatJson(stream, true), getStreamName(stream, index)])
  );
  builderFormValues.streams = streams.map((stream, index) =>
    manifestStreamToBuilder(
      stream,
      getStreamName(stream, index),
      index.toString(),
      streamNameToIndex,
      serializedStreamToName,
      firstStreamRetriever.requester.url_base,
      firstStreamRetriever.requester.authenticator,
      builderMetadata,
      resolvedManifest.spec
    )
  );

  builderFormValues.assist = builderMetadata?.assist ?? {};

  builderFormValues.global.authenticator = convertOrDumpAsString(
    streams[0].retriever.requester.authenticator,
    (authenticator: HttpRequesterAuthenticator | undefined, _streamName?: string, spec?: Spec) =>
      manifestAuthenticatorToBuilder(authenticator, spec),
    {
      name: "authenticator",
      streamName: undefined,
    },
    builderMetadata,
    resolvedManifest.spec
  );

  builderFormValues.inputs = manifestSpecToBuilderInputs(
    resolvedManifest.spec,
    builderFormValues.global.authenticator,
    builderFormValues.streams
  );

  return builderFormValues;
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
  "access_token_value",
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
  streamName: string,
  streamId: string,
  streamNameToId: Record<string, string>,
  serializedStreamToName: Record<string, string>,
  firstStreamUrlBase: string,
  firstStreamAuthenticator?: HttpRequesterAuthenticator,
  metadata?: BuilderMetadata,
  spec?: Spec
): BuilderStream => {
  const {
    type,
    incremental_sync,
    name,
    primary_key,
    retriever,
    schema_loader,
    transformations,
    ...unknownStreamFields
  } = stream;
  assertType<SimpleRetriever>(retriever, "SimpleRetriever", streamName);

  const {
    type: retrieverType,
    paginator,
    partition_router,
    record_selector,
    requester,
    decoder,
    ...unknownRetrieverFields
  } = retriever;
  assertType<HttpRequester>(requester, "HttpRequester", stream.name);

  const {
    type: requesterType,
    authenticator,
    error_handler,
    http_method,
    path,
    request_body_data,
    request_body_json,
    request_headers,
    request_parameters,
    url_base,
    ...unknownRequesterFields
  } = requester;

  const cleanedAuthenticator = pick(authenticator, authenticatorKeysToCheck);
  const cleanedFirstStreamAuthenticator = pick(firstStreamAuthenticator, authenticatorKeysToCheck);

  if (
    !firstStreamAuthenticator || firstStreamAuthenticator.type === "NoAuth"
      ? authenticator && authenticator.type !== "NoAuth"
      : !isEqual(cleanedAuthenticator, cleanedFirstStreamAuthenticator)
  ) {
    throw new ManifestCompatibilityError(streamName, "authenticator does not match the first stream's");
  }

  if (url_base !== firstStreamUrlBase) {
    throw new ManifestCompatibilityError(streamName, "url_base does not match the first stream's");
  }

  if (![undefined, "GET", "POST"].includes(http_method)) {
    throw new ManifestCompatibilityError(streamName, "http_method is not GET or POST");
  }

  const substreamPartitionRouterToBuilder = (
    partitionRouter: SimpleRetrieverPartitionRouter | undefined,
    streamName?: string
  ) => manifestSubstreamPartitionRouterToBuilder(partitionRouter, streamNameToId, streamName);

  return {
    ...DEFAULT_BUILDER_STREAM_VALUES,
    id: streamId,
    name: streamName,
    urlPath: path,
    httpMethod: http_method === "POST" ? "POST" : "GET",
    decoder: manifestDecoderToBuilder(decoder, streamName),
    requestOptions: {
      requestParameters: Object.entries(request_parameters ?? {}),
      requestHeaders: Object.entries(request_headers ?? {}),
      requestBody: requesterToRequestBody(requester),
    },
    primaryKey: manifestPrimaryKeyToBuilder(primary_key, streamName),
    recordSelector: convertOrDumpAsString(
      record_selector,
      manifestRecordSelectorToBuilder,
      {
        name: "recordSelector",
        streamName,
      },
      metadata
    ),
    paginator: convertOrDumpAsString(
      paginator,
      manifestPaginatorToBuilder,
      {
        name: "paginator",
        streamName,
      },
      metadata
    ),
    incrementalSync: convertOrDumpAsString(
      incremental_sync,
      manifestIncrementalSyncToBuilder,
      {
        name: "incrementalSync",
        streamName,
      },
      metadata,
      spec
    ),
    parentStreams: convertOrDumpAsString(
      replaceParentStreamsWithRefs(
        filterPartitionRouterToType(partition_router, ["SubstreamPartitionRouter", "CustomPartitionRouter"]),
        serializedStreamToName,
        streamName
      ),
      substreamPartitionRouterToBuilder,
      {
        name: "parentStreams",
        streamName,
      },
      metadata
    ),
    parameterizedRequests: convertOrDumpAsString(
      filterPartitionRouterToType(partition_router, ["ListPartitionRouter"]),
      manifestListPartitionRouterToBuilder,
      {
        name: "parameterizedRequests",
        streamName,
      },
      metadata
    ),
    schema: manifestSchemaLoaderToBuilderSchema(schema_loader),
    errorHandler: convertOrDumpAsString(
      error_handler,
      manifestErrorHandlerToBuilder,
      {
        name: "errorHandler",
        streamName,
      },
      metadata
    ),
    transformations: convertOrDumpAsString(
      transformations,
      manifestTransformationsToBuilder,
      {
        name: "transformations",
        streamName,
      },
      metadata
    ),
    autoImportSchema: metadata?.autoImportSchema?.[streamName] === true,
    unknownFields:
      !isEmpty(unknownStreamFields) || !isEmpty(unknownRetrieverFields) || !isEmpty(unknownRequesterFields)
        ? dump({
            ...unknownStreamFields,
            ...((!isEmpty(unknownRetrieverFields) || !isEmpty(unknownRequesterFields)) && {
              retriever: {
                ...unknownRetrieverFields,
                ...(!isEmpty(unknownRequesterFields) && {
                  requester: {
                    ...unknownRequesterFields,
                  },
                }),
              },
            }),
          })
        : undefined,
    testResults: metadata?.testedStreams?.[streamName],
  };
};

function requesterToRequestBody(requester: HttpRequester): BuilderRequestBody {
  if (requester.request_body_data && isObject(requester.request_body_data)) {
    return { type: "form_list", values: Object.entries(requester.request_body_data) };
  }
  if (requester.request_body_data && isString(requester.request_body_data)) {
    return { type: "string_freeform", value: requester.request_body_data };
  }
  if (!requester.request_body_json) {
    return { type: "json_list", values: [] };
  }
  if (isString(requester.request_body_json)) {
    return { type: "string_freeform", value: requester.request_body_json };
  }
  if (
    isObject(requester.request_body_json) &&
    Object.values(requester.request_body_json).every((value) => isString(value))
  ) {
    return { type: "json_list", values: Object.entries(requester.request_body_json) };
  }
  return {
    type: "json_freeform",
    value: formatJson(requester.request_body_json),
  };
}

const manifestDecoderToBuilder = (decoder: SimpleRetrieverDecoder | undefined, streamName: string): BuilderDecoder => {
  const supportedDecoderTypes: Array<string | undefined> = [
    undefined,
    JsonDecoderType.JsonDecoder,
    JsonlDecoderType.JsonlDecoder,
    XmlDecoderType.XmlDecoder,
    IterableDecoderType.IterableDecoder,
    GzipJsonDecoderType.GzipJsonDecoder,
  ];
  const decoderType = decoder?.type;
  if (!supportedDecoderTypes.includes(decoderType)) {
    throw new ManifestCompatibilityError(streamName, "decoder is not supported");
  }

  switch (decoderType) {
    case JsonDecoderType.JsonDecoder:
      return "JSON";
    case XmlDecoderType.XmlDecoder:
      return "XML";
    case JsonlDecoderType.JsonlDecoder:
      return "JSON Lines";
    case IterableDecoderType.IterableDecoder:
      return "Iterable";
    case GzipJsonDecoderType.GzipJsonDecoder:
      return "gzip JSON";
    default:
      return "JSON";
  }
};

export function manifestRecordSelectorToBuilder(
  recordSelector: RecordSelector,
  streamName?: string
): BuilderRecordSelector | undefined {
  assertType(recordSelector, "RecordSelector", streamName);
  const selector = filterKnownFields(
    recordSelector,
    ["type", "extractor", "record_filter", "schema_normalization"],
    recordSelector.type,
    streamName
  );

  assertType(selector.extractor, "DpathExtractor", streamName);
  const extractor = filterKnownFields(
    selector.extractor,
    ["type", "field_path"],
    `${selector.type}.extractor`,
    streamName
  );

  const filter = filterKnownFields(
    selector.record_filter,
    ["type", "condition"],
    `${selector.type}.record_filter`,
    streamName
  );
  if (filter) {
    assertType(filter, "RecordFilter", streamName);
  }

  if (
    extractor.field_path.length === 0 &&
    !filter &&
    (!selector.schema_normalization || selector.schema_normalization === "None")
  ) {
    return undefined;
  }

  return {
    fieldPath: extractor.field_path as string[],
    filterCondition: filter?.condition,
    normalizeToSchema: selector.schema_normalization === "Default",
  };
}

export function manifestListPartitionRouterToBuilder(
  partitionRouter: SimpleRetrieverPartitionRouter | undefined,
  streamName?: string
): BuilderParameterizedRequests[] | undefined {
  if (partitionRouter === undefined) {
    return undefined;
  }

  if (Array.isArray(partitionRouter)) {
    const builderParameterizedRequests = partitionRouter
      .map((subRouter) => manifestListPartitionRouterToBuilder(subRouter, streamName))
      .flatMap((subRouter) => subRouter ?? []);
    return builderParameterizedRequests.length === 0 ? undefined : builderParameterizedRequests;
  }

  if (partitionRouter.type !== "ListPartitionRouter") {
    throw new ManifestCompatibilityError(streamName, "a non-ListPartitionRouter is used");
  }

  const listPartitionRouter = filterKnownFields(
    partitionRouter,
    ["type", "cursor_field", "request_option", "values"],
    partitionRouter.type,
    streamName
  );
  if (listPartitionRouter.request_option) {
    filterRequestOption(listPartitionRouter.request_option, `${partitionRouter.type}.request_option`, streamName);
  }

  return [
    {
      ...listPartitionRouter,
      values: isString(listPartitionRouter.values)
        ? {
            value: listPartitionRouter.values,
            type: "variable" as const,
          }
        : {
            value: listPartitionRouter.values,
            type: "list" as const,
          },
    },
  ];
}

function replaceParentStreamsWithRefs(
  partitionRouters: SimpleRetrieverPartitionRouterAnyOfItem[] | undefined,
  serializedStreamToName: Record<string, string>,
  streamName?: string
) {
  return partitionRouters?.map((router) => {
    if (router.type === "SubstreamPartitionRouter") {
      return {
        ...router,
        parent_stream_configs: router.parent_stream_configs.map((parentStreamConfig) => {
          const parentStreamName = serializedStreamToName[formatJson(parentStreamConfig.stream, true)];
          if (!parentStreamName) {
            throw new ManifestCompatibilityError(
              streamName,
              "SubstreamPartitionRouter's parent stream doesn't match any other stream"
            );
          }
          return {
            ...parentStreamConfig,
            stream: streamRef(parentStreamName),
          };
        }),
      };
    }
    return router;
  });
}

export function manifestSubstreamPartitionRouterToBuilder(
  partitionRouter: SimpleRetrieverPartitionRouter | undefined,
  streamNameToId: Record<string, string>,
  streamName?: string
): BuilderParentStream[] | undefined {
  if (partitionRouter === undefined) {
    return undefined;
  }

  if (Array.isArray(partitionRouter)) {
    return partitionRouter
      .map((subRouter) => manifestSubstreamPartitionRouterToBuilder(subRouter, streamNameToId, streamName))
      .flatMap((subRouter) => subRouter ?? []);
  }

  if (partitionRouter.type !== "SubstreamPartitionRouter") {
    throw new ManifestCompatibilityError(streamName, "a non-SubstreamPartitionRouter is used");
  }

  const substreamPartitionRouter = filterKnownFields(
    partitionRouter,
    ["type", "parent_stream_configs"],
    partitionRouter.type,
    streamName
  );

  if (substreamPartitionRouter.parent_stream_configs.length < 1) {
    throw new ManifestCompatibilityError(streamName, "SubstreamPartitionRouter has no parent streams");
  }

  if (substreamPartitionRouter.parent_stream_configs.length > 1) {
    throw new ManifestCompatibilityError(streamName, "SubstreamPartitionRouter has more than one parent stream");
  }

  const parentStreamConfig = filterKnownFields(
    substreamPartitionRouter.parent_stream_configs[0],
    ["type", "parent_key", "partition_field", "request_option", "stream", "incremental_dependency"],
    `${partitionRouter.type}.parent_stream_configs`,
    streamName
  );
  if (parentStreamConfig.request_option) {
    filterRequestOption(
      parentStreamConfig.request_option,
      `${partitionRouter.type}.parent_stream_configs.request_option`
    );
  }

  if (!parentStreamConfig.stream.$ref) {
    throw new ManifestCompatibilityError(
      streamName,
      "SubstreamPartitionRouter.parent_stream_configs.stream must use $ref"
    );
  }
  const parentStream = filterKnownFields(
    parentStreamConfig.stream,
    ["$ref"],
    `${partitionRouter.type}.parent_stream_configs.stream`,
    streamName
  );
  const streamRefPattern = /^#\/definitions\/streams\/([^/]+)$/;
  const match = streamRefPattern.exec(parentStream.$ref);

  if (!match) {
    throw new ManifestCompatibilityError(
      streamName,
      "SubstreamPartitionRouter's parent stream reference must match the pattern '#/definitions/streams/streamName'"
    );
  }

  const parentStreamName = match[1];
  const parentStreamId = streamNameToId[parentStreamName];
  if (!parentStreamId) {
    throw new ManifestCompatibilityError(
      streamName,
      `SubstreamPartitionRouter references parent stream name '${parentStreamName}' which could not be found`
    );
  }

  return [
    {
      parent_key: parentStreamConfig.parent_key,
      partition_field: parentStreamConfig.partition_field,
      parentStreamReference: parentStreamId,
      request_option: parentStreamConfig.request_option,
      incremental_dependency: parentStreamConfig.incremental_dependency,
    },
  ];
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

  const defaultHandlers: DefaultErrorHandler[] = handlers.map((handler) =>
    match(handler)
      .with({ type: "DefaultErrorHandler" }, (defaultHandler) => defaultHandler)
      .with({ type: "CompositeErrorHandler" }, () => {
        throw new ManifestCompatibilityError(streamName, "nested composite error handlers are not supported");
      })
      .otherwise((handler) => {
        throw new ManifestCompatibilityError(streamName, `error handler type '${handler.type}' is unsupported`);
      })
  );

  return defaultHandlers.map((handler) => {
    const defaultHandler = filterKnownFields(
      handler,
      ["type", "backoff_strategies", "response_filters", "max_retries"],
      handler.type,
      streamName
    );

    if (defaultHandler.backoff_strategies && defaultHandler.backoff_strategies.length > 1) {
      throw new ManifestCompatibilityError(streamName, "more than one backoff strategy per handler");
    }
    const backoffStrategy = match(defaultHandler.backoff_strategies?.[0])
      .with(undefined, () => undefined)
      .with({ type: "ConstantBackoffStrategy" }, (strategy) => ({
        ...filterKnownFields(strategy, ["type", "backoff_time_in_seconds"], strategy.type, streamName),
        backoff_time_in_seconds: convertToNumber(
          strategy.backoff_time_in_seconds,
          `${strategy.type}.backoff_time_in_seconds`,
          streamName
        ),
      }))
      .with({ type: "ExponentialBackoffStrategy" }, (strategy) => ({
        ...filterKnownFields(strategy, ["type", "factor"], strategy.type, streamName),
        factor: convertToNumber(strategy.factor, `${strategy.type}.factor`, streamName),
      }))
      .with({ type: "WaitUntilTimeFromHeader" }, (strategy) => ({
        ...filterKnownFields(strategy, ["type", "header", "regex", "min_wait"], strategy.type, streamName),
        min_wait: convertToNumber(strategy.min_wait, `${strategy.type}.min_wait`, streamName),
      }))
      .with({ type: "WaitTimeFromHeader" }, (strategy) =>
        filterKnownFields(strategy, ["type", "header", "regex"], strategy.type, streamName)
      )
      .otherwise((strategy) => {
        throw new ManifestCompatibilityError(streamName, `unsupported backoff strategy type: ${strategy.type}`);
      });

    if (defaultHandler.response_filters && defaultHandler.response_filters.length > 1) {
      throw new ManifestCompatibilityError(streamName, "more than one response filter per handler");
    }
    const responseFilter = filterKnownFields(
      defaultHandler.response_filters?.[0],
      ["type", "action", "error_message", "error_message_contains", "http_codes", "predicate"],
      defaultHandler.response_filters?.[0]?.type,
      streamName
    );

    return {
      ...handler,
      response_filters: undefined,
      response_filter: responseFilter
        ? { ...responseFilter, http_codes: responseFilter.http_codes?.map((code) => String(code)) }
        : undefined,
      backoff_strategies: undefined,
      backoff_strategy: backoffStrategy,
    };
  });
}

function manifestPrimaryKeyToBuilder(
  primaryKey: PrimaryKey | undefined,
  streamName?: string
): BuilderStream["primaryKey"] {
  if (primaryKey === undefined) {
    return [];
  } else if (Array.isArray(primaryKey)) {
    if (primaryKey.length > 0 && Array.isArray(primaryKey[0])) {
      throw new ManifestCompatibilityError(streamName, "primary_key contains nested arrays");
    } else {
      return primaryKey as string[];
    }
  } else {
    return [primaryKey];
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
        ...(
          filterKnownFields(transformation, ["type", "fields"], transformation.type, streamName).fields as Array<{
            value: string;
            path: string[];
          }>
        ).map((field) => {
          if (field.value === undefined) {
            throw new ManifestCompatibilityError(streamName, `'value' must be set on ${transformation.type}.fields`);
          }
          if (field.path === undefined) {
            throw new ManifestCompatibilityError(streamName, `'path' must be set on ${transformation.type}.fields`);
          }
          return {
            type: "add" as const,
            value: field.value,
            path: field.path,
          };
        })
      );
    }
    if (transformation.type === "RemoveFields") {
      builderTransformations.push(
        ...filterKnownFields(
          transformation,
          ["type", "field_pointers"],
          transformation.type,
          streamName
        ).field_pointers.map((path) => ({ type: "remove" as const, path }))
      );
    }
  });

  if (builderTransformations.length === 0) {
    return undefined;
  }
  return builderTransformations;
}

function getFormat(
  manifestCursorDatetime: DatetimeBasedCursorStartDatetime | DatetimeBasedCursorEndDatetime,
  datetimeBasedCursor: Pick<DatetimeBasedCursor, "datetime_format">
) {
  if (isString(manifestCursorDatetime) || !manifestCursorDatetime.datetime_format) {
    return datetimeBasedCursor.datetime_format;
  }
  return manifestCursorDatetime.datetime_format;
}

function isFormatSupported(
  manifestCursorDatetime: DatetimeBasedCursorStartDatetime | DatetimeBasedCursorEndDatetime,
  datetimeBasedCursor: Pick<DatetimeBasedCursor, "datetime_format">
) {
  return getFormat(manifestCursorDatetime, datetimeBasedCursor) === INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT;
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
  const datetimeBasedCursor = filterKnownFields(
    manifestIncrementalSync,
    [
      "type",
      "cursor_datetime_formats",
      "cursor_field",
      "cursor_granularity",
      "datetime_format",
      "end_datetime",
      "end_time_option",
      "is_data_feed",
      "lookback_window",
      "partition_field_end",
      "partition_field_start",
      "start_datetime",
      "start_time_option",
      "step",
    ],
    manifestIncrementalSync.type,
    streamName
  );
  if (datetimeBasedCursor.start_time_option) {
    filterRequestOption(
      datetimeBasedCursor.start_time_option,
      `${datetimeBasedCursor.type}.start_time_option`,
      streamName
    );
  }
  if (datetimeBasedCursor.end_time_option) {
    filterRequestOption(datetimeBasedCursor.end_time_option, `${datetimeBasedCursor.type}.end_time_option`, streamName);
  }

  if (datetimeBasedCursor.partition_field_start || datetimeBasedCursor.partition_field_end) {
    throw new ManifestCompatibilityError(
      streamName,
      `${datetimeBasedCursor.type} partition_field_start and partition_field_end are not supported`
    );
  }

  const {
    cursor_datetime_formats,
    datetime_format,
    partition_field_end,
    partition_field_start,
    end_datetime,
    start_datetime,
    step,
    cursor_granularity,
    is_data_feed,
    type,
    ...regularFields
  } = datetimeBasedCursor;

  const manifestStartDatetime = isString(start_datetime)
    ? start_datetime
    : filterKnownFields(start_datetime, ["type", "datetime", "datetime_format"], start_datetime.type, streamName);

  const manifestEndDatetime = end_datetime
    ? isString(end_datetime)
      ? end_datetime
      : filterKnownFields(end_datetime, ["type", "datetime", "datetime_format"], end_datetime.type, streamName)
    : undefined;

  let builderStartDatetime: BuilderIncrementalSync["start_datetime"] = {
    type: "custom",
    value: isString(manifestStartDatetime) ? manifestStartDatetime : manifestStartDatetime.datetime,
    format: getFormat(manifestStartDatetime, datetimeBasedCursor),
  };
  let builderEndDatetime: BuilderIncrementalSync["end_datetime"] = {
    type: "custom",
    value: isString(manifestEndDatetime) ? manifestEndDatetime : manifestEndDatetime?.datetime ?? "",
    format: manifestEndDatetime ? getFormat(manifestEndDatetime, datetimeBasedCursor) : undefined,
  };

  const startDateSpecKey = tryExtractAndValidateIncrementalKey(
    ["start_datetime"],
    builderStartDatetime.value,
    spec,
    streamName
  );
  if (startDateSpecKey && isFormatSupported(manifestStartDatetime, datetimeBasedCursor)) {
    builderStartDatetime = { type: "user_input", value: interpolateConfigKey(startDateSpecKey) };
  }

  const endDateSpecKey = tryExtractAndValidateIncrementalKey(
    ["end_datetime"],
    builderEndDatetime.value,
    spec,
    streamName
  );
  if (manifestEndDatetime && endDateSpecKey && isFormatSupported(manifestEndDatetime, datetimeBasedCursor)) {
    builderEndDatetime = { type: "user_input", value: interpolateConfigKey(endDateSpecKey) };
  } else if (
    !manifestEndDatetime ||
    builderEndDatetime.value === `{{ now_utc().strftime('${INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT}') }}`
  ) {
    builderEndDatetime = { type: "now" };
  }

  return {
    ...regularFields,
    cursor_datetime_formats: cursor_datetime_formats ?? [datetime_format],
    datetime_format:
      cursor_datetime_formats && datetime_format === cursor_datetime_formats[0] ? undefined : datetime_format,
    end_datetime: builderEndDatetime,
    start_datetime: builderStartDatetime,
    slicer: step && cursor_granularity ? { step, cursor_granularity } : undefined,
    filter_mode: is_data_feed ? "no_filter" : manifestEndDatetime ? "range" : "start",
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
  strategy: DefaultPaginator["pagination_strategy"],
  streamName?: string
): BuilderPaginator["strategy"] {
  if (strategy.type === "OffsetIncrement") {
    return filterKnownFields(strategy, ["type", "inject_on_first_request", "page_size"], strategy.type, streamName);
  }

  if (strategy.type === "PageIncrement") {
    return filterKnownFields(
      strategy,
      ["type", "inject_on_first_request", "page_size", "start_from_page"],
      strategy.type,
      streamName
    );
  }

  if (strategy.type !== "CursorPagination") {
    throw new ManifestCompatibilityError(
      undefined,
      `paginator.pagination_strategy uses an unsupported type: ${strategy.type}`
    );
  }

  const cursorPagination = filterKnownFields(
    strategy,
    ["type", "cursor_value", "stop_condition", "page_size"],
    strategy.type,
    streamName
  );

  const { cursor_value, stop_condition, ...rest } = cursorPagination;

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
  const paginator = filterKnownFields(
    manifestPaginator,
    ["type", "pagination_strategy", "page_size_option", "page_token_option"],
    manifestPaginator.type,
    streamName
  );

  if (manifestPaginator.pagination_strategy.type === "CustomPaginationStrategy") {
    throw new ManifestCompatibilityError(streamName, "paginator.pagination_strategy uses a CustomPaginationStrategy");
  }

  const pageSizeOption = paginator.page_size_option
    ? filterRequestOption(paginator.page_size_option, `${paginator.type}.page_size_option`, streamName)
    : undefined;

  let pageTokenOption: RequestOptionOrPathInject | undefined = undefined;
  if (paginator.page_token_option?.type === "RequestPath") {
    filterKnownFields(paginator.page_token_option, ["type"], paginator.page_token_option.type, streamName);
    pageTokenOption = { inject_into: "path" };
  } else if (paginator.page_token_option?.type === "RequestOption") {
    const requestOption = filterRequestOption(
      paginator.page_token_option,
      `${paginator.type}.page_token_option`,
      streamName
    );
    pageTokenOption = {
      inject_into: requestOption.inject_into,
      field_name: requestOption.field_name,
    };
  }

  return {
    strategy: manifestPaginatorStrategyToBuilder(manifestPaginator.pagination_strategy, streamName),
    pageTokenOption,
    pageSizeOption,
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

export function manifestAuthenticatorToBuilder(
  authenticator: HttpRequesterAuthenticator | undefined,
  spec: Spec | undefined
): BuilderFormAuthenticator {
  if (authenticator === undefined) {
    return {
      type: NO_AUTH,
    };
  } else if (authenticator.type === undefined) {
    throw new ManifestCompatibilityError(undefined, "Authenticator has no type");
  } else if (!isSupportedAuthenticator(authenticator)) {
    throw new ManifestCompatibilityError(undefined, `Unsupported authenticator type: ${authenticator.type}`);
  }

  switch (authenticator.type) {
    case NO_AUTH: {
      const noAuth = filterKnownFields(authenticator, ["type"], authenticator.type);
      return {
        type: noAuth.type,
      };
    }

    case API_KEY_AUTHENTICATOR: {
      const apiKeyAuth = filterKnownFields(
        authenticator,
        ["type", "api_token", "inject_into", "header"],
        authenticator.type
      );
      if (authenticator.inject_into) {
        filterRequestOption(authenticator.inject_into, `${authenticator.type}.inject_into`);
      }

      return {
        ...apiKeyAuth,
        inject_into: apiKeyAuth.inject_into ?? {
          type: "RequestOption",
          field_name: apiKeyAuth.header || "",
          inject_into: "header",
        },
        api_token: interpolateConfigKey(extractAndValidateAuthKey(["api_token"], apiKeyAuth, spec)),
      };
    }

    case BEARER_AUTHENTICATOR: {
      const bearerAuth = filterKnownFields(authenticator, ["type", "api_token"], authenticator.type);

      return {
        ...bearerAuth,
        api_token: interpolateConfigKey(extractAndValidateAuthKey(["api_token"], bearerAuth, spec)),
      };
    }

    case BASIC_AUTHENTICATOR: {
      const basicAuth = filterKnownFields(authenticator, ["type", "username", "password"], authenticator.type);

      return {
        ...basicAuth,
        username: interpolateConfigKey(extractAndValidateAuthKey(["username"], basicAuth, spec)),
        password: interpolateConfigKey(extractAndValidateAuthKey(["password"], basicAuth, spec)),
      };
    }

    case OAUTH_AUTHENTICATOR: {
      const oauth = filterKnownFields(
        authenticator,
        [
          "type",
          "access_token_name",
          "client_id",
          "client_secret",
          "expires_in_name",
          "grant_type",
          "refresh_request_body",
          "refresh_token",
          "refresh_token_updater",
          "scopes",
          "token_expiry_date",
          "token_expiry_date_format",
          "token_refresh_endpoint",
        ],
        authenticator.type
      );

      if (Object.values(oauth.refresh_request_body ?? {}).filter((value) => typeof value !== "string").length > 0) {
        throw new ManifestCompatibilityError(
          undefined,
          "OAuthAuthenticator contains a refresh_request_body with non-string values"
        );
      }
      if (oauth.grant_type && oauth.grant_type !== "refresh_token" && oauth.grant_type !== "client_credentials") {
        throw new ManifestCompatibilityError(
          undefined,
          "OAuthAuthenticator sets custom grant_type, but it must be one of 'refresh_token' or 'client_credentials'"
        );
      }

      let builderAuthenticator: BuilderFormAuthenticator = {
        ...oauth,
        refresh_request_body: Object.entries(oauth.refresh_request_body ?? {}),
        grant_type: oauth.grant_type ?? "refresh_token",
        refresh_token_updater: undefined,
        client_id: interpolateConfigKey(extractAndValidateAuthKey(["client_id"], oauth, spec)),
        client_secret: interpolateConfigKey(extractAndValidateAuthKey(["client_secret"], oauth, spec)),
      };

      if (!oauth.grant_type || oauth.grant_type === "refresh_token") {
        const refreshTokenSpecKey = extractAndValidateAuthKey(["refresh_token"], oauth, spec);
        builderAuthenticator = {
          ...builderAuthenticator,
          refresh_token: interpolateConfigKey(refreshTokenSpecKey),
        };

        if (authenticator.refresh_token_updater) {
          const refreshTokenUpdater = filterKnownFields(
            authenticator.refresh_token_updater,
            [
              "access_token_config_path",
              "refresh_token_config_path",
              "refresh_token_name",
              "token_expiry_date_config_path",
            ],
            `${authenticator.type}.refresh_token_updater`
          );

          if (!isEqual(refreshTokenUpdater?.refresh_token_config_path, [refreshTokenSpecKey])) {
            throw new ManifestCompatibilityError(
              undefined,
              "OAuthAuthenticator.refresh_token_updater.refresh_token_config_path needs to match the config path used for refresh_token"
            );
          }
          const {
            access_token_config_path,
            token_expiry_date_config_path,
            refresh_token_config_path,
            ...refresh_token_updater
          } = refreshTokenUpdater;
          builderAuthenticator = {
            ...builderAuthenticator,
            refresh_token_updater: {
              ...refresh_token_updater,
              access_token: interpolateConfigKey(
                extractAndValidateAuthKey(["refresh_token_updater", "access_token_config_path"], oauth, spec)
              ),
              token_expiry_date: interpolateConfigKey(
                extractAndValidateAuthKey(["refresh_token_updater", "token_expiry_date_config_path"], oauth, spec)
              ),
            },
          };
        }
      }

      return builderAuthenticator;
    }

    case SESSION_TOKEN_AUTHENTICATOR: {
      const sessionTokenAuth = filterKnownFields(
        authenticator,
        ["type", "expiration_duration", "login_requester", "request_authentication", "session_token_path", "decoder"],
        authenticator.type
      );
      if (sessionTokenAuth.request_authentication.type === SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR) {
        filterKnownFields(
          sessionTokenAuth.request_authentication,
          ["type", "inject_into"],
          `${sessionTokenAuth.type}.request_authentication`
        );
        filterRequestOption(
          sessionTokenAuth.request_authentication.inject_into,
          `${sessionTokenAuth.type}.request_authentication.inject_into`
        );
      } else if (sessionTokenAuth.request_authentication.type === SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR) {
        filterKnownFields(
          sessionTokenAuth.request_authentication,
          ["type"],
          `${authenticator.type}.request_authentication`
        );
      } else {
        throw new ManifestCompatibilityError(
          undefined,
          `SessionTokenAuthenticator request_authentication must have one of the following types: ${SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR}, ${SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR}`
        );
      }

      const decoderType = sessionTokenAuth.decoder?.type;
      if (![undefined, JsonDecoderType.JsonDecoder, XmlDecoderType.XmlDecoder].includes(decoderType)) {
        throw new ManifestCompatibilityError(undefined, "SessionTokenAuthenticator decoder is not supported");
      }

      const manifestLoginRequester = filterKnownFields(
        sessionTokenAuth.login_requester,
        [
          "type",
          "authenticator",
          "url_base",
          "path",
          "http_method",
          "request_parameters",
          "request_headers",
          "request_body_data",
          "request_body_json",
          "error_handler",
        ],
        `${sessionTokenAuth.type}.login_requester`
      );
      if (
        manifestLoginRequester.authenticator &&
        manifestLoginRequester.authenticator?.type !== NO_AUTH &&
        manifestLoginRequester.authenticator?.type !== API_KEY_AUTHENTICATOR &&
        manifestLoginRequester.authenticator?.type !== BEARER_AUTHENTICATOR &&
        manifestLoginRequester.authenticator?.type !== BASIC_AUTHENTICATOR
      ) {
        throw new ManifestCompatibilityError(
          undefined,
          `SessionTokenAuthenticator.login_requester.authenticator must have one of the following types: ${NO_AUTH}, ${API_KEY_AUTHENTICATOR}, ${BEARER_AUTHENTICATOR}, ${BASIC_AUTHENTICATOR}`
        );
      }
      const builderLoginRequesterAuthenticator = manifestAuthenticatorToBuilder(
        manifestLoginRequester.authenticator,
        spec
      );

      return {
        ...sessionTokenAuth,
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
        decoder: decoderType ? (decoderType === XmlDecoderType.XmlDecoder ? "XML" : "JSON") : "JSON",
      };
    }
  }
}

function manifestSpecToBuilderInputs(
  manifestSpec: Spec | undefined,
  authenticator: BuilderFormValues["global"]["authenticator"],
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
  convertFn: (manifestValue: ManifestInput, streamName?: string, spec?: Spec) => BuilderOutput,
  component:
    | {
        name: YamlSupportedComponentName["stream"];
        streamName: string;
      }
    | {
        name: YamlSupportedComponentName["global"];
        streamName: undefined;
      },
  metadata?: BuilderMetadata,
  spec?: Spec
): BuilderOutput | YamlString {
  if (component.streamName && metadata?.yamlComponents?.streams?.[component.streamName]?.includes(component.name)) {
    return dump(manifestValue);
  } else if (component.streamName === undefined && metadata?.yamlComponents?.global?.includes(component.name)) {
    return dump(manifestValue);
  }

  try {
    return convertFn(manifestValue, component.streamName, spec);
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

function filterRequestOption(requestOption: RequestOption, componentName: string, streamName?: string) {
  return filterKnownFields(requestOption, ["type", "field_name", "inject_into"], componentName, streamName);
}

function filterKnownFields<T extends object, K extends keyof T>(
  component: T,
  knownFields: K[],
  componentName: string,
  streamName?: string
): Pick<T, K>;
function filterKnownFields<T extends object, K extends keyof T>(
  component: T | undefined,
  knownFields: K[],
  componentName: string | undefined,
  streamName?: string
): Pick<T, K> | undefined;
function filterKnownFields<T extends object, K extends keyof T>(
  component: T | undefined,
  knownFields: K[],
  componentName: string | undefined,
  streamName?: string
): Pick<T, K> | undefined {
  if (component === undefined) {
    return undefined;
  }

  const componentKeys = Object.keys(component) as K[];
  const unknownFields = componentKeys.filter((key) => !knownFields.includes(key));

  if (unknownFields.length > 0) {
    throw new ManifestCompatibilityError(
      streamName,
      `${componentName} contains fields unsupported by the UI: ${unknownFields.join(", ")}`
    );
  }

  return componentKeys.reduce(
    (result, key) => {
      result[key] = component[key];
      return result;
    },
    {} as Pick<T, K>
  );
}

function convertToNumber(value: string | number, fieldName: string, streamName?: string): number;
function convertToNumber(
  value: string | number | undefined,
  fieldName: string,
  streamName?: string
): number | undefined;
function convertToNumber(
  value: string | number | undefined,
  fieldName: string,
  streamName?: string
): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const numericValue = Number(value);
  if (isNaN(numericValue)) {
    throw new ManifestCompatibilityError(streamName, `${fieldName} must be a number; found '${value}'`);
  }
  return numericValue;
}
