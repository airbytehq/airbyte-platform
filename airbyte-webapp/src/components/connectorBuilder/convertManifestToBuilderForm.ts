import cloneDeep from "lodash/cloneDeep";
import isEqual from "lodash/isEqual";

import {
  authTypeToKeyToInferredInput,
  BuilderFormAuthenticator,
  BuilderFormValues,
  BuilderIncrementalSync,
  BuilderPaginator,
  BuilderStream,
  BuilderTransformation,
  DEFAULT_BUILDER_FORM_VALUES,
  DEFAULT_BUILDER_STREAM_VALUES,
  hasIncrementalSyncUserInput,
  INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  incrementalSyncInferredInputs,
  isInterpolatedConfigKey,
  RequestOptionOrPathInject,
} from "./types";
import { formatJson } from "./utils";
import { AirbyteJSONSchema } from "../../core/jsonSchema/types";
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
} from "../../core/request/ConnectorManifest";

export const convertToBuilderFormValuesSync = (resolvedManifest: ConnectorManifest, connectorName: string) => {
  const builderFormValues = cloneDeep(DEFAULT_BUILDER_FORM_VALUES);
  builderFormValues.global.connectorName = connectorName;
  builderFormValues.checkStreams = resolvedManifest.check.stream_names;

  const streams = resolvedManifest.streams;
  if (streams === undefined || streams.length === 0) {
    const { inputs, inferredInputOverrides } = manifestSpecAndAuthToBuilder(
      resolvedManifest.spec,
      undefined,
      undefined
    );
    builderFormValues.inputs = inputs;
    builderFormValues.inferredInputOverrides = inferredInputOverrides;

    return builderFormValues;
  }

  assertType<SimpleRetriever>(streams[0].retriever, "SimpleRetriever", streams[0].name);
  assertType<HttpRequester>(streams[0].retriever.requester, "HttpRequester", streams[0].name);
  builderFormValues.global.urlBase = streams[0].retriever.requester.url_base;

  const serializedStreamToIndex = Object.fromEntries(streams.map((stream, index) => [JSON.stringify(stream), index]));
  builderFormValues.streams = streams.map((stream, index) =>
    manifestStreamToBuilder(
      stream,
      index,
      serializedStreamToIndex,
      streams[0].retriever.requester.url_base,
      streams[0].retriever.requester.authenticator
    )
  );

  const { inputs, inferredInputOverrides, auth } = manifestSpecAndAuthToBuilder(
    resolvedManifest.spec,
    streams[0].retriever.requester.authenticator,
    builderFormValues.streams
  );
  builderFormValues.inputs = inputs;
  builderFormValues.inferredInputOverrides = inferredInputOverrides;
  builderFormValues.global.authenticator = auth;

  return builderFormValues;
};

const manifestStreamToBuilder = (
  stream: DeclarativeStream,
  index: number,
  serializedStreamToIndex: Record<string, number>,
  firstStreamUrlBase: string,
  firstStreamAuthenticator?: HttpRequesterAuthenticator
): BuilderStream => {
  assertType<SimpleRetriever>(stream.retriever, "SimpleRetriever", stream.name);
  const retriever = stream.retriever;

  assertType<HttpRequester>(retriever.requester, "HttpRequester", stream.name);
  const requester = retriever.requester;

  if (
    !firstStreamAuthenticator || firstStreamAuthenticator.type === "NoAuth"
      ? requester.authenticator && requester.authenticator.type !== "NoAuth"
      : !isEqual(retriever.requester.authenticator, firstStreamAuthenticator)
  ) {
    throw new ManifestCompatibilityError(stream.name, "authenticator does not match the first stream's");
  }

  if (retriever.requester.url_base !== firstStreamUrlBase) {
    throw new ManifestCompatibilityError(stream.name, "url_base does not match the first stream's");
  }

  if (![undefined, "GET", "POST"].includes(requester.http_method)) {
    throw new ManifestCompatibilityError(stream.name, "http_method is not GET or POST");
  }

  assertType<DpathExtractor>(retriever.record_selector.extractor, "DpathExtractor", stream.name);

  return {
    ...DEFAULT_BUILDER_STREAM_VALUES,
    id: index.toString(),
    name: stream.name ?? "",
    urlPath: requester.path,
    httpMethod: (requester.http_method as "GET" | "POST" | undefined) ?? "GET",
    fieldPointer: retriever.record_selector.extractor.field_path as string[],
    requestOptions: {
      requestParameters: Object.entries(requester.request_parameters ?? {}),
      requestHeaders: Object.entries(requester.request_headers ?? {}),
      // try getting this from request_body_data first, and if not set then pull from request_body_json
      requestBody: Object.entries(requester.request_body_data ?? requester.request_body_json ?? {}),
    },
    primaryKey: manifestPrimaryKeyToBuilder(stream),
    paginator: manifestPaginatorToBuilder(retriever.paginator, stream.name),
    incrementalSync: manifestIncrementalSyncToBuilder(stream.incremental_sync, stream.name),
    partitionRouter: manifestPartitionRouterToBuilder(retriever.partition_router, serializedStreamToIndex, stream.name),
    schema: manifestSchemaLoaderToBuilderSchema(stream.schema_loader),
    errorHandler: manifestErrorHandlerToBuilder(stream.name, requester),
    transformations: manifestTransformationsToBuilder(stream.name, stream.transformations),
    unsupportedFields: {
      retriever: {
        record_selector: {
          record_filter: stream.retriever.record_selector.record_filter,
        },
      },
    },
  };
};

function manifestPartitionRouterToBuilder(
  partitionRouter: SimpleRetrieverPartitionRouter | SimpleRetrieverPartitionRouterAnyOfItem | undefined,
  serializedStreamToIndex: Record<string, number>,
  streamName?: string
): BuilderStream["partitionRouter"] {
  if (partitionRouter === undefined) {
    return undefined;
  }

  if (Array.isArray(partitionRouter)) {
    return partitionRouter.flatMap(
      (subRouter) => manifestPartitionRouterToBuilder(subRouter, serializedStreamToIndex, streamName) || []
    );
  }

  if (partitionRouter.type === undefined) {
    throw new ManifestCompatibilityError(streamName, "partition_router has no type");
  }

  if (partitionRouter.type === "CustomPartitionRouter") {
    throw new ManifestCompatibilityError(streamName, "partition_router contains a CustomPartitionRouter");
  }

  if (partitionRouter.type === "ListPartitionRouter") {
    return [
      {
        ...partitionRouter,
        values:
          typeof partitionRouter.values === "string"
            ? {
                value: partitionRouter.values,
                type: "variable",
              }
            : {
                value: partitionRouter.values,
                type: "list",
              },
      },
    ];
  }

  if (partitionRouter.type === "SubstreamPartitionRouter") {
    const manifestSubstreamPartitionRouter = partitionRouter;

    if (manifestSubstreamPartitionRouter.parent_stream_configs.length > 1) {
      throw new ManifestCompatibilityError(streamName, "SubstreamPartitionRouter has more than one parent stream");
    }
    const parentStreamConfig = manifestSubstreamPartitionRouter.parent_stream_configs[0];

    const matchingStreamIndex = serializedStreamToIndex[JSON.stringify(parentStreamConfig.stream)];
    if (matchingStreamIndex === undefined) {
      throw new ManifestCompatibilityError(
        streamName,
        "SubstreamPartitionRouter's parent stream doesn't match any other stream"
      );
    }

    return [
      {
        type: "SubstreamPartitionRouter",
        parent_key: parentStreamConfig.parent_key,
        partition_field: parentStreamConfig.partition_field,
        parentStreamReference: matchingStreamIndex.toString(),
      },
    ];
  }

  throw new ManifestCompatibilityError(streamName, "partition_router type is unsupported");
}

function manifestErrorHandlerToBuilder(
  streamName: string | undefined,
  requester: HttpRequester
): BuilderStream["errorHandler"] {
  if (!requester.error_handler) {
    return undefined;
  }
  const handlers =
    requester.error_handler.type === "CompositeErrorHandler"
      ? requester.error_handler.error_handlers
      : [requester.error_handler];
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

  return handlers as DefaultErrorHandler[];
}

function manifestPrimaryKeyToBuilder(manifestStream: DeclarativeStream): BuilderStream["primaryKey"] {
  if (!isEqual(manifestStream.primary_key, manifestStream.primary_key)) {
    throw new ManifestCompatibilityError(
      manifestStream.name,
      "primary_key is not consistent across stream and retriever levels"
    );
  }
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
    partition_field_end,
    partition_field_start,
    end_datetime: manifestEndDateTime,
    start_datetime: manifestStartDateTime,
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
    value: typeof manifestEndDateTime === "string" ? manifestEndDateTime : manifestEndDateTime.datetime,
    format: getFormat(manifestEndDateTime, manifestIncrementalSync),
  };

  if (
    start_datetime.value === "{{ config['start_date'] }}" &&
    isFormatSupported(manifestStartDateTime, manifestIncrementalSync)
  ) {
    start_datetime = { type: "user_input" };
  }

  if (
    end_datetime.value === "{{ config['end_date'] }}" &&
    isFormatSupported(manifestEndDateTime, manifestIncrementalSync)
  ) {
    end_datetime = { type: "user_input" };
  } else if (end_datetime.value === `{{ now_utc().strftime('${INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT}') }}`) {
    end_datetime = { type: "now" };
  }

  return {
    ...regularFields,
    end_datetime,
    start_datetime,
  };
}

function manifestPaginatorToBuilder(
  manifestPaginator: SimpleRetrieverPaginator | undefined,
  streamName: string | undefined
): BuilderPaginator | undefined {
  if (manifestPaginator === undefined || manifestPaginator.type === "NoPagination") {
    return undefined;
  }

  if (manifestPaginator.page_token_option === undefined) {
    throw new ManifestCompatibilityError(streamName, "paginator does not define a page_token_option");
  }

  if (manifestPaginator.pagination_strategy.type === "CustomPaginationStrategy") {
    throw new ManifestCompatibilityError(streamName, "paginator.pagination_strategy uses a CustomPaginationStrategy");
  }

  let pageTokenOption: RequestOptionOrPathInject | undefined = undefined;

  if (manifestPaginator.page_token_option.type === "RequestPath") {
    pageTokenOption = { inject_into: "path" };
  } else {
    pageTokenOption = {
      inject_into: manifestPaginator.page_token_option.inject_into,
      field_name: manifestPaginator.page_token_option.field_name,
    };
  }

  return {
    strategy: manifestPaginator.pagination_strategy,
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
  } else if (manifestAuthenticator.type === "SessionTokenAuthenticator") {
    throw new ManifestCompatibilityError(streamName, "uses a SessionTokenAuthenticator");
  } else if (manifestAuthenticator.type === "SingleUseRefreshTokenOAuthAuthenticator") {
    throw new ManifestCompatibilityError(streamName, "uses a SingleUseRefreshTokenOAuthAuthenticator");
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

    if (manifestAuthenticator.grant_type && manifestAuthenticator.grant_type !== "refresh_token") {
      throw new ManifestCompatibilityError(streamName, "OAuthAuthenticator sets custom grant_type");
    }

    builderAuthenticator = {
      ...manifestAuthenticator,
      refresh_request_body: Object.entries(manifestAuthenticator.refresh_request_body ?? {}),
    };
  } else {
    builderAuthenticator = manifestAuthenticator;
  }

  // verify that all auth keys which require a user input have a {{config[]}} value

  const userInputAuthKeys = Object.keys(authTypeToKeyToInferredInput[builderAuthenticator.type]);

  for (const userInputAuthKey of userInputAuthKeys) {
    if (!isInterpolatedConfigKey(Reflect.get(builderAuthenticator, userInputAuthKey))) {
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
  } = {
    inputs: [],
    inferredInputOverrides: {},
    auth: manifestAuthenticatorToBuilder(manifestAuthenticator),
  };

  if (manifestSpec === undefined) {
    return result;
  }

  const required = manifestSpec.connection_specification.required as string[];

  Object.entries(manifestSpec.connection_specification.properties as Record<string, AirbyteJSONSchema>).forEach(
    ([specKey, specDefinition]) => {
      const matchingInferredInput = getMatchingInferredInput(result.auth.type, streams, specKey);
      if (matchingInferredInput) {
        result.inferredInputOverrides[matchingInferredInput.key] = specDefinition;
      } else {
        result.inputs.push({
          key: specKey,
          definition: specDefinition,
          required: required.includes(specKey),
        });
      }
    }
  );

  return result;
}

function getMatchingInferredInput(
  authType: BuilderFormAuthenticator["type"],
  streams: BuilderStream[] | undefined,
  specKey: string
) {
  if (streams && specKey === "start_date" && hasIncrementalSyncUserInput(streams, "start_datetime")) {
    return incrementalSyncInferredInputs.start_date;
  }
  if (streams && specKey === "end_date" && hasIncrementalSyncUserInput(streams, "end_datetime")) {
    return incrementalSyncInferredInputs.end_date;
  }
  return Object.values(authTypeToKeyToInferredInput[authType]).find((input) => input.key === specKey);
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

  constructor(public streamName: string | undefined, public message: string) {
    const errorMessage = `${streamName ? `Stream ${streamName}: ` : ""}${message}`;
    super(errorMessage);
    this.message = errorMessage;
  }
}

export function isManifestCompatibilityError(error: { __type?: string }): error is ManifestCompatibilityError {
  return error.__type === "connectorBuilder.manifestCompatibility";
}
