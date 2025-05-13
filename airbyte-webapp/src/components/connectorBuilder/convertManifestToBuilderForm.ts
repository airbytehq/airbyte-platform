import { dump } from "js-yaml";
import cloneDeep from "lodash/cloneDeep";
import get from "lodash/get";
import isArray from "lodash/isArray";
import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import isObject from "lodash/isObject";
import isString from "lodash/isString";
import omit from "lodash/omit";
import pick from "lodash/pick";
import set from "lodash/set";
import { match } from "ts-pattern";

import { formatGraphqlQuery } from "components/ui/CodeEditor/GraphqlFormatter";

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
  OAuthConfigSpecificationOauthConnectorInputSpecification,
  CheckDynamicStreamType,
  JwtAuthenticator,
  RequestOptionInjectInto,
  CsvDecoderType,
  ZipfileDecoderType,
  GzipDecoderType,
  StateDelegatingStreamType,
  ParentStreamConfigStream,
  SimpleRetrieverType,
  AsyncRetriever,
  AsyncRetrieverType,
  DpathExtractor,
  ResponseToFileExtractorType,
  CustomRecordExtractorType,
  HttpComponentsResolverType,
  DynamicDeclarativeStreamComponentsResolver,
  SimpleRetrieverRequester,
  HttpRequesterRequestParameters,
  HttpComponentsResolver,
} from "core/api/types/ConnectorManifest";

import {
  BuilderDecoderConfig,
  API_KEY_AUTHENTICATOR,
  BASIC_AUTHENTICATOR,
  BEARER_AUTHENTICATOR,
  BuilderErrorHandler,
  BuilderFormAuthenticator,
  BuilderFormOAuthAuthenticator,
  BuilderFormDeclarativeOAuthAuthenticator,
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
  DeclarativeOAuthAuthenticatorType,
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
  JWT_AUTHENTICATOR,
  BuilderNestedDecoderConfig,
  BuilderDpathExtractor,
  BuilderDynamicStream,
  BuilderComponentsResolver,
  BuilderRequestOptions,
  declarativeStreamIsGenerated,
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
  if (resolvedManifest.check.type === CheckDynamicStreamType.CheckDynamicStream) {
    throw new ManifestCompatibilityError(undefined, `${CheckDynamicStreamType.CheckDynamicStream} is not supported`);
  }
  builderFormValues.checkStreams = resolvedManifest.check.stream_names ?? [];
  builderFormValues.dynamicStreamCheckConfigs = resolvedManifest.check.dynamic_streams_check_configs ?? [];
  builderFormValues.description = resolvedManifest.description;

  const streams = resolvedManifest.streams ?? [];
  const dynamicStreams = resolvedManifest.dynamic_streams ?? [];

  if (streams.length === 0 && dynamicStreams.length === 0) {
    builderFormValues.inputs = manifestSpecToBuilderInputs(resolvedManifest.spec, { type: NO_AUTH }, []);
    return builderFormValues;
  }

  if (streams.some((stream) => stream.type === StateDelegatingStreamType.StateDelegatingStream)) {
    throw new ManifestCompatibilityError(
      undefined,
      `${StateDelegatingStreamType.StateDelegatingStream} is not supported`
    );
  }
  const declarativeStreams = streams as DeclarativeStream[];

  const streamRetrievers = declarativeStreams.map((stream) => stream.retriever);
  const dynamicStreamSimpleRetrievers = dynamicStreams
    .filter(
      (dynamicStream) =>
        dynamicStream.components_resolver.type === HttpComponentsResolverType.HttpComponentsResolver &&
        dynamicStream.components_resolver.retriever.type === SimpleRetrieverType.SimpleRetriever
    )
    .map((dynamicStream) => (dynamicStream.components_resolver as HttpComponentsResolver).retriever);

  const firstSimpleRetriever: SimpleRetriever | undefined = [
    ...streamRetrievers,
    ...dynamicStreamSimpleRetrievers,
  ].find((retriever): retriever is SimpleRetriever => retriever.type === "SimpleRetriever");

  if (firstSimpleRetriever) {
    assertType<HttpRequester>(firstSimpleRetriever.requester, "HttpRequester", "stream");
    builderFormValues.global.urlBase = firstSimpleRetriever.requester.url_base;
  } else if (declarativeStreams.length > 0) {
    const firstStream = declarativeStreams[0];
    assertType<AsyncRetriever>(firstStream.retriever, "AsyncRetriever", "stream");
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    builderFormValues.global.urlBase = firstStream.retriever.creation_requester.url_base;
  } else if (dynamicStreams.length > 0) {
    const firstDynamicStream = dynamicStreams[0];
    assertType<SimpleRetriever>(
      firstDynamicStream.stream_template.retriever,
      "SimpleRetriever",
      "dynamic_stream",
      firstDynamicStream.name
    );
    assertType<HttpRequester>(
      firstDynamicStream.stream_template.retriever.requester,
      "HttpRequester",
      "dynamic_stream",
      firstDynamicStream.name
    );
    builderFormValues.global.urlBase = firstDynamicStream.stream_template.retriever.requester.url_base;
  }

  // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
  const builderMetadata = resolvedManifest.metadata ? (resolvedManifest.metadata as BuilderMetadata) : undefined;

  const getStreamName = (stream: DeclarativeStream, index: number) => streamNameOrDefault(stream.name, index);

  const streamNameToIndex = declarativeStreams.reduce((acc, stream, index) => {
    const streamName = getStreamName(stream, index);
    return { ...acc, [streamName]: index.toString() };
  }, {});

  const serializedStreamToName = Object.fromEntries(
    declarativeStreams.map((stream, index) => [formatJson(stream, true), getStreamName(stream, index)])
  );
  builderFormValues.streams = declarativeStreams.map((stream, index) => {
    const streamName = getStreamName(stream, index);
    const streamId = index.toString();
    if (stream.retriever.type === SimpleRetrieverType.SimpleRetriever) {
      return manifestSyncStreamToBuilder(
        stream,
        streamName,
        streamId,
        streamNameToIndex,
        serializedStreamToName,
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        firstSimpleRetriever?.requester?.url_base,
        firstSimpleRetriever?.requester?.authenticator,
        builderMetadata,
        resolvedManifest.spec
      );
    } else if (stream.retriever.type === "AsyncRetriever") {
      return manifestAsyncStreamToBuilder(
        stream,
        streamName,
        streamId,
        streamNameToIndex,
        serializedStreamToName,
        builderMetadata,
        resolvedManifest.spec
      );
    }
    throw new ManifestCompatibilityError(
      streamName,
      `Only ${SimpleRetrieverType.SimpleRetriever} and ${AsyncRetrieverType.AsyncRetriever} are supported`
    );
  });

  function assertResolverWithSimpleRetriever(
    resolver: DynamicDeclarativeStreamComponentsResolver
  ): resolver is DynamicDeclarativeStreamComponentsResolver & { retriever: SimpleRetriever } {
    return (
      resolver.type === HttpComponentsResolverType.HttpComponentsResolver &&
      resolver.retriever.type === SimpleRetrieverType.SimpleRetriever
    );
  }

  builderFormValues.dynamicStreams = dynamicStreams.map((dynamicStream, idx): BuilderDynamicStream => {
    if (!assertResolverWithSimpleRetriever(dynamicStream.components_resolver)) {
      throw new ManifestCompatibilityError(
        undefined,
        `Dynamic stream ${dynamicStream.name}: Only ${HttpComponentsResolverType.HttpComponentsResolver} with ${SimpleRetrieverType.SimpleRetriever} is supported`
      );
    }

    const componentsResolver = structuredClone(dynamicStream.components_resolver as BuilderComponentsResolver);
    componentsResolver.retriever.requester = {
      $ref: "#/definitions/base_requester",
      path: dynamicStream.components_resolver.retriever.requester.path,
    } as unknown as SimpleRetrieverRequester;

    // if there isn't a record filter, add a default one so the form controls pathing works
    if (!componentsResolver.retriever.record_selector.record_filter) {
      componentsResolver.retriever.record_selector.record_filter = {
        type: "RecordFilter",
        condition: "",
      };
    }

    // write any component mappings to the stream template
    const streamTemplateWithMappings = structuredClone(dynamicStream.stream_template);

    dynamicStream.components_resolver.components_mapping.forEach(({ field_path, value }) => {
      set(streamTemplateWithMappings, field_path, value);
    });

    const dynamicStreamName = dynamicStream.name ?? `dynamic_stream_${idx}`;
    return {
      dynamicStreamName,
      componentsResolver,
      streamTemplate: {
        ...manifestSyncStreamToBuilder(
          streamTemplateWithMappings,
          streamTemplateWithMappings.name ?? "",
          "",
          streamNameToIndex,
          serializedStreamToName,
          // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
          firstSimpleRetriever?.requester?.url_base,
          firstSimpleRetriever?.requester?.authenticator,
          builderMetadata,
          resolvedManifest.spec
        ),
        autoImportSchema: builderMetadata?.autoImportSchema?.[dynamicStreamName] === true,
      },
    };
  });

  builderFormValues.assist = builderMetadata?.assist ?? {};

  builderFormValues.global.authenticator = firstSimpleRetriever
    ? convertOrDumpAsString(
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        firstSimpleRetriever.requester.authenticator,
        manifestAuthenticatorToBuilder,
        {
          name: "authenticator",
          streamName: undefined,
        },
        resolvedManifest.spec
      )
    : { type: NO_AUTH };

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
  "client_id_name",
  "client_secret_name",
  "grant_type_name",
  "profile_assertion",
  "refresh_request_headers",
  "refresh_token_name",
  "use_profile_assertion",
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

const manifestSyncStreamToBuilder = (
  stream: DeclarativeStream,
  streamName: string,
  streamId: string,
  streamNameToId: Record<string, string>,
  serializedStreamToName: Record<string, string>,
  firstSimpleRetrieverUrlBase: string,
  firstSimpleRetrieverAuthenticator?: HttpRequesterAuthenticator,
  metadata?: BuilderMetadata,
  spec?: Spec
): BuilderStream => {
  const {
    type,
    incremental_sync,
    name,
    dynamic_stream_name,
    primary_key,
    retriever,
    schema_loader,
    transformations,
    ...unknownStreamFields
  } = stream;
  assertType<SimpleRetriever>(retriever, "SimpleRetriever", "stream", streamName);

  const {
    type: retrieverType,
    paginator,
    partition_router,
    record_selector,
    requester,
    decoder,
    ...unknownRetrieverFields
  } = retriever;
  assertType<HttpRequester>(requester, "HttpRequester", "stream", streamName);

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
  const cleanedFirstStreamAuthenticator = pick(firstSimpleRetrieverAuthenticator, authenticatorKeysToCheck);

  if (url_base !== firstSimpleRetrieverUrlBase) {
    throw new ManifestCompatibilityError(streamName, "url_base does not match the first stream's");
  }

  if (
    !firstSimpleRetrieverAuthenticator || firstSimpleRetrieverAuthenticator.type === "NoAuth"
      ? authenticator && authenticator.type !== "NoAuth"
      : !isEqual(cleanedAuthenticator, cleanedFirstStreamAuthenticator)
  ) {
    throw new ManifestCompatibilityError(streamName, "authenticator does not match the first stream's");
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
    requestType: "sync" as const,
    id: streamId,
    name: streamName,
    dynamicStreamName: declarativeStreamIsGenerated(stream) ? stream.dynamic_stream_name : undefined,
    urlPath: path,
    httpMethod: http_method === "POST" ? "POST" : "GET",
    decoder: manifestDecoderToBuilder(decoder, streamName),
    requestOptions: {
      requestParameters: manifestRequestParametersToBuilder(request_parameters, streamName),
      requestHeaders: Object.entries(request_headers ?? {}),
      requestBody: requesterToRequestBody(requester),
    },
    primaryKey: manifestPrimaryKeyToBuilder(primary_key, streamName),
    recordSelector: convertOrDumpAsString(record_selector, manifestRecordSelectorToBuilder, {
      name: "recordSelector",
      streamName,
    }),
    paginator: convertOrDumpAsString(paginator, manifestPaginatorToBuilder, {
      name: "paginator",
      streamName,
    }),
    incrementalSync: convertOrDumpAsString(
      incremental_sync,
      manifestIncrementalSyncToBuilder,
      {
        name: "incrementalSync",
        streamName,
      },
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
      }
    ),
    parameterizedRequests: convertOrDumpAsString(
      filterPartitionRouterToType(partition_router, ["ListPartitionRouter"]),
      manifestListPartitionRouterToBuilder,
      {
        name: "parameterizedRequests",
        streamName,
      }
    ),
    schema: manifestSchemaLoaderToBuilderSchema(schema_loader, streamName),
    errorHandler: convertOrDumpAsString(error_handler, manifestErrorHandlerToBuilder, {
      name: "errorHandler",
      streamName,
    }),
    transformations: convertOrDumpAsString(transformations, manifestTransformationsToBuilder, {
      name: "transformations",
      streamName,
    }),
    autoImportSchema: metadata?.autoImportSchema?.[streamName] === true,
    unknownFields: dumpUnknownFields(unknownStreamFields, unknownRetrieverFields, unknownRequesterFields),
    testResults: metadata?.testedStreams?.[streamName],
  };
};

const dumpUnknownFields = (
  unknownStreamFields: Partial<DeclarativeStream>,
  unknownRetrieverFields: Partial<SimpleRetriever> | Partial<AsyncRetriever>,
  unknownRequesterFields: Partial<HttpRequester>
) => {
  return !isEmpty(unknownStreamFields) || !isEmpty(unknownRetrieverFields) || !isEmpty(unknownRequesterFields)
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
    : undefined;
};

const manifestAsyncStreamToBuilder = (
  stream: DeclarativeStream,
  streamName: string,
  streamId: string,
  streamNameToId: Record<string, string>,
  serializedStreamToName: Record<string, string>,
  metadata?: BuilderMetadata,
  spec?: Spec
): BuilderStream => {
  const { retriever, schema_loader, incremental_sync, primary_key, transformations, ...unknownStreamFields } = stream;
  assertType<AsyncRetriever>(retriever, "AsyncRetriever", "stream", streamName);

  const {
    creation_requester,
    polling_requester,
    download_requester,
    partition_router,
    decoder,
    download_decoder,
    record_selector,
    download_paginator,
    download_extractor,
    status_extractor,
    status_mapping,
    download_target_extractor,
    polling_job_timeout,
    ...unknownRetrieverFields
  } = retriever;
  assertType<HttpRequester>(creation_requester, "HttpRequester", "stream", streamName);
  assertType<HttpRequester>(polling_requester, "HttpRequester", "stream", streamName);
  assertType<HttpRequester>(download_requester, "HttpRequester", "stream", streamName);

  if (download_extractor?.type === CustomRecordExtractorType.CustomRecordExtractor) {
    throw new ManifestCompatibilityError(streamName, "CustomRecordExtractor is not supported on download_extractor");
  }
  if (download_extractor?.type === ResponseToFileExtractorType.ResponseToFileExtractor) {
    throw new ManifestCompatibilityError(
      streamName,
      "ResponseToFileExtractorType is not supported on download_extractor"
    );
  }
  if (status_extractor?.type === CustomRecordExtractorType.CustomRecordExtractor) {
    throw new ManifestCompatibilityError(streamName, "CustomRecordExtractor is not supported on status_extractor");
  }
  if (download_target_extractor?.type === CustomRecordExtractorType.CustomRecordExtractor) {
    throw new ManifestCompatibilityError(
      streamName,
      "CustomRecordExtractor is not supported on download_target_extractor"
    );
  }

  const substreamPartitionRouterToBuilder = (
    partitionRouter: SimpleRetrieverPartitionRouter | undefined,
    streamName?: string
  ) => manifestSubstreamPartitionRouterToBuilder(partitionRouter, streamNameToId, streamName);

  return {
    requestType: "async" as const,
    id: streamId,
    name: streamName,
    schema: manifestSchemaLoaderToBuilderSchema(schema_loader, streamName),
    autoImportSchema: metadata?.autoImportSchema?.[streamName] === true,
    unknownFields: dumpUnknownFields(unknownStreamFields, unknownRetrieverFields, {}),
    testResults: metadata?.testedStreams?.[streamName],
    creationRequester: {
      ...manifestAsyncHttpRequesterToBuilder(creation_requester, streamName, spec),
      incrementalSync: convertOrDumpAsString(
        incremental_sync,
        manifestIncrementalSyncToBuilder,
        {
          name: "incrementalSync",
          streamName,
        },
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
        }
      ),
      parameterizedRequests: convertOrDumpAsString(
        filterPartitionRouterToType(partition_router, ["ListPartitionRouter"]),
        manifestListPartitionRouterToBuilder,
        {
          name: "parameterizedRequests",
          streamName,
        }
      ),
      decoder: manifestDecoderToBuilder(decoder, streamName),
    },
    pollingRequester: {
      ...manifestAsyncHttpRequesterToBuilder(polling_requester, streamName, spec),
      statusMapping: status_mapping,
      statusExtractor: manifestDpathExtractorToBuilder(status_extractor),
      downloadTargetExtractor: manifestDpathExtractorToBuilder(download_target_extractor),
      pollingTimeout: isString(polling_job_timeout)
        ? { type: "custom", value: polling_job_timeout }
        : { type: "number", value: polling_job_timeout ?? 15 },
    },
    downloadRequester: {
      ...manifestAsyncHttpRequesterToBuilder(download_requester, streamName, spec),
      decoder: manifestDecoderToBuilder(download_decoder, streamName),
      primaryKey: manifestPrimaryKeyToBuilder(primary_key, streamName),
      transformations: convertOrDumpAsString(transformations, manifestTransformationsToBuilder, {
        name: "transformations",
        streamName,
      }),
      recordSelector: convertOrDumpAsString(record_selector, manifestRecordSelectorToBuilder, {
        name: "recordSelector",
        streamName,
      }),
      paginator: convertOrDumpAsString(download_paginator, manifestPaginatorToBuilder, {
        name: "paginator",
        streamName,
      }),
      downloadExtractor: download_extractor ? manifestDpathExtractorToBuilder(download_extractor) : undefined,
    },
  };
};

const manifestDpathExtractorToBuilder = (extractor: DpathExtractor): BuilderDpathExtractor => {
  return {
    ...extractor,
    field_path: extractor.field_path.map((field) => String(field)),
  };
};

const manifestAsyncHttpRequesterToBuilder = (requester: HttpRequester, streamName: string, spec?: Spec) => {
  return {
    url: extractFullUrl(requester),
    httpMethod: requester.http_method === "POST" ? ("POST" as const) : ("GET" as const),
    requestOptions: {
      requestParameters: manifestRequestParametersToBuilder(requester.request_parameters, streamName),
      requestHeaders: Object.entries(requester.request_headers ?? {}),
      requestBody: requesterToRequestBody(requester),
    },
    errorHandler: convertOrDumpAsString(requester.error_handler, manifestErrorHandlerToBuilder, {
      name: "errorHandler",
      streamName,
    }),
    authenticator: convertOrDumpAsString(
      requester.authenticator,
      manifestAuthenticatorToBuilder,
      {
        name: "authenticator",
        streamName,
      },
      spec
    ),
  };
};

function extractFullUrl(requester: HttpRequester): string {
  if (requester.url) {
    return requester.url;
  }
  if (requester.url_base && requester.path) {
    return `${removeTrailingSlashes(requester.url_base)}/${removeLeadingSlashes(requester.path)}`;
  }
  return requester.url_base ?? requester.path ?? "";
}

function manifestRequestParametersToBuilder(
  requestParameters: HttpRequesterRequestParameters | undefined,
  streamName?: string
): BuilderRequestOptions["requestParameters"] {
  if (!requestParameters) {
    return [];
  }
  if (isString(requestParameters)) {
    throw new ManifestCompatibilityError(streamName, "request_parameters must be an object");
  }
  if (!Object.values(requestParameters).every((value) => isString(value))) {
    throw new ManifestCompatibilityError(streamName, "all request_parameters values must be strings");
  }
  return Object.entries(requestParameters as Record<string, string>);
}

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
    Object.keys(requester.request_body_json).length === 1 &&
    "query" in requester.request_body_json
  ) {
    try {
      // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
      const formattedQuery = formatGraphqlQuery(requester.request_body_json.query);
      return {
        type: "graphql",
        value: formattedQuery,
      };
    } catch {
      return {
        type: "graphql",
        value: requester.request_body_json.query as string,
      };
    }
  }

  if (
    isObject(requester.request_body_json) &&
    Object.values(requester.request_body_json).every((value) => isString(value))
  ) {
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    return { type: "json_list", values: Object.entries(requester.request_body_json) };
  }
  return {
    type: "json_freeform",
    value: formatJson(requester.request_body_json),
  };
}

const manifestDecoderToBuilder = (
  decoder: SimpleRetrieverDecoder | undefined,
  streamName: string
): BuilderDecoderConfig => {
  const supportedDecoderTypes: Array<string | undefined> = [
    undefined,
    JsonDecoderType.JsonDecoder,
    JsonlDecoderType.JsonlDecoder,
    XmlDecoderType.XmlDecoder,
    IterableDecoderType.IterableDecoder,
    CsvDecoderType.CsvDecoder,
    GzipDecoderType.GzipDecoder,
    ZipfileDecoderType.ZipfileDecoder,
  ];
  const decoderType = decoder?.type;
  if (!supportedDecoderTypes.includes(decoderType)) {
    throw new ManifestCompatibilityError(streamName, "decoder is not supported");
  }

  switch (decoderType) {
    case JsonDecoderType.JsonDecoder:
      return { type: "JSON" };
    case XmlDecoderType.XmlDecoder:
      return { type: "XML" };
    case JsonlDecoderType.JsonlDecoder:
      return { type: "JSON Lines" };
    case IterableDecoderType.IterableDecoder:
      return { type: "Iterable" };
    case CsvDecoderType.CsvDecoder:
      return { type: "CSV", delimiter: decoder?.delimiter, encoding: decoder?.encoding };
    case GzipDecoderType.GzipDecoder:
      return {
        type: "gzip",
        decoder: manifestDecoderToBuilder(decoder?.decoder, streamName) as BuilderNestedDecoderConfig,
      };
    case ZipfileDecoderType.ZipfileDecoder:
      return {
        type: "ZIP file",
        decoder: manifestDecoderToBuilder(decoder?.decoder, streamName) as BuilderNestedDecoderConfig,
      };
    default:
      return { type: "JSON" };
  }
};

export function manifestRecordSelectorToBuilder(
  recordSelector: RecordSelector,
  streamName?: string
): BuilderRecordSelector | undefined {
  assertType(recordSelector, "RecordSelector", "stream", streamName);
  const selector = filterKnownFields(
    recordSelector,
    ["type", "extractor", "record_filter", "schema_normalization"],
    recordSelector.type,
    streamName
  );

  assertType(selector.extractor, "DpathExtractor", "stream", streamName);
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
    assertType(filter, "RecordFilter", "stream", streamName);
  }

  if (
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    extractor.field_path.length === 0 &&
    !filter &&
    (!selector.schema_normalization || selector.schema_normalization === "None")
  ) {
    return undefined;
  }

  return {
    fieldPath: extractor.field_path as string[],
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
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
    listPartitionRouter.request_option = validateAndConvertRequestOption(
      listPartitionRouter.request_option,
      `${partitionRouter.type}.request_option`,
      streamName
    );
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
    parentStreamConfig.request_option = validateAndConvertRequestOption(
      parentStreamConfig.request_option,
      `${partitionRouter.type}.parent_stream_configs.request_option`,
      streamName
    );
  }

  function hasRefField(stream: ParentStreamConfigStream): stream is ParentStreamConfigStream & { $ref: string } {
    return "$ref" in stream;
  }

  if (!hasRefField(parentStreamConfig.stream)) {
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

function manifestPrimaryKeyToBuilder(primaryKey: PrimaryKey | undefined, streamName?: string): string[] {
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
): BuilderIncrementalSync | undefined {
  if (!manifestIncrementalSync) {
    return undefined;
  }
  if (manifestIncrementalSync.type === "CustomIncrementalSync") {
    throw new ManifestCompatibilityError(streamName, "incremental sync uses a custom implementation");
  }
  if (manifestIncrementalSync.type === "IncrementingCountCursor") {
    throw new ManifestCompatibilityError(
      streamName,
      "incremental sync uses an IncrementingCountCursor, which is unsupported"
    );
  }
  assertType(manifestIncrementalSync, "DatetimeBasedCursor", "stream", streamName);
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
    datetimeBasedCursor.start_time_option = validateAndConvertRequestOption(
      datetimeBasedCursor.start_time_option,
      `${datetimeBasedCursor.type}.start_time_option`,
      streamName
    );
  }
  if (datetimeBasedCursor.end_time_option) {
    datetimeBasedCursor.end_time_option = validateAndConvertRequestOption(
      datetimeBasedCursor.end_time_option,
      `${datetimeBasedCursor.type}.end_time_option`,
      streamName
    );
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
  assertType(manifestPaginator, "DefaultPaginator", "stream", streamName);
  const paginator = filterKnownFields(
    manifestPaginator,
    ["type", "pagination_strategy", "page_size_option", "page_token_option"],
    manifestPaginator.type,
    streamName
  );

  if (manifestPaginator.pagination_strategy.type === "CustomPaginationStrategy") {
    throw new ManifestCompatibilityError(streamName, "paginator.pagination_strategy uses a CustomPaginationStrategy");
  }

  let pageSizeOption: RequestOption | undefined = undefined;

  if (paginator.page_size_option) {
    pageSizeOption = validateAndConvertRequestOption(
      paginator.page_size_option,
      `${paginator.type}.page_size_option`,
      streamName
    );
  }

  let pageTokenOption: RequestOptionOrPathInject | undefined = undefined;

  if (paginator.page_token_option?.type === "RequestPath") {
    filterKnownFields(paginator.page_token_option, ["type"], paginator.page_token_option.type, streamName);
    pageTokenOption = { inject_into: "path" };
  } else if (paginator.page_token_option?.type === "RequestOption") {
    pageTokenOption = validateAndConvertRequestOption(
      paginator.page_token_option,
      `${paginator.type}.page_token_option`,
      streamName
    );
  }

  return {
    strategy: manifestPaginatorStrategyToBuilder(manifestPaginator.pagination_strategy, streamName),
    pageTokenOption,
    pageSizeOption,
  };
}

function manifestSchemaLoaderToBuilderSchema(
  manifestSchemaLoader: DeclarativeStreamSchemaLoader | undefined,
  streamName: string
): BuilderStream["schema"] {
  if (manifestSchemaLoader === undefined) {
    return undefined;
  }

  if (Array.isArray(manifestSchemaLoader) || manifestSchemaLoader.type !== "InlineSchemaLoader") {
    throw new ManifestCompatibilityError(streamName, "schema_loader must be a single InlineSchemaLoader");
  }

  const inlineSchemaLoader = manifestSchemaLoader;
  return inlineSchemaLoader.schema ? formatJson(inlineSchemaLoader.schema, true) : undefined;
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
  | JwtAuthenticator
  | SessionTokenAuthenticator;

function isSupportedAuthenticator(authenticator: HttpRequesterAuthenticator): authenticator is SupportedAuthenticator {
  const supportedAuthTypes: string[] = [
    NO_AUTH,
    API_KEY_AUTHENTICATOR,
    BEARER_AUTHENTICATOR,
    BASIC_AUTHENTICATOR,
    JWT_AUTHENTICATOR,
    OAUTH_AUTHENTICATOR,
    SESSION_TOKEN_AUTHENTICATOR,
  ];
  return supportedAuthTypes.includes(authenticator.type);
}

const isSpecDeclarativeOAuth = (
  spec: Spec | undefined
): spec is Spec & {
  advanced_auth: {
    oauth_config_specification: {
      oauth_connector_input_specification: OAuthConfigSpecificationOauthConnectorInputSpecification;
    };
  };
} => {
  return !!spec?.advanced_auth?.oauth_config_specification?.oauth_connector_input_specification;
};

export function manifestAuthenticatorToBuilder(
  authenticator: HttpRequesterAuthenticator | undefined,
  streamName: string | undefined,
  spec: Spec | undefined
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

      let inject_into = apiKeyAuth.inject_into;
      if (authenticator.inject_into && apiKeyAuth.inject_into) {
        inject_into = validateAndConvertRequestOption(
          apiKeyAuth.inject_into,
          `${authenticator.type}.inject_into`,
          undefined
        );
      } else {
        inject_into = {
          type: "RequestOption",
          field_name: apiKeyAuth.header || "",
          inject_into: "header",
        };
      }

      return {
        ...apiKeyAuth,
        inject_into,
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

    case JWT_AUTHENTICATOR: {
      const jwtAuth = filterKnownFields(
        authenticator,
        [
          "type",
          "secret_key",
          "algorithm",
          "token_duration",
          "additional_jwt_headers",
          "additional_jwt_payload",
          "jwt_headers",
          "jwt_payload",
          "header_prefix",
          "base64_encode_secret_key",
        ],
        authenticator.type
      );

      const jwtHeaders = pick(jwtAuth.jwt_headers, ["kid", "typ", "cty"]);
      const jwtPayload = pick(jwtAuth.jwt_payload, ["iss", "sub", "aud"]);
      const headerPrefix = jwtAuth.header_prefix === "" ? undefined : jwtAuth.header_prefix;

      return {
        ...jwtAuth,
        secret_key: interpolateConfigKey(extractAndValidateAuthKey(["secret_key"], jwtAuth, spec)),
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        additional_jwt_headers: Object.entries(jwtAuth.additional_jwt_headers ?? {}),
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        additional_jwt_payload: Object.entries(jwtAuth.additional_jwt_payload ?? {}),
        jwt_headers: jwtHeaders,
        jwt_payload: jwtPayload,
        header_prefix: headerPrefix,
        token_duration: jwtAuth.token_duration ?? undefined,
      };
    }

    case OAUTH_AUTHENTICATOR: {
      const oauth = filterKnownFields(
        authenticator,
        [
          "type",
          "access_token_name",
          "access_token_value",
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

      const isDeclarativeOAuth = isSpecDeclarativeOAuth(spec);

      if (Object.values(oauth.refresh_request_body ?? {}).filter((value) => typeof value !== "string").length > 0) {
        throw new ManifestCompatibilityError(
          streamName,
          "OAuthAuthenticator contains a refresh_request_body with non-string values"
        );
      }
      if (oauth.grant_type && oauth.grant_type !== "refresh_token" && oauth.grant_type !== "client_credentials") {
        throw new ManifestCompatibilityError(
          streamName,
          "OAuthAuthenticator sets custom grant_type, but it must be one of 'refresh_token' or 'client_credentials'"
        );
      }

      let builderAuthenticator: BuilderFormOAuthAuthenticator & {
        declarative?: BuilderFormDeclarativeOAuthAuthenticator["declarative"];
      } = {
        ...oauth,
        type: isDeclarativeOAuth ? DeclarativeOAuthAuthenticatorType : OAUTH_AUTHENTICATOR,
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        refresh_request_body: Object.entries(oauth.refresh_request_body ?? {}),
        grant_type: oauth.grant_type ?? oauth.refresh_token_updater ? "refresh_token" : "client_credentials",
        refresh_token_updater: undefined,
        client_id: interpolateConfigKey(extractAndValidateAuthKey(["client_id"], oauth, spec)),
        client_secret: interpolateConfigKey(extractAndValidateAuthKey(["client_secret"], oauth, spec)),
      };

      if (isDeclarativeOAuth) {
        if (!spec.advanced_auth.oauth_config_specification.oauth_connector_input_specification.extract_output) {
          throw new ManifestCompatibilityError(streamName, "OAuthAuthenticator.extract_output is missing");
        }

        builderAuthenticator.declarative = {
          ...(omit(
            spec.advanced_auth.oauth_config_specification.oauth_connector_input_specification,
            "extract_output",
            "access_token_params",
            "access_token_headers",
            "state"
          ) as BuilderFormDeclarativeOAuthAuthenticator["declarative"]),
          access_token_key:
            spec.advanced_auth.oauth_config_specification.oauth_connector_input_specification.extract_output[0],

          access_token_params: Object.entries(
            spec.advanced_auth.oauth_config_specification.oauth_connector_input_specification.access_token_params ?? {}
          ),
          access_token_headers: Object.entries(
            spec.advanced_auth.oauth_config_specification.oauth_connector_input_specification.access_token_headers ?? {}
          ),

          state: spec.advanced_auth.oauth_config_specification.oauth_connector_input_specification.state
            ? JSON.stringify(
                spec.advanced_auth.oauth_config_specification.oauth_connector_input_specification.state,
                null,
                2
              )
            : undefined,
        };
      }

      const authenticatorDefinesRefreshToken =
        !isDeclarativeOAuth /* Legacy OAuth flow */ ||
        !!authenticator.refresh_token_updater; /* declarative w/refresh_token */

      if (authenticatorDefinesRefreshToken && (!oauth.grant_type || oauth.grant_type === "refresh_token")) {
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

          if (!isDeclarativeOAuth && !isEqual(refreshTokenUpdater?.refresh_token_config_path, [refreshTokenSpecKey])) {
            throw new ManifestCompatibilityError(
              streamName,
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
            refresh_token_updater: isDeclarativeOAuth
              ? { ...refresh_token_updater, access_token: "", token_expiry_date: "" }
              : {
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
          streamName,
          `SessionTokenAuthenticator request_authentication must have one of the following types: ${SESSION_TOKEN_REQUEST_API_KEY_AUTHENTICATOR}, ${SESSION_TOKEN_REQUEST_BEARER_AUTHENTICATOR}`
        );
      }

      const decoderType = sessionTokenAuth.decoder?.type;
      const supportedDecoders: Array<string | undefined> = [
        undefined,
        JsonDecoderType.JsonDecoder,
        XmlDecoderType.XmlDecoder,
      ];
      if (!supportedDecoders.includes(decoderType)) {
        throw new ManifestCompatibilityError(streamName, "SessionTokenAuthenticator decoder is not supported");
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
          streamName,
          `SessionTokenAuthenticator.login_requester.authenticator must have one of the following types: ${NO_AUTH}, ${API_KEY_AUTHENTICATOR}, ${BEARER_AUTHENTICATOR}, ${BASIC_AUTHENTICATOR}`
        );
      }
      const builderLoginRequesterAuthenticator = manifestAuthenticatorToBuilder(
        manifestLoginRequester.authenticator,
        streamName,
        spec
      );

      return {
        ...sessionTokenAuth,
        login_requester: {
          url: extractFullUrl(manifestLoginRequester),
          authenticator: builderLoginRequesterAuthenticator as
            | ApiKeyAuthenticator
            | BearerAuthenticator
            | BasicHttpAuthenticator,
          httpMethod: manifestLoginRequester.http_method === "GET" ? "GET" : "POST",
          requestOptions: {
            requestParameters: manifestRequestParametersToBuilder(
              manifestLoginRequester.request_parameters,
              streamName
            ),
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
  entityType: "stream" | "dynamic_stream",
  entityName?: string
): asserts object is T {
  if (object.type !== typeString) {
    if (entityType === "stream") {
      throw new ManifestCompatibilityError(entityName, `doesn't use a ${typeString}`);
    } else if (entityType === "dynamic_stream") {
      throw new ManifestCompatibilityError(undefined, `Dynamic stream ${entityName} doesn't use a ${typeString}`);
    }
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
  spec?: Spec
): BuilderOutput | YamlString {
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
  const isDeclarativeOAuth = isSpecDeclarativeOAuth(manifestSpec);
  return extractAndValidateSpecKey(
    path,
    get(authenticator, path),
    get(
      LOCKED_INPUT_BY_FIELD_NAME_BY_AUTH_TYPE[
        isDeclarativeOAuth ? DeclarativeOAuthAuthenticatorType : authenticator.type
      ],
      path
    ),
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

  // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
  const specDefinition = specKey ? spec?.connection_specification?.properties?.[specKey] : undefined;
  if (!specDefinition) {
    throw new ManifestCompatibilityError(
      streamName,
      `${manifestPath} references spec key "${specKey}", which must appear in the spec`
    );
  }
  // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
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
  return filterKnownFields(
    requestOption,
    ["type", "field_name", "field_path", "inject_into"],
    componentName,
    streamName
  );
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

function convertRequestOptionLegacyFieldNameToFieldPath(option: RequestOption, streamName?: string): RequestOption {
  // We are introducing a new field_path (type: string[]) to the RequestOption type, to support nested field injection.
  // Eventually, we should deprecate field_name (type: string), since field_path is more flexible.
  // However, because existing builder projects already use field_name and we trigger stream change warnings on any schema change,
  // we need to support both fields for now to avoid triggering unnecessary and potentially confusing warnings.
  // This function converts a manifest field_name into a single element field_path array for the UI component:
  // RequestOption.field_name: 'page_size' -> RequestOption.field_path: ['page_size']

  // TODO: Remove this function once we are ready to fully deprecate RequestOption.field_name

  const base = filterRequestOption(option, `${option.type}`, streamName);
  if (option.inject_into === RequestOptionInjectInto.body_json && option.field_name) {
    const value = {
      ...base,
      field_path: [option.field_name],
      field_name: undefined,
    };

    return value;
  }

  return base;
}

function validateRequestOptionFieldExclusivity(option: RequestOption, componentPath: string, streamName?: string) {
  // Validates that a RequestOption has either a field_name OR field_path
  if (option.field_name && option.field_path) {
    throw new ManifestCompatibilityError(streamName, `${componentPath} cannot have both field_name and field_path`);
  }
}

function validateAndConvertRequestOption(
  // Helper function combining the validation and conversion of a RequestOption field_name to field_path
  option: RequestOption,
  componentPath: string,
  streamName?: string
): RequestOption {
  validateRequestOptionFieldExclusivity(option, componentPath, streamName);
  return convertRequestOptionLegacyFieldNameToFieldPath(option, streamName);
}
