import { ConnectorBuilderProjectTestingValues } from "core/api/types/AirbyteClient";
import {
  ConnectorManifest,
  DeclarativeStream,
  AsyncRetrieverType,
  DeclarativeStreamType,
  SimpleRetrieverType,
  HttpRequesterType,
  DpathExtractorType,
  DynamicDeclarativeStream,
  DynamicDeclarativeStreamType,
  RecordSelectorType,
  HttpRequesterHttpMethod,
  AsyncJobStatusMapType,
  CustomRetrieverType,
  HttpComponentsResolverType,
} from "core/api/types/ConnectorManifest";

import { CDK_VERSION } from "./cdk";
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
  yaml: string;
  customComponentsCode?: string;
  view: { type: "global" } | { type: "inputs" } | { type: "components" } | StreamId;
  streamTab: BuilderStreamTab;
  testStreamId: StreamId;
  testingValues: ConnectorBuilderProjectTestingValues | undefined;
  manifest: ConnectorManifest;
  generatedStreams: Record<string, DeclarativeStream[]>;
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

export interface StreamTestResults {
  streamHash: string | null;
  isStale?: boolean;
  hasResponse?: boolean;
  responsesAreSuccessful?: boolean;
  hasRecords?: boolean;
  primaryKeysArePresent?: boolean;
  primaryKeysAreUnique?: boolean;
}

export type GeneratedDeclarativeStream = DeclarativeStream & {
  dynamic_stream_name: string;
};

export const DEFAULT_CONNECTOR_NAME = "Untitled";

// TODO(lmossman): add these back as suggestions
// export const LARGE_DURATION_OPTIONS = [
//   { value: "PT1H", description: "1 hour" },
//   { value: "P1D", description: "1 day" },
//   { value: "P1W", description: "1 week" },
//   { value: "P1M", description: "1 month" },
//   { value: "P1Y", description: "1 year" },
// ];
// export const SMALL_DURATION_OPTIONS = [
//   { value: "PT0.000001S", description: "1 microsecond" },
//   { value: "PT0.001S", description: "1 millisecond" },
//   { value: "PT1S", description: "1 second" },
//   { value: "PT1M", description: "1 minute" },
//   { value: "PT1H", description: "1 hour" },
//   { value: "P1D", description: "1 day" },
// ];
// export const DATETIME_FORMAT_OPTIONS = [
//   { value: "%Y-%m-%d" },
//   { value: "%Y-%m-%d %H:%M:%S" },
//   { value: "%Y-%m-%dT%H:%M:%S" },
//   { value: "%Y-%m-%dT%H:%M:%SZ" },
//   { value: "%Y-%m-%dT%H:%M:%S%z" },
//   { value: "%Y-%m-%dT%H:%M:%S.%fZ" },
//   { value: "%Y-%m-%dT%H:%M:%S.%f%z" },
//   { value: "%Y-%m-%d %H:%M:%S.%f+00:00" },
//   { value: "%s" },
//   { value: "%ms" },
// ];

export const DEFAULT_SCHEMA_LOADER_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  properties: {},
  additionalProperties: true,
};

export const BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE = "manifest-only";

export const OAUTH_ACCESS_TOKEN_INPUT = "oauth_access_token";
export const OAUTH_TOKEN_EXPIRY_DATE_INPUT = "oauth_token_expiry_date";

export function isStreamDynamicStream(streamId: StreamId): boolean {
  return streamId.type === "dynamic_stream";
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

export const DEFAULT_SYNC_STREAM: DeclarativeStream = {
  type: DeclarativeStreamType.DeclarativeStream,
  retriever: {
    type: SimpleRetrieverType.SimpleRetriever,
    record_selector: {
      type: RecordSelectorType.RecordSelector,
      extractor: {
        type: DpathExtractorType.DpathExtractor,
        field_path: [],
      },
    },
    requester: {
      type: HttpRequesterType.HttpRequester,
      http_method: HttpRequesterHttpMethod.GET,
    },
  },
};

export const DEFAULT_ASYNC_STREAM: DeclarativeStream = {
  type: DeclarativeStreamType.DeclarativeStream,
  retriever: {
    type: AsyncRetrieverType.AsyncRetriever,
    record_selector: {
      type: RecordSelectorType.RecordSelector,
      extractor: {
        type: DpathExtractorType.DpathExtractor,
        field_path: [],
      },
    },
    creation_requester: {
      type: HttpRequesterType.HttpRequester,
      http_method: HttpRequesterHttpMethod.POST,
    },
    polling_requester: {
      type: HttpRequesterType.HttpRequester,
      http_method: HttpRequesterHttpMethod.GET,
    },
    download_requester: {
      type: HttpRequesterType.HttpRequester,
      http_method: HttpRequesterHttpMethod.GET,
    },
    status_mapping: {
      type: AsyncJobStatusMapType.AsyncJobStatusMap,
      running: [],
      completed: [],
      failed: [],
      timeout: [],
    },
    status_extractor: {
      type: DpathExtractorType.DpathExtractor,
      field_path: [],
    },
    download_target_extractor: {
      type: DpathExtractorType.DpathExtractor,
      field_path: [],
    },
  },
};

export const DEFAULT_CUSTOM_STREAM: DeclarativeStream = {
  type: DeclarativeStreamType.DeclarativeStream,
  retriever: {
    type: CustomRetrieverType.CustomRetriever,
    class_name: "",
  },
};

export const DEFAULT_DYNAMIC_STREAM: DynamicDeclarativeStream = {
  type: DynamicDeclarativeStreamType.DynamicDeclarativeStream,
  stream_template: DEFAULT_SYNC_STREAM,
  components_resolver: {
    type: HttpComponentsResolverType.HttpComponentsResolver,
    retriever: {
      type: SimpleRetrieverType.SimpleRetriever,
      record_selector: {
        type: RecordSelectorType.RecordSelector,
        extractor: {
          type: DpathExtractorType.DpathExtractor,
          field_path: [],
        },
      },
      requester: {
        type: HttpRequesterType.HttpRequester,
        http_method: HttpRequesterHttpMethod.GET,
      },
    },
    components_mapping: [],
  },
};

export const getStreamFieldPath = <T extends string>(streamId: StreamId, fieldPath?: T, templatePath?: boolean) => {
  const basePath =
    streamId.type === "stream"
      ? `manifest.streams.${streamId.index}`
      : streamId.type === "dynamic_stream"
      ? templatePath
        ? `manifest.dynamic_streams.${streamId.index}.stream_template`
        : `manifest.dynamic_streams.${streamId.index}`
      : `generatedStreams.${streamId.dynamicStreamName}.${streamId.index}`;

  if (fieldPath) {
    return `${basePath}.${fieldPath}`;
  }
  return basePath;
};
