import {
  ConnectorManifest,
  DeclarativeStream,
  HttpRequesterHttpMethod,
  HttpRequesterType,
  DpathExtractorType,
  SimpleRetrieverType,
  DeclarativeStreamType,
  RecordSelectorType,
  AsyncRetrieverType,
  AsyncJobStatusMapType,
  HttpComponentsResolverType,
  DynamicDeclarativeStreamType,
  DynamicDeclarativeStream,
  CustomRetrieverType,
} from "core/api/types/ConnectorManifest";

import { CDK_VERSION } from "./cdk";

export const DEFAULT_CONNECTOR_NAME = "Untitled";

export const DEFAULT_SCHEMA_LOADER_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  properties: {},
  additionalProperties: true,
};

export const BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE = "manifest-only";

export const OAUTH_ACCESS_TOKEN_INPUT = "oauth_access_token";
export const OAUTH_TOKEN_EXPIRY_DATE_INPUT = "oauth_token_expiry_date";

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

export const DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM: ConnectorManifest = {
  ...DEFAULT_JSON_MANIFEST_VALUES,
  streams: [DEFAULT_SYNC_STREAM],
};

// TODO(lmossman): add these back as suggestions (https://github.com/airbytehq/airbyte-internal-issues/issues/13341)
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
