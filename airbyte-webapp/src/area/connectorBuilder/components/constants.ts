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
  SelectiveAuthenticatorType,
  LegacySessionTokenAuthenticatorType,
  NoAuthType,
  CustomAuthenticatorType,
  ApiKeyAuthenticatorType,
  BasicHttpAuthenticatorType,
  BearerAuthenticatorType,
  OAuthAuthenticatorType,
  JwtAuthenticatorType,
  SessionTokenAuthenticatorType,
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
  stream_template: {
    ...DEFAULT_SYNC_STREAM,
    name: "placeholder",
  },
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

export const INPUT_REFERENCE_KEYWORD = "config";

export const VALID_AUTHENTICATOR_TYPES = [
  ApiKeyAuthenticatorType.ApiKeyAuthenticator,
  BasicHttpAuthenticatorType.BasicHttpAuthenticator,
  BearerAuthenticatorType.BearerAuthenticator,
  OAuthAuthenticatorType.OAuthAuthenticator,
  JwtAuthenticatorType.JwtAuthenticator,
  SessionTokenAuthenticatorType.SessionTokenAuthenticator,
  SelectiveAuthenticatorType.SelectiveAuthenticator,
  CustomAuthenticatorType.CustomAuthenticator,
  NoAuthType.NoAuth,
  LegacySessionTokenAuthenticatorType.LegacySessionTokenAuthenticator,
] as const;
