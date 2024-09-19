import merge from "lodash/merge";

import { CDK_VERSION } from "components/connectorBuilder/cdk";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useAssistApiMutation, useAssistApiProxyQuery } from "core/api";
import { AssistV1ProcessRequestBody } from "core/api/types/ConnectorBuilderClient";
import {
  DeclarativeComponentSchema,
  HttpRequesterAuthenticator,
  HttpRequesterHttpMethod,
  PrimaryKey,
  RecordSelector,
  SimpleRetrieverPaginator,
  SpecConnectionSpecification,
} from "core/api/types/ConnectorManifest";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { convertToBuilderFormValuesSync } from "../../convertManifestToBuilderForm";
import { BuilderFormValues, useBuilderWatch } from "../../types";

export type AssistKey = "urlbase" | "auth" | "metadata" | "record_selector" | "paginator";

export const convertToAssistFormValuesSync = (updates: BuilderAssistManifestResponse): BuilderFormValues => {
  const update = updates.manifest_update;
  const updatedManifest: DeclarativeComponentSchema = {
    type: "DeclarativeSource",
    version: "",
    check: {
      type: "CheckStream",
      stream_names: [],
    },
    streams: [
      {
        type: "DeclarativeStream",
        retriever: {
          type: "SimpleRetriever",
          record_selector: update?.record_selector ?? {
            type: "RecordSelector",
            extractor: {
              type: "DpathExtractor",
              field_path: [],
            },
          },
          requester: {
            type: "HttpRequester",
            url_base: update?.url_base ?? "",
            authenticator: update?.auth ?? undefined,
            path: update?.stream_path ?? "",
            http_method: update?.stream_http_method ?? "GET",
          },
          paginator: update?.paginator ?? undefined,
        },
        primary_key: update?.primary_key ?? undefined,
      },
    ],
    spec: {
      type: "Spec",
      connection_specification: update?.connection_specification ?? {
        required: [],
        properties: {},
      },
    },
  };
  const updatedForm = convertToBuilderFormValuesSync(updatedManifest);
  return updatedForm;
};

export interface BuilderAssistCoreParams {
  docs_url?: string;
  openapi_spec_url?: string;
  app_name: string;
}

export interface BuilderAssistGlobalMetadataParams {
  cdk_version: string;
  workspace_id: string;
}
export interface BuilderAssistProjectMetadataParams {
  project_id: string;
  manifest: DeclarativeComponentSchema;
  url_base?: string;
  session_id: string;
}
export interface BuilderAssistInputStreamParams {
  stream_name: string;
}
type BuilderAssistUseProject = BuilderAssistCoreParams & BuilderAssistProjectMetadataParams;
type BuilderAssistUseProjectStream = BuilderAssistUseProject & BuilderAssistInputStreamParams;

export type BuilderAssistApiAllParams = BuilderAssistCoreParams &
  BuilderAssistProjectMetadataParams &
  BuilderAssistProjectMetadataParams &
  BuilderAssistInputStreamParams;
export type BuilderAssistInputAllParams = BuilderAssistInputStreamParams;

export interface ManifestUpdate {
  cdk_version: string;
  url_base: string | null;
  auth: HttpRequesterAuthenticator | null;
  paginator: SimpleRetrieverPaginator | null;
  connection_specification: SpecConnectionSpecification | null;
  stream_path: string | null;
  stream_http_method: HttpRequesterHttpMethod | null;
  record_selector: RecordSelector | null;
  primary_key: PrimaryKey | null;
}

export interface BuilderAssistBaseResponse {
  metadata: {
    session_id: string;
    server_cdk_version: string;
  };
}
export interface BuilderAssistManifestResponse extends BuilderAssistBaseResponse {
  manifest_update: ManifestUpdate;
}

const useAssistProxyQuery = <T>(
  controller: string,
  enabled: boolean,
  input: AssistV1ProcessRequestBody,
  ignoreCacheKeys: Array<keyof BuilderAssistApiAllParams>
) => {
  const { params: globalParams } = useAssistGlobalContext();
  const params = merge({}, globalParams, input);
  return useAssistApiProxyQuery<T>(controller, enabled, params, ignoreCacheKeys);
};

const hasValueWithTrim = (value: unknown) => {
  // if the value is a string, trim first
  if (typeof value === "string") {
    return value.trim() !== "";
  }
  // else check if truthy
  return !!value;
};

const hasOneOf = <T>(params: T, keys: Array<keyof T>) => {
  return keys.some((key) => hasValueWithTrim(params[key]));
};

const hasAllOf = <T>(params: T, keys: Array<keyof T>) => {
  return keys.every((key) => !!(params[key] as string)?.trim());
};

const useAssistGlobalContext = (): { params: BuilderAssistGlobalMetadataParams } => {
  const params: BuilderAssistGlobalMetadataParams = {
    cdk_version: CDK_VERSION,
    workspace_id: useCurrentWorkspaceId(),
  };
  return { params };
};

const useAssistProjectContext = (): { params: BuilderAssistUseProject; hasRequiredParams: boolean } => {
  const { assistEnabled, projectId, assistSessionId, jsonManifest } = useConnectorBuilderFormState();
  // session id on form
  const params: BuilderAssistUseProject = {
    docs_url: useBuilderWatch("formValues.assist.docsUrl"),
    openapi_spec_url: useBuilderWatch("formValues.assist.openApiSpecUrl"),
    app_name: useBuilderWatch("name") || "Connector",
    url_base: useBuilderWatch("formValues.global.urlBase"),
    project_id: projectId,
    manifest: jsonManifest,
    session_id: assistSessionId,
  };
  const hasBase = hasOneOf(params, ["docs_url", "openapi_spec_url"]);
  const hasRequiredParams = assistEnabled && hasBase;
  return { params, hasRequiredParams };
};

const useAssistProjectStreamContext = (
  input: BuilderAssistInputStreamParams
): { params: BuilderAssistUseProjectStream; hasRequiredParams: boolean } => {
  const { params: baseParams, hasRequiredParams: baseRequiredParams } = useAssistProjectContext();
  const params = { ...baseParams, ...input };
  const hasStream = hasAllOf(params, ["stream_name"]);
  const hasRequiredParams = baseRequiredParams && hasStream;
  return { params, hasRequiredParams };
};

export const useBuilderAssistFindUrlBase = () => {
  const { params, hasRequiredParams } = useAssistProjectContext();
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_url_base", hasRequiredParams, params, ["url_base"]);
};

export const useBuilderAssistFindAuth = () => {
  const { params, hasRequiredParams } = useAssistProjectContext();
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_auth", hasRequiredParams, params, ["url_base"]);
};

export const useBuilderAssistCreateStream = (input: BuilderAssistInputStreamParams) => {
  // this one is always enabled, let the server return an error if there is a problem
  const { params } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("create_stream", true, params, []);
};

export const useBuilderAssistFindStreamPaginator = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_stream_pagination", hasRequiredParams, params, [
    "url_base",
  ]);
};

export const useBuilderAssistStreamMetadata = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  // note: url_base important here because path is built off of it
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_stream_metadata", hasRequiredParams, params, []);
};

export const useBuilderAssistStreamResponse = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_response_structure", hasRequiredParams, params, [
    "url_base",
  ]);
};

export interface BuilderAssistFindStreamsParams {
  enabled: boolean;
}
export interface BuilderAssistFoundStream {
  stream_name: string;
}
export interface BuilderAssistFindStreamsResponse extends BuilderAssistBaseResponse {
  streams: BuilderAssistFoundStream[];
}

export const useBuilderAssistFindStreams = (input: BuilderAssistFindStreamsParams) => {
  const { params, hasRequiredParams } = useAssistProjectContext();
  const enabled = input.enabled && hasRequiredParams;
  return useAssistProxyQuery<BuilderAssistFindStreamsResponse>("find_streams", enabled, params, ["url_base"]);
};

export interface BuilderAssistCreateConnectorParams extends BuilderAssistCoreParams {
  stream_name: string;
  session_id: string;
}
export interface BuilderAssistCreateConnectorResponse extends BuilderAssistBaseResponse {
  connector: DeclarativeComponentSchema;
}

export const useBuilderAssistCreateConnectorMutation = () => {
  const { params: globalParams } = useAssistGlobalContext();
  return useAssistApiMutation<BuilderAssistCreateConnectorParams, BuilderAssistCreateConnectorResponse>(globalParams);
};
