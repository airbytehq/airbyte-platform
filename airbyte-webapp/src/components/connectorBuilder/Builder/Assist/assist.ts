import merge from "lodash/merge";

import { CDK_VERSION } from "components/connectorBuilder/cdk";
import { convertToBuilderFormValuesSync } from "components/connectorBuilder/convertManifestToBuilderForm";
import { BuilderFormValues } from "components/connectorBuilder/types";
import { useBuilderWatch } from "components/connectorBuilder/useBuilderWatch";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { HttpError, useAssistApiMutation, useAssistApiProxyQuery } from "core/api";
import { KnownExceptionInfo } from "core/api/types/ConnectorBuilderClient";
import {
  DatetimeBasedCursor,
  DeclarativeComponentSchema,
  HttpRequesterAuthenticator,
  HttpRequesterHttpMethod,
  PrimaryKey,
  RecordSelector,
  SimpleRetrieverPaginator,
  SpecConnectionSpecification,
} from "core/api/types/ConnectorManifest";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

export type AssistKey =
  | "urlbase"
  | "auth"
  | "metadata"
  | "record_selector"
  | "paginator"
  | "request_options"
  | "incremental_sync";

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
            request_headers: update?.request_headers ?? undefined,
            request_body_json: update?.request_body_json ?? undefined,
          },
          paginator: update?.paginator ?? undefined,
        },
        primary_key: update?.primary_key ?? undefined,
        incremental_sync: update?.incremental_sync ?? undefined,
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
  stream_response?: object;
}
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
  request_headers: Record<string, string> | null;
  request_body_json: Record<string, string> | null;
  incremental_sync: DatetimeBasedCursor | null;
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
  input: Record<string, unknown>,
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

const useAssistGlobalContext = (): { params: Record<string, unknown> } => {
  const params = {
    cdk_version: CDK_VERSION,
    workspace_id: useCurrentWorkspaceId(),
  };
  return { params };
};

const useAssistProjectContext = (): { params: Record<string, unknown>; hasRequiredParams: boolean } => {
  const { assistEnabled, projectId, assistSessionId, jsonManifest } = useConnectorBuilderFormState();
  // session id on form
  const params = {
    docs_url: useBuilderWatch("formValues.assist.docsUrl"),
    openapi_spec_url: useBuilderWatch("formValues.assist.openapiSpecUrl"),
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
): { params: Record<string, unknown>; hasRequiredParams: boolean } => {
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

export const useBuilderAssistFindRequestOptions = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_request_parameters", hasRequiredParams, params, [
    "url_base",
  ]);
};

export const useBuilderAssistFindIncrementalSync = (input: BuilderAssistInputStreamParams) => {
  const { params, hasRequiredParams } = useAssistProjectStreamContext(input);
  return useAssistProxyQuery<BuilderAssistManifestResponse>("find_stream_incremental_sync", hasRequiredParams, params, [
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
  return useAssistApiMutation<Record<string, unknown>, BuilderAssistCreateConnectorResponse>(globalParams);
};

const assistErrorCodesToi18n = {
  url_format_error: "connectorBuilder.assist.error.urlFormatError",
  url_not_reachable: "connectorBuilder.assist.error.urlNotReachable",
};

export interface ExpectedAssistErrorFormError {
  field?: string;
  error_type?: string;
  message?: string;
}

export interface AssistErrorFormError {
  fieldName: string;
  errorType: string;
  errorMessage: string;
}

/**
 * Applies i18n to an assist error message.
 *
 * Note: This uses the "error_code" appended to the end of some error message to look up the i18n key.
 * This is not part of the public API and may change.
 */
const applyi18n = (errorMessage: string): string => {
  const [message, error_code] = errorMessage.split("error_code:").map((s) => s.trim());
  if (error_code) {
    const i18nKey = assistErrorCodesToi18n[error_code as keyof typeof assistErrorCodesToi18n];
    return i18nKey ?? errorMessage;
  }
  return message;
};

/**
 * Safely casts an assist validation error to a form error.
 */
export const safeCastAssistValidationError = (
  apiValidationError: ExpectedAssistErrorFormError
): AssistErrorFormError => {
  const { field, error_type, message } = apiValidationError;
  const fieldName = field ?? "";
  const errorType = error_type ?? "";
  const errorMessage = applyi18n(message ?? "");

  return {
    fieldName,
    errorType,
    errorMessage: errorMessage.trim(),
  };
};

export const parseAssistErrorToFormErrors = (assistError: Error | null): AssistErrorFormError[] => {
  if (!assistError || !(assistError instanceof HttpError)) {
    return [];
  }

  const details = (assistError as HttpError<KnownExceptionInfo>).response?.details;
  if (!details) {
    return [];
  }

  const validationErrors = Array.isArray(details?.validation_errors) ? details.validation_errors : [];
  return validationErrors?.map(safeCastAssistValidationError) ?? [];
};

const safeJsonParse = (jsonString?: string): object | undefined => {
  if (!jsonString) {
    return undefined;
  }

  try {
    return JSON.parse(jsonString);
  } catch (error) {
    console.error("Surpressed Error parsing JSON string:", error);
    return undefined;
  }
};

/**
 * Compute the stream response value expected by the assist API from the stream response body and header.
 */
export const computeStreamResponse = (
  streamResponseBodyJsonString: string,
  streamResponseHeaderJsonString: string = "{}"
): object | undefined => {
  const streamResponseBody = safeJsonParse(streamResponseBodyJsonString);
  if (streamResponseBody === undefined) {
    return undefined;
  }

  const stream_response = {
    header_schema: safeJsonParse(streamResponseHeaderJsonString),
    body_schema: streamResponseBody,
  };
  return stream_response;
};
