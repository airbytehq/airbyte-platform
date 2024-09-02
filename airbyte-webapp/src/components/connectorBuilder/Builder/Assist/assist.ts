import { BuilderAssistManifestResponse } from "core/api";
import { DeclarativeComponentSchema } from "core/api/types/ConnectorManifest";

import { convertToBuilderFormValuesSync } from "../../convertManifestToBuilderForm";
import { BuilderFormValues } from "../../types";

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
