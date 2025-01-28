import { UseQueryResult, useQuery } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { AppActionCodes, trackAction } from "core/utils/datadog";
import { isDevelopment } from "core/utils/isDevelopment";
import { links } from "core/utils/links";

import { getConnectorDocumentation } from "../generated/AirbyteClient";
import { ConnectorDocumentationRead } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";

const DOCS_URL = /^https:\/\/docs\.airbyte\.(io|com)/;
const LOCAL_DOCS_PATH = "/docs";
const GITHUB_DOCS_INTEGRATIONS_URL = `https://raw.githubusercontent.com/airbytehq/airbyte/master/docs/integrations`;
const ENTERPRISE_STUBS_URL = `${GITHUB_DOCS_INTEGRATIONS_URL}/enterprise-connectors/`;
export const GITHUB_DOCS_SOURCES_URL = `${GITHUB_DOCS_INTEGRATIONS_URL}/sources/`;
export const GITHUB_DOCS_DESTINATIONS_URL = `${GITHUB_DOCS_INTEGRATIONS_URL}/destinations/`;
export const REMOTE_DOCS_SOURCES_URL = `${links.docsLink}/integrations/sources/`;
export const REMOTE_DOCS_DESTINATIONS_URL = `${links.docsLink}/integrations/destinations/`;
export const LOCAL_DOCS_SOURCES_PATH = `${LOCAL_DOCS_PATH}/integrations/sources/`;
export const LOCAL_DOCS_DESTINATIONS_PATH = `${LOCAL_DOCS_PATH}/integrations/sources/`;

// Documentation URLs used to be required for custom connector validation (no longer true), and the
// Connector Builder UI provided no way to set one (still true); this URL was automatically set to make
// the connector valid. Exclude this to avoid rendering bogus or broken docs for those already-existing
// custom connectors.
export const EXCLUDED_DOC_URLS = ["https://example.org"];

const connectorDocumentationKeys = {
  all: ["connectorDocumentation"] as const,
  get: (workspaceId: string, actorDefinitionId: string | undefined, actorId?: string) =>
    [...connectorDocumentationKeys.all, "get", { workspaceId, actorDefinitionId, actorId }] as const,
};

export const useConnectorDocumentation = (
  actorType: "source" | "destination" | undefined,
  actorDefinitionId: string | undefined,
  actorId?: string,
  documentationUrl?: string
): UseQueryResult<ConnectorDocumentationRead | undefined, unknown> => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  const isDev = isDevelopment();

  let fetchDocumentation = async () => {
    if (actorDefinitionId?.startsWith("source-")) {
      // Case 1: Handle enterprise stub documentation

      // This is a temporary hack for displaying enterprise stub documentation.
      // We cannot use the existing getConnectorDocumentation function because it expects a UUID actorDefinitionId.
      // Instead, we pass the stub id (formatted as source-<connectorName>-enterprise), and fetch the raw markdown directly from Github.
      // This is not viable long-term, and should be replaced ASAP with a solution that integrates cleanly with the existing API call.
      // Tracking issue: https://github.com/airbytehq/airbyte-internal-issues/issues/10098
      const connectorName = actorDefinitionId.replace("-enterprise", "");
      const markdownUrl = `${ENTERPRISE_STUBS_URL}${connectorName}.md`;

      const response = await fetch(markdownUrl);
      if (!response.ok) {
        throw new Error(`Failed to fetch documentation: ${response.statusText}`);
      }

      const text = await response.text();
      return { doc: text };
    } else if (actorType && actorDefinitionId) {
      // Case 2: Handle regular connector documentation
      return getConnectorDocumentation({ actorType, actorDefinitionId, workspaceId, actorId }, requestOptions);
    }
    // Case 3: Return undefined if neither condition is met
    return undefined;
  };

  if (isDev && documentationUrl) {
    const localDocPath = documentationUrl.replace(DOCS_URL, LOCAL_DOCS_PATH);
    fetchDocumentation = () =>
      fetch(`${localDocPath}.md`)
        .then((response) => response.text())
        .then((text) => {
          return {
            doc: text,
          };
        });
  }

  return useQuery(connectorDocumentationKeys.get(workspaceId, actorDefinitionId, actorId), fetchDocumentation, {
    onError: (error) => {
      trackAction(AppActionCodes.CONNECTOR_DOCUMENTATION_FETCH_ERROR, {
        workspaceId,
        actorDefinitionId,
        actorId,
        error,
      });
    },
  });
};
