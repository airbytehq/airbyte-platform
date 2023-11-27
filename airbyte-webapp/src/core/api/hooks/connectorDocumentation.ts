import { UseQueryResult, useQuery } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { isDevelopment } from "core/utils/isDevelopment";
import { links } from "core/utils/links";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";

import { getConnectorDocumentation } from "../generated/AirbyteClient";
import { ConnectorDocumentationRead } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";

const DOCS_URL = /^https:\/\/docs\.airbyte\.(io|com)/;
const LOCAL_DOCS_PATH = "/docs";
const GITHUB_DOCS_INTEGRATIONS_URL = `https://raw.githubusercontent.com/airbytehq/airbyte/master/docs/integrations`;
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
  const { trackAction } = useAppMonitoringService();

  let fetchDocumentation = () =>
    actorType && actorDefinitionId
      ? getConnectorDocumentation({ actorType, actorDefinitionId, workspaceId, actorId }, requestOptions)
      : undefined;

  if (isDev && documentationUrl) {
    const localDocPath = documentationUrl.replace(DOCS_URL, LOCAL_DOCS_PATH);
    fetchDocumentation = () =>
      fetch(`${localDocPath}.inapp.md`)
        .then((response) => {
          // try to fetch inapp doc first
          if (response.ok) {
            return response;
          }
          // if inapp doc is not found, try to fetch the full doc
          return fetch(`${localDocPath}.md`);
        })
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
