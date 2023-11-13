import {
  ConnectionCreate,
  ConnectionIdRequestBody,
  DestinationCreate,
  DestinationIdRequestBody,
  DestinationRead,
  DestinationReadList,
  SourceCreate,
  SourceDiscoverSchemaRead,
  SourceDiscoverSchemaRequestBody,
  SourceIdRequestBody,
  SourceRead,
  SourceReadList,
  WebBackendConnectionRead,
  WebBackendConnectionReadList,
  WebBackendConnectionRequestBody,
  WebBackendConnectionUpdate,
} from "@src/core/api/types/AirbyteClient";

import { getWorkspaceId, setWorkspaceId } from "./workspace";

const getApiUrl = (path: string): string => `${Cypress.env("AIRBYTE_SERVER_BASE_URL")}/api/v1${path}`;

const apiRequest = <T = void>(
  method: Cypress.HttpMethod,
  path: string,
  payload?: Cypress.RequestBody,
  expectedStatus = 200
): Cypress.Chainable<T> =>
  cy.request(method, getApiUrl(path), payload).then((response) => {
    expect(response.status).to.eq(expectedStatus, "response status");
    return response.body;
  });

export const requestWorkspaceId = () =>
  apiRequest<{ workspaces: Array<{ workspaceId: string }> }>("POST", "/workspaces/list").then(
    ({ workspaces: [{ workspaceId }] }) => {
      setWorkspaceId(workspaceId);
    }
  );

export const completeInitialSetup = () =>
  apiRequest("POST", "/workspaces/update", {
    workspaceId: getWorkspaceId(),
    initialSetupComplete: true,
    displaySetupWizard: true,
  });

export const requestConnectionsList = () =>
  apiRequest<WebBackendConnectionReadList>("POST", "/connections/list", {
    workspaceId: getWorkspaceId(),
  });

export const requestCreateConnection = (body: ConnectionCreate) =>
  apiRequest<WebBackendConnectionRead>("POST", "/web_backend/connections/create", body);

export const requestUpdateConnection = (body: WebBackendConnectionUpdate) =>
  apiRequest<WebBackendConnectionRead>("POST", "/web_backend/connections/update", body);

export const requestGetConnection = (body: WebBackendConnectionRequestBody) =>
  apiRequest<WebBackendConnectionRead>("POST", "/web_backend/connections/get", body);

export const requestDeleteConnection = (body: ConnectionIdRequestBody) =>
  apiRequest("POST", "/connections/delete", body, 204);

export const requestSourcesList = () =>
  apiRequest<SourceReadList>("POST", "/sources/list", {
    workspaceId: getWorkspaceId(),
  });

export const requestSourceDiscoverSchema = (body: SourceDiscoverSchemaRequestBody) =>
  apiRequest<SourceDiscoverSchemaRead>("POST", "/sources/discover_schema", body);

export const requestCreateSource = (body: SourceCreate) => apiRequest<SourceRead>("POST", "/sources/create", body);

export const requestDeleteSource = (body: SourceIdRequestBody) => apiRequest("POST", "/sources/delete", body, 204);

export const requestDestinationsList = () =>
  apiRequest<DestinationReadList>("POST", "/destinations/list", {
    workspaceId: getWorkspaceId(),
  });

export const requestCreateDestination = (body: DestinationCreate) =>
  apiRequest<DestinationRead>("POST", "/destinations/create", body);

export const requestDeleteDestination = (body: DestinationIdRequestBody) =>
  apiRequest("POST", "/destinations/delete", body, 204);
