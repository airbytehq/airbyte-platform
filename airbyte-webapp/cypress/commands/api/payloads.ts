import { ConnectorIds } from "@src/area/connector/utils/constants";
import {
  WebBackendConnectionCreate,
  DestinationCreate,
  SourceCreate,
  WebBackendConnectionUpdate,
} from "@src/core/api/types/AirbyteClient";

import { getWorkspaceId } from "./workspace";

export const getConnectionCreateRequest = (params: WebBackendConnectionCreate): WebBackendConnectionCreate => ({
  geography: "auto",
  namespaceDefinition: "source",
  namespaceFormat: "${SOURCE_NAMESPACE}",
  nonBreakingChangesPreference: "ignore",
  operations: [],
  prefix: "",
  scheduleType: "manual",
  ...params,
});

export const getPostgresCreateSourceBody = (name: string): SourceCreate => ({
  name,
  sourceDefinitionId: ConnectorIds.Sources.Postgres,
  workspaceId: getWorkspaceId(),
  connectionConfiguration: {
    ssl_mode: { mode: "disable" },
    tunnel_method: { tunnel_method: "NO_TUNNEL" },
    replication_method: { method: "Standard" },
    ssl: false,
    port: Cypress.env("SOURCE_DB_PORT") || 5433,
    schemas: ["public"],
    host: Cypress.env("SOURCE_DB_HOST") || "localhost",
    database: "airbyte_ci_source",
    username: "postgres",
    password: "secret_password",
  },
});

export const getE2ETestingCreateDestinationBody = (name: string) => ({
  name,
  workspaceId: getWorkspaceId(),
  destinationDefinitionId: ConnectorIds.Destinations.E2ETesting,
  connectionConfiguration: {
    type: "LOGGING",
    logging_config: {
      logging_type: "FirstN",
      max_entry_count: 100,
    },
  },
});

export const getPostgresCreateDestinationBody = (name: string): DestinationCreate => ({
  name,
  workspaceId: getWorkspaceId(),
  destinationDefinitionId: ConnectorIds.Destinations.Postgres,
  connectionConfiguration: {
    ssl_mode: { mode: "disable" },
    tunnel_method: { tunnel_method: "NO_TUNNEL" },
    ssl: false,
    port: Cypress.env("DESTINATION_DB_PORT") || 5434,
    schema: "public",
    host: Cypress.env("DESTINATION_DB_HOST") || "localhost",
    database: "airbyte_ci_destination",
    username: "postgres",
    password: "secret_password",
  },
});

export const getPokeApiCreateSourceBody = (sourceName: string, pokeName: string): SourceCreate => ({
  name: sourceName,
  workspaceId: getWorkspaceId(),
  sourceDefinitionId: ConnectorIds.Sources.PokeApi,
  connectionConfiguration: { pokemon_name: pokeName },
});

export const getLocalJSONCreateDestinationBody = (name: string): DestinationCreate => ({
  name,
  destinationDefinitionId: ConnectorIds.Destinations.LocalJson,
  workspaceId: getWorkspaceId(),
  connectionConfiguration: {
    destination_path: "/local",
  },
});

export const getPostgresToPostgresUpdateConnectionBody = (
  connectionId: string,
  updateParams: Partial<WebBackendConnectionUpdate>
): WebBackendConnectionUpdate => ({
  ...updateParams,
  connectionId,
});
