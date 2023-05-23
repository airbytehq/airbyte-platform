import { ConnectorIds } from "@src/utils/connectors";

import { ConnectionCreateRequestBody } from "./types";
import { getWorkspaceId } from "./workspace";

type RequiredConnectionCreateRequestProps = "name" | "sourceId" | "destinationId" | "syncCatalog" | "sourceCatalogId";
type CreationConnectRequestParams = Pick<ConnectionCreateRequestBody, RequiredConnectionCreateRequestProps> &
  Partial<Omit<ConnectionCreateRequestBody, RequiredConnectionCreateRequestProps>>;

export const getConnectionCreateRequest = (params: CreationConnectRequestParams): ConnectionCreateRequestBody => ({
  geography: "auto",
  namespaceDefinition: "source",
  namespaceFormat: "${SOURCE_NAMESPACE}",
  nonBreakingChangesPreference: "ignore",
  operations: [],
  prefix: "",
  scheduleType: "manual",
  status: "active",
  ...params,
});

export const getPostgresCreateSourceBody = (name: string) => ({
  name,
  sourceDefinitionId: ConnectorIds.Sources.Postgres,
  workspaceId: getWorkspaceId(),
  connectionConfiguration: {
    ssl_mode: { mode: "disable" },
    tunnel_method: { tunnel_method: "NO_TUNNEL" },
    replication_method: { method: "Standard" },
    ssl: false,
    port: 5433,
    schemas: ["public"],
    host: "localhost",
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

export const getPostgresCreateDestinationBody = (name: string) => ({
  name,
  workspaceId: getWorkspaceId(),
  destinationDefinitionId: ConnectorIds.Destinations.Postgres,
  connectionConfiguration: {
    ssl_mode: { mode: "disable" },
    tunnel_method: { tunnel_method: "NO_TUNNEL" },
    ssl: false,
    port: 5434,
    schema: "public",
    host: "localhost",
    database: "airbyte_ci_destination",
    username: "postgres",
    password: "secret_password",
  },
});
