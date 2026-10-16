import { APIRequestContext } from "@playwright/test";
import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";
import { ActorListCursorPaginatedRequestBody } from "@src/core/api/generated/AirbyteClient.schemas";
import { SourceRead, DestinationRead } from "@src/core/api/types/AirbyteClient";

import { getApiBaseUrl } from "./api";

// This file contains helper methods for connector CRUD operations

// Connector configuration definitions for test connectors
const CONNECTOR_CONFIGS = {
  source: {
    pokeapi: {
      endpoint: "sources",
      definitionId: sourceIds.PokeApi,
      idField: "sourceId",
      responseKey: "sources",
      defaultConfig: {
        pokemon_name: "venusaur",
      },
    },
  },
  destination: {
    e2etesting: {
      endpoint: "destinations",
      definitionId: destinationIds.EndToEndTesting,
      idField: "destinationId",
      responseKey: "destinations",
      defaultConfig: {
        test_destination: {
          test_destination_type: "LOGGING",
          logging_config: {
            logging_type: "FirstN",
            max_entry_count: 100,
          },
        },
      },
    },
  },
} as const;

// One connector configuration of CONNECTOR_CONFIGS. Sources and destinations have no connector name in common, so the
// lookup by name needs a type that covers both.
type Configs = typeof CONNECTOR_CONFIGS;
type ConnectorConfig =
  | Configs["source"][keyof Configs["source"]]
  | Configs["destination"][keyof Configs["destination"]];

// Create a connector API interface for the specified connector type
// This factory function generates type-safe API methods for sources or destinations
export const createConnectorAPI = (connectorType: keyof typeof CONNECTOR_CONFIGS, connectorName?: string) => {
  // Default to backwards compatible connectors
  const defaultConnector = connectorType === "source" ? "pokeapi" : "e2etesting";
  const selectedConnector = connectorName || defaultConnector;
  const config = (CONNECTOR_CONFIGS[connectorType] as Record<string, ConnectorConfig | undefined>)[selectedConnector];
  if (!config) {
    throw new Error(`Unknown ${connectorType} connector "${selectedConnector}"`);
  }
  const apiBaseUrl = getApiBaseUrl();

  return {
    create: async (
      request: APIRequestContext,
      name: string,
      workspaceId: string
    ): Promise<SourceRead | DestinationRead> => {
      const createData = {
        name,
        [`${connectorType}DefinitionId`]: config.definitionId,
        workspaceId,
        connectionConfiguration: config.defaultConfig,
      };

      const response = await request.post(`${apiBaseUrl}/${config.endpoint}/create`, {
        data: createData,
      });

      if (!response.ok()) {
        throw new Error(`Failed to create ${connectorType}: ${response.status()}`);
      }

      const result = await response.json();
      return result as SourceRead | DestinationRead;
    },

    // Delete a connector via API
    delete: async (request: APIRequestContext, connectorId: string) => {
      const deleteRequest = {
        [config.idField]: connectorId,
      };

      const response = await request.post(`${apiBaseUrl}/${config.endpoint}/delete`, {
        data: deleteRequest,
        timeout: 30_000,
      });

      if (!response.ok()) {
        throw new Error(`Failed to delete ${connectorType}: ${response.status()}`);
      }
    },

    // List existing connectors for a workspace
    list: async (request: APIRequestContext, workspaceId: string): Promise<Array<SourceRead | DestinationRead>> => {
      try {
        const listRequest: ActorListCursorPaginatedRequestBody = {
          workspaceId,
        };

        const response = await request.post(`${apiBaseUrl}/${config.endpoint}/list`, {
          data: listRequest,
        });

        if (response.ok()) {
          const result = await response.json();
          return result[config.responseKey] as Array<SourceRead | DestinationRead>;
        }
      } catch (error) {
        // Warn on listing errors
        console.warn(`⚠️ Failed to list ${connectorType}s:`, error);
      }
      return [];
    },
  };
};

// Preconfigured connector APIs used in tests. Example usage:
// await pokeSourceAPI.create(request, name, workspaceId)
// await pokeSourceAPI.delete(request, sourceId)
// await e2eDestinationAPI.list(request, workspaceId)

export const pokeSourceAPI = createConnectorAPI("source", "pokeapi");
export const e2eDestinationAPI = createConnectorAPI("destination", "e2etesting");
