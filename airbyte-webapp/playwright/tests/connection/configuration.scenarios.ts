import type { MockAirbyteScenario } from "../../helpers/mockApiState";

import { randomUUID } from "node:crypto";

import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";

import { createMockAirbyteState, fakerExistingConnectionScenario } from "../../helpers/mockApiState";
import { createMockPostgresCatalog, createMockPokeApiCatalog } from "../../helpers/mocks";

export { fakerExistingConnectionScenario };

const postgresScenario: MockAirbyteScenario = {
  source: {
    definition: {
      sourceDefinitionId: sourceIds.Postgres,
      name: "Postgres",
      dockerRepository: "airbyte/source-postgres",
      dockerImageTag: "1.0.0",
    },
    name: "Postgres source",
    connectionConfiguration: {},
    specification: { type: "object", properties: {} },
    catalog: {
      streams: createMockPostgresCatalog().streams.map((stream) => ({
        ...stream,
        config: { ...stream.config!, selected: true },
      })),
    },
  },
  destination: {
    definition: {
      destinationDefinitionId: destinationIds.Postgres,
      name: "Postgres",
      dockerRepository: "airbyte/destination-postgres",
      dockerImageTag: "1.0.0",
      documentationUrl: "",
    },
    name: "Postgres destination",
    connectionConfiguration: {},
    specification: { type: "object", properties: {} },
    supportedSyncModes: ["overwrite", "append", "append_dedup"],
  },
  connections: [],
};
export const postgresConnectionScenario: MockAirbyteScenario = {
  ...postgresScenario,
  connections: [
    { ...createMockAirbyteState(postgresScenario).connectionTemplate, namespaceDefinition: "destination", prefix: "" },
  ],
};
const pokeScenario: MockAirbyteScenario = {
  source: {
    definition: {
      sourceDefinitionId: sourceIds.PokeApi,
      name: "PokeAPI",
      dockerRepository: "airbyte/source-pokeapi",
      dockerImageTag: "1.0.0",
    },
    name: "PokeAPI source",
    connectionConfiguration: { pokemon_name: "ditto" },
    specification: { type: "object", properties: {} },
    catalog: {
      streams: createMockPokeApiCatalog().streams.map((stream) => ({
        ...stream,
        config: { syncMode: "full_refresh", destinationSyncMode: "overwrite", selected: true },
      })),
    },
  },
  destination: fakerExistingConnectionScenario.destination,
  connections: [],
};
export const pokeConnectionScenario: MockAirbyteScenario = {
  ...pokeScenario,
  connections: [
    { ...createMockAirbyteState(pokeScenario).connectionTemplate, namespaceDefinition: "source", prefix: "" },
  ],
};
const pokePostgresScenario: MockAirbyteScenario = {
  ...pokeScenario,
  destination: postgresScenario.destination,
  connections: [],
};
export const pokePostgresConnectionScenario: MockAirbyteScenario = {
  ...pokePostgresScenario,
  connections: [
    {
      ...createMockAirbyteState(pokePostgresScenario).connectionTemplate,
      namespaceDefinition: "destination",
      prefix: "",
    },
  ],
};

export const deletionScenario: MockAirbyteScenario = {
  ...pokeConnectionScenario,
  connections: [
    pokeConnectionScenario.connections[0],
    { ...pokeConnectionScenario.connections[0], connectionId: randomUUID(), name: "Another connection" },
  ],
};

export const customNamespaceScenario: MockAirbyteScenario = {
  ...pokePostgresConnectionScenario,
  connections: [
    {
      ...pokePostgresConnectionScenario.connections[0],
      namespaceDefinition: "customformat",
      namespaceFormat: `\${SOURCE_NAMESPACE}_test`,
    },
  ],
};

export const prefixedConnectionScenario: MockAirbyteScenario = {
  ...pokePostgresConnectionScenario,
  connections: [{ ...pokePostgresConnectionScenario.connections[0], prefix: "auto_test" }],
};

export const deletedConnectionScenario: MockAirbyteScenario = {
  ...deletionScenario,
  connections: [{ ...deletionScenario.connections[0], status: "deprecated" }, deletionScenario.connections[1]],
};

export const disabledConnectionScenario: MockAirbyteScenario = {
  ...fakerExistingConnectionScenario,
  connections: [{ ...fakerExistingConnectionScenario.connections[0], status: "inactive" }],
};
