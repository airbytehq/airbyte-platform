import type { Handle } from "./handle";
import type {
  DestinationConfiguration,
  DestinationDefinitionRead,
  DestinationDefinitionSpecification,
  DestinationSyncMode,
  SourceConfiguration,
  SourceDefinitionRead,
  SourceDefinitionSpecification,
} from "@src/core/api/types/AirbyteClient";

import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";

import { createHandle } from "./handle";

export interface SourceDefinitionInput {
  definition: SourceDefinitionRead;
  /** JSON schema of the connector configuration (the `connectionSpecification` of the spec). */
  specification: SourceDefinitionSpecification;
}
export type SourceDefinitionHandle = Handle<"sourceDefinition", SourceDefinitionInput>;

export interface DestinationDefinitionInput {
  definition: DestinationDefinitionRead;
  /** JSON schema of the connector configuration (the `connectionSpecification` of the spec). */
  specification: DestinationDefinitionSpecification;
  supportedSyncModes: DestinationSyncMode[];
}
export type DestinationDefinitionHandle = Handle<"destinationDefinition", DestinationDefinitionInput>;

const emptySpecification = { type: "object", properties: {} };

/**
 * The configuration form of source-pokeapi 0.3.75, from oss/airbyte-config/specs/src/main/resources/seed/
 * local_oss_registry.json. The list of Pokémon names is cut to four.
 */
const pokeApiSpecification = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  required: ["pokemon_name"],
  additionalProperties: true,
  properties: {
    pokemon_name: {
      type: "string",
      title: "Pokemon Name",
      description: "Pokemon requested from the API.",
      pattern: "^[a-z0-9_\\-]+$",
      enum: ["bulbasaur", "ivysaur", "venusaur", "ditto"],
      examples: ["ditto", "luxray", "snorlax"],
      order: 0,
    },
  },
};

/**
 * The configuration form of destination-dev-null 0.9.6 ("End-to-End Testing (/dev/null)"), from
 * oss/airbyte-config/specs/src/main/resources/seed/local_oss_registry.json. The test destination types are cut to
 * Logging and Silent, and the logging types are cut to First N Entries.
 */
const e2eTestingSpecification = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "E2E Test Destination Spec",
  type: "object",
  required: ["test_destination"],
  additionalProperties: true,
  properties: {
    test_destination: {
      title: "Test Destination",
      description: "The type of destination to be used",
      type: "object",
      oneOf: [
        {
          title: "Logging",
          type: "object",
          required: ["test_destination_type", "logging_config"],
          additionalProperties: true,
          properties: {
            test_destination_type: { type: "string", enum: ["LOGGING"], default: "LOGGING" },
            logging_config: {
              title: "Logging Configuration",
              description: "Configurate how the messages are logged.",
              type: "object",
              oneOf: [
                {
                  title: "First N Entries",
                  description: "Log first N entries per stream.",
                  type: "object",
                  required: ["logging_type", "max_entry_count"],
                  additionalProperties: true,
                  properties: {
                    logging_type: { type: "string", enum: ["FirstN"], default: "FirstN" },
                    max_entry_count: {
                      title: "N",
                      description: "Number of entries to log. The maximum is 1,000 entries.",
                      type: "number",
                      default: 100,
                      minimum: 1,
                      maximum: 1000,
                      examples: [100],
                    },
                  },
                },
              ],
            },
          },
        },
        {
          title: "Silent",
          type: "object",
          required: ["test_destination_type"],
          additionalProperties: true,
          properties: {
            test_destination_type: { type: "string", enum: ["SILENT"], default: "SILENT" },
          },
        },
      ],
    },
  },
};

const sourceDefinition = (input: SourceDefinitionInput) => createHandle("sourceDefinition", input);
const destinationDefinition = (input: DestinationDefinitionInput) => createHandle("destinationDefinition", input);

/**
 * Connector definitions with their real definition IDs. Specifications are empty until a test needs a form.
 *
 * `documentationUrl` stays empty on purpose: with a URL, the docs panel fetches /docs/*.md in dev builds only
 * (connectorDocumentation.ts), so dev and prod builds would send different requests.
 */
export const defs = {
  pokeApiSource: sourceDefinition({
    definition: {
      sourceDefinitionId: sourceIds.PokeApi,
      name: "PokeAPI",
      dockerRepository: "airbyte/source-pokeapi",
      dockerImageTag: "1.0.0",
      // Community connectors show in the Marketplace tab of the connector list.
      supportLevel: "community",
    },
    specification: pokeApiSpecification,
  }),
  postgresSource: sourceDefinition({
    definition: {
      sourceDefinitionId: sourceIds.Postgres,
      name: "Postgres",
      dockerRepository: "airbyte/source-postgres",
      dockerImageTag: "1.0.0",
    },
    specification: emptySpecification,
  }),
  e2eTestingDestination: destinationDefinition({
    definition: {
      destinationDefinitionId: destinationIds.EndToEndTesting,
      name: "End-to-End Testing",
      dockerRepository: "airbyte/destination-e2e-test",
      dockerImageTag: "1.0.0",
      documentationUrl: "",
      // Community connectors show in the Marketplace tab of the connector list.
      supportLevel: "community",
    },
    specification: e2eTestingSpecification,
    supportedSyncModes: ["overwrite"],
  }),
  postgresDestination: destinationDefinition({
    definition: {
      destinationDefinitionId: destinationIds.Postgres,
      name: "Postgres",
      dockerRepository: "airbyte/destination-postgres",
      dockerImageTag: "1.0.0",
      documentationUrl: "",
    },
    specification: emptySpecification,
    supportedSyncModes: ["overwrite", "append", "append_dedup", "overwrite_dedup"],
  }),
};

export interface SourceInput {
  def: SourceDefinitionHandle;
  name?: string;
  config?: SourceConfiguration;
}
export type SourceHandle = Handle<"source", SourceInput>;

export function source(input: SourceInput): SourceHandle {
  return createHandle("source", input);
}

export interface DestinationInput {
  def: DestinationDefinitionHandle;
  name?: string;
  config?: DestinationConfiguration;
}
export type DestinationHandle = Handle<"destination", DestinationInput>;

export function destination(input: DestinationInput): DestinationHandle {
  return createHandle("destination", input);
}
