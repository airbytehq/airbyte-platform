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
    },
    specification: emptySpecification,
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
    },
    specification: emptySpecification,
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
