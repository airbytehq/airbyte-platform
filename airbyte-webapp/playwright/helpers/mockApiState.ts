import type {
  SourceRead,
  DestinationRead,
  WebBackendConnectionRead,
  WorkspaceRead,
  UserRead,
  SourceDefinitionRead,
  DestinationDefinitionRead,
  AirbyteCatalog,
  SourceConfiguration,
  DestinationConfiguration,
  SourceDefinitionSpecification,
  DestinationDefinitionSpecification,
  DestinationSyncMode,
} from "@src/core/api/types/AirbyteClient";

import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";

/** Connector definitions, specifications and discovered data share one scenario. IDs derive from these seeds. */
export interface MockAirbyteScenario {
  source: {
    definition: SourceDefinitionRead;
    name: string;
    connectionConfiguration: SourceConfiguration;
    specification: SourceDefinitionSpecification;
    catalog: AirbyteCatalog;
  };
  destination: {
    definition: DestinationDefinitionRead;
    name: string;
    connectionConfiguration: DestinationConfiguration;
    specification: DestinationDefinitionSpecification;
    supportedSyncModes: DestinationSyncMode[];
  };
  /** Explicit initial connections; use [] for creation flows. */
  connections: WebBackendConnectionRead[];
}

export function createMockAirbyteState(scenario: MockAirbyteScenario) {
  // Test.use options can be shared across cases; mutable mock state must never be shared.
  const seed = structuredClone(scenario);
  const user: UserRead = {
    userId: "43bcf34b-7e49-424e-a758-3fdd10a73699",
    email: "test@example.com",
    name: "Test User",
    metadata: {},
  };
  const workspace: WorkspaceRead = {
    workspaceId: "47c74b9b-9b89-4af1-8331-4865af6c4e4d",
    customerId: "55dd55e2-33ac-44dc-8d65-5aa7c8624f72",
    organizationId: "db338860-f35c-4b5b-b57f-5171070bdcd9",
    email: user.email,
    name: "Test workspace",
    slug: "test-workspace",
    initialSetupComplete: true,
    displaySetupWizard: false,
    anonymousDataCollection: false,
  };
  const source: SourceRead = {
    sourceId: "b5117460-9237-4fd0-b6a9-45ac22a6db59",
    sourceDefinitionId: seed.source.definition.sourceDefinitionId,
    workspaceId: workspace.workspaceId,
    name: seed.source.name,
    sourceName: seed.source.definition.name,
    connectionConfiguration: seed.source.connectionConfiguration,
    createdAt: 0,
    isDraft: false,
  };
  const destination: DestinationRead = {
    destinationId: "35df745e-6e2c-4e83-b9d1-36f560403d2a",
    destinationDefinitionId: seed.destination.definition.destinationDefinitionId,
    workspaceId: workspace.workspaceId,
    name: seed.destination.name,
    destinationName: seed.destination.definition.name,
    connectionConfiguration: seed.destination.connectionConfiguration,
    createdAt: 0,
    isDraft: false,
  };
  const connectionTemplate: WebBackendConnectionRead = {
    connectionId: "a9c8e4b5-349d-4a17-bdff-5ad2f6fbd611",
    name: `${source.name} → ${destination.name}`,
    sourceId: source.sourceId,
    destinationId: destination.destinationId,
    source,
    destination,
    syncCatalog: seed.source.catalog,
    scheduleType: "manual",
    status: "active",
    isSyncing: false,
    schemaChange: "no_change",
    notifySchemaChanges: true,
    notifySchemaChangesByEmail: false,
    nonBreakingChangesPreference: "ignore",
    sourceActorDefinitionVersion: {
      dockerRepository: seed.source.definition.dockerRepository,
      dockerImageTag: seed.source.definition.dockerImageTag,
      supportsRefreshes: true,
      isVersionOverrideApplied: false,
      supportState: "supported",
      supportsFileTransfer: false,
      supportsDataActivation: false,
    },
    destinationActorDefinitionVersion: {
      dockerRepository: seed.destination.definition.dockerRepository,
      dockerImageTag: seed.destination.definition.dockerImageTag,
      supportsRefreshes: true,
      isVersionOverrideApplied: false,
      supportState: "supported",
      supportsFileTransfer: false,
      supportsDataActivation: false,
    },
    tags: [],
    onDemandEnabled: false,
  };

  const catalog = seed.source.catalog;
  const catalogId = "66aa3e2e-8267-48e3-b79c-78a85279069e";
  const connections = seed.connections;
  const sourceDefinition = seed.source.definition;
  const destinationDefinition = seed.destination.definition;

  return {
    seed,
    user,
    workspace,
    source,
    destination,
    connectionTemplate,
    catalog,
    catalogId,
    connections,
    sourceDefinition,
    destinationDefinition,
  };
}

export type MockAirbyteState = ReturnType<typeof createMockAirbyteState>;

const fakerScenario: MockAirbyteScenario = {
  source: {
    definition: {
      sourceDefinitionId: sourceIds.Faker,
      name: "Faker",
      dockerRepository: "airbyte/source-faker",
      dockerImageTag: "1.0.0",
    },
    name: "Faker source",
    connectionConfiguration: { count: 10, seed: 12345 },
    specification: { type: "object", properties: {} },
    catalog: {
      streams: [
        {
          stream: {
            name: "users",
            jsonSchema: { type: "object", properties: { id: { type: "integer" } } },
            supportedSyncModes: ["full_refresh"],
          },
          config: { syncMode: "full_refresh", destinationSyncMode: "overwrite", selected: true },
        },
      ],
    },
  },
  destination: {
    definition: {
      destinationDefinitionId: destinationIds.EndToEndTesting,
      name: "End-to-End Testing",
      dockerRepository: "airbyte/destination-e2e-test",
      dockerImageTag: "1.0.0",
      documentationUrl: "",
    },
    name: "E2E Testing destination",
    connectionConfiguration: {},
    specification: { type: "object", properties: {} },
    supportedSyncModes: ["overwrite"],
  },
  connections: [],
};

export const fakerExistingConnectionScenario: MockAirbyteScenario = {
  ...fakerScenario,
  connections: [createMockAirbyteState(fakerScenario).connectionTemplate],
};
