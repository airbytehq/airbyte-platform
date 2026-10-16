import type { HandlerMap } from "./handler";
import type { Store } from "../store";

import { notFound } from "./handler";
import { isOtherWorkspace } from "./identity";
import { actorDefinitionVersion } from "../store";

/** A finished spec job. The server attaches one to every specification read. */
const specJobInfo = { id: "spec-job", configType: "get_spec", createdAt: 0, endedAt: 0, succeeded: true } as const;

/**
 * Deletes a source or a destination. The server answers 204, and reads and lists no longer find it. The server also
 * deletes the connections of the source or destination (SourceHandler.kt, DestinationHandler.kt). The mock refuses to
 * delete one that has connections (a handler error), because the pages of a deleted connection still read its source
 * and destination.
 */
function deleteActor(store: Store, kind: "source" | "destination", actorId: string) {
  const collection = kind === "source" ? store.sources : store.destinations;
  if (!collection.find(actorId)) {
    return notFound();
  }
  if (store.connections.all().some(({ sourceId, destinationId }) => [sourceId, destinationId].includes(actorId))) {
    throw new Error(`The mock does not delete ${kind} ${actorId}, because it has connections`);
  }
  collection.remove(actorId);
  return undefined;
}

/** Sources, destinations, and their definitions, versions and specifications. */
export const connectorHandlers: HandlerMap = {
  listSourcesForWorkspace: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { sources: [...store.sources.all()] },
  getSource: ({ body, store }) => store.sources.find(body.sourceId) ?? notFound(),
  updateSource: ({ body, store }) =>
    store.sources.find(body.sourceId)
      ? store.sources.update(body.sourceId, { name: body.name, connectionConfiguration: body.connectionConfiguration })
      : notFound(),
  deleteSource: ({ body, store }) => deleteActor(store, "source", body.sourceId),

  /** The create page saves a draft source (`createAsDraft: true`) before it checks the connection. */
  createSource: ({ body, store }) => {
    const definition = store.definitions.source(body.sourceDefinitionId);
    if (!definition || isOtherWorkspace(store, body.workspaceId)) {
      return notFound();
    }
    const sourceId = store.sources.nextId();
    return store.sources.add(sourceId, {
      sourceId,
      sourceDefinitionId: body.sourceDefinitionId,
      workspaceId: body.workspaceId,
      name: body.name,
      sourceName: definition.definition.name,
      connectionConfiguration: body.connectionConfiguration,
      createdAt: 0,
      isDraft: body.createAsDraft ?? false,
      isEntitled: true,
    });
  },

  listDestinationsForWorkspace: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { destinations: [...store.destinations.all()] },
  getDestination: ({ body, store }) => store.destinations.find(body.destinationId) ?? notFound(),
  updateDestination: ({ body, store }) =>
    store.destinations.find(body.destinationId)
      ? store.destinations.update(body.destinationId, {
          name: body.name,
          connectionConfiguration: body.connectionConfiguration,
        })
      : notFound(),
  deleteDestination: ({ body, store }) => deleteActor(store, "destination", body.destinationId),

  /** The create page saves a draft destination (`createAsDraft: true`) before it checks the connection. */
  createDestination: ({ body, store }) => {
    const definition = store.definitions.destination(body.destinationDefinitionId);
    if (!definition || isOtherWorkspace(store, body.workspaceId)) {
      return notFound();
    }
    const destinationId = store.destinations.nextId();
    return store.destinations.add(destinationId, {
      destinationId,
      destinationDefinitionId: body.destinationDefinitionId,
      workspaceId: body.workspaceId,
      name: body.name,
      destinationName: definition.definition.name,
      connectionConfiguration: body.connectionConfiguration,
      createdAt: 0,
      isDraft: body.createAsDraft ?? false,
      isEntitled: true,
    });
  },

  /** Enterprise connectors are not in the OSS marketplace. */
  listEnterpriseSourceStubsForWorkspace: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { enterpriseConnectorStubs: [] },
  listEnterpriseDestinationStubsForWorkspace: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { enterpriseConnectorStubs: [] },
  getSourceDefinitionSpecification: ({ body, store }) => {
    const definition = store.definitions.source(body.sourceDefinitionId);
    if (!definition || isOtherWorkspace(store, body.workspaceId)) {
      return notFound();
    }
    return {
      sourceDefinitionId: body.sourceDefinitionId,
      documentationUrl: definition.definition.documentationUrl,
      connectionSpecification: definition.specification,
      jobInfo: specJobInfo,
    };
  },
  getDestinationDefinitionSpecification: ({ body, store }) => {
    const definition = store.definitions.destination(body.destinationDefinitionId);
    if (!definition || isOtherWorkspace(store, body.workspaceId)) {
      return notFound();
    }
    return {
      destinationDefinitionId: body.destinationDefinitionId,
      documentationUrl: definition.definition.documentationUrl,
      connectionSpecification: definition.specification,
      supportedDestinationSyncModes: definition.supportedSyncModes,
      jobInfo: specJobInfo,
    };
  },
  getConnectorDocumentation: ({ body, store }) => {
    const definition =
      body.actorType === "source"
        ? store.definitions.source(body.actorDefinitionId)?.definition
        : store.definitions.destination(body.actorDefinitionId)?.definition;
    return definition && !isOtherWorkspace(store, body.workspaceId) ? { doc: `# ${definition.name}` } : notFound();
  },
  /** No definition in the mock was published from a Connector Builder project. */
  getConnectorBuilderProjectIdForDefinitionId: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { builderProjectId: null },

  listSourceDefinitionsForWorkspace: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId)
      ? notFound()
      : { sourceDefinitions: store.definitions.allSources().map(({ definition }) => definition) },
  listDestinationDefinitionsForWorkspace: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId)
      ? notFound()
      : { destinationDefinitions: store.definitions.allDestinations().map(({ definition }) => definition) },
  getSourceDefinitionForWorkspace: ({ body, store }) => {
    const definition = store.definitions.source(body.sourceDefinitionId);
    return definition && !isOtherWorkspace(store, body.workspaceId) ? definition.definition : notFound();
  },
  getDestinationDefinitionForWorkspace: ({ body, store }) => {
    const definition = store.definitions.destination(body.destinationDefinitionId);
    return definition && !isOtherWorkspace(store, body.workspaceId) ? definition.definition : notFound();
  },

  getActorDefinitionVersionForSourceId: ({ body, store }) => {
    const source = store.sources.find(body.sourceId);
    const definition = source && store.definitions.source(source.sourceDefinitionId);
    return definition ? actorDefinitionVersion(definition.definition) : notFound();
  },
  getActorDefinitionVersionForDestinationId: ({ body, store }) => {
    const destination = store.destinations.find(body.destinationId);
    const definition = destination && store.definitions.destination(destination.destinationDefinitionId);
    return definition ? actorDefinitionVersion(definition.definition) : notFound();
  },

  getSpecificationForSourceId: ({ body, store }) => {
    const source = store.sources.find(body.sourceId);
    const definition = source && store.definitions.source(source.sourceDefinitionId);
    if (!source || !definition) {
      return notFound();
    }
    return {
      sourceDefinitionId: source.sourceDefinitionId,
      documentationUrl: definition.definition.documentationUrl,
      connectionSpecification: definition.specification,
      jobInfo: specJobInfo,
    };
  },
  getSpecificationForDestinationId: ({ body, store }) => {
    const destination = store.destinations.find(body.destinationId);
    const definition = destination && store.definitions.destination(destination.destinationDefinitionId);
    if (!destination || !definition) {
      return notFound();
    }
    return {
      destinationDefinitionId: destination.destinationDefinitionId,
      documentationUrl: definition.definition.documentationUrl,
      connectionSpecification: definition.specification,
      supportedDestinationSyncModes: definition.supportedSyncModes,
      jobInfo: specJobInfo,
    };
  },
};
