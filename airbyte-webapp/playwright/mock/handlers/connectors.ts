import type { HandlerMap } from "./handler";

import { notFound } from "./handler";
import { isOtherWorkspace } from "./identity";
import { actorDefinitionVersion } from "../store";

/** A finished spec job. The server attaches one to every specification read. */
const specJobInfo = { id: "spec-job", configType: "get_spec", createdAt: 0, endedAt: 0, succeeded: true } as const;

/** Sources, destinations, and their definitions, versions and specifications. */
export const connectorHandlers: HandlerMap = {
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
