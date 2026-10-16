import type { HandlerMap } from "./handler";

import { notFound } from "./handler";
import { isOtherWorkspace } from "./identity";

/**
 * Connector Builder projects and test reads. builderProjects.ts keeps the projects. The result of a test read comes
 * from the scenario (`outcomes.streamRead`), because the server computes it by running the connector.
 */
export const builderHandlers: HandlerMap = {
  getConnectorBuilderCapabilities: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { customCodeExecution: false },

  listConnectorBuilderProjects: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : { projects: [...store.builderProjects.all()] },
  createConnectorBuilderProject: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId)
      ? notFound()
      : { workspaceId: body.workspaceId, builderProjectId: store.builderProjects.create(body.builderProject) },
  getConnectorBuilderProject: ({ body, store }) => store.builderProjects.get(body.builderProjectId) ?? notFound(),
  updateConnectorBuilderProject: ({ body, store }) =>
    store.builderProjects.update(body.builderProjectId, body.builderProject) ? undefined : notFound(),
  updateConnectorBuilderProjectTestingValues: ({ body, store }) =>
    store.builderProjects.setTestingValues(body.builderProjectId, body.testingValues) ?? notFound(),

  /**
   * The server resolves references ($ref, $parameters) in the manifest. The mock returns the manifest unchanged,
   * which is correct for manifests without references, such as the ones the builder form writes.
   */
  resolveManifestBuilderProject: ({ body }) => ({ manifest: body.manifest }),

  readConnectorBuilderProjectStream: ({ body, store }) =>
    store.builderProjects.get(body.builderProjectId) ? store.builderProjects.streamRead() : notFound(),
};
