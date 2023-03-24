import {
  getConnectorBuilderProject,
  createConnectorBuilderProject,
  deleteConnectorBuilderProject,
  listConnectorBuilderProjects,
  updateConnectorBuilderProject,
  publishConnectorBuilderProject,
  listDeclarativeManifests,
} from "core/request/AirbyteClient";
import { AirbyteRequestService } from "core/request/AirbyteRequestService";
import { ConnectorManifest } from "core/request/ConnectorManifest";

export class ConnectorBuilderProjectsRequestService extends AirbyteRequestService {
  public list(workspaceId: string) {
    return listConnectorBuilderProjects({ workspaceId }, this.requestOptions);
  }
  public getConnectorBuilderProject(workspaceId: string, builderProjectId: string, version?: number) {
    return getConnectorBuilderProject({ workspaceId, builderProjectId, version }, this.requestOptions);
  }
  public getConnectorBuilderProjectVersions(workspaceId: string, sourceDefinitionId: string) {
    return listDeclarativeManifests({ workspaceId, sourceDefinitionId }, this.requestOptions);
  }
  public updateBuilderProject(
    workspaceId: string,
    builderProjectId: string,
    name: string,
    draftManifest: ConnectorManifest | undefined
  ) {
    return updateConnectorBuilderProject(
      { workspaceId, builderProjectId, builderProject: { name, draftManifest } },
      this.requestOptions
    );
  }

  public createBuilderProject(workspaceId: string, name: string, draftManifest?: ConnectorManifest) {
    return createConnectorBuilderProject({ workspaceId, builderProject: { name, draftManifest } }, this.requestOptions);
  }

  public deleteBuilderProject(workspaceId: string, builderProjectId: string) {
    return deleteConnectorBuilderProject({ workspaceId, builderProjectId }, this.requestOptions);
  }

  public publishBuilderProject(workspaceId: string, projectId: string, name: string, manifest: ConnectorManifest) {
    return publishConnectorBuilderProject(
      {
        workspaceId,
        builderProjectId: projectId,
        initialDeclarativeManifest: {
          manifest,
          description: "Test release",
          spec: {
            documentationUrl: manifest.spec?.documentation_url,
            connectionSpecification: manifest.spec?.connection_specification,
          },
          version: 1,
        },
        name,
      },
      this.requestOptions
    );
  }
}
