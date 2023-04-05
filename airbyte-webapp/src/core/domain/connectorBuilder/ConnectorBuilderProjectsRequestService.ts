import {
  getConnectorBuilderProject,
  createConnectorBuilderProject,
  deleteConnectorBuilderProject,
  listConnectorBuilderProjects,
  updateConnectorBuilderProject,
  publishConnectorBuilderProject,
  listDeclarativeManifests,
  updateDeclarativeManifestVersion,
  createDeclarativeSourceDefinitionManifest,
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

  public changeVersion(workspaceId: string, sourceDefinitionId: string, version: number) {
    return updateDeclarativeManifestVersion({ sourceDefinitionId, version, workspaceId }, this.requestOptions);
  }

  public deleteBuilderProject(workspaceId: string, builderProjectId: string) {
    return deleteConnectorBuilderProject({ workspaceId, builderProjectId }, this.requestOptions);
  }

  public releaseNewVersion(
    workspaceId: string,
    sourceDefinitionId: string,
    description: string,
    version: number,
    useAsActiveVersion: boolean,
    manifest: ConnectorManifest
  ) {
    return createDeclarativeSourceDefinitionManifest(
      {
        workspaceId,
        sourceDefinitionId,
        declarativeManifest: {
          description,
          manifest,
          version,
          spec: {
            documentationUrl: manifest.spec?.documentation_url,
            connectionSpecification: manifest.spec?.connection_specification,
          },
        },
        setAsActiveManifest: useAsActiveVersion,
      },
      this.requestOptions
    );
  }

  public publishBuilderProject(
    workspaceId: string,
    projectId: string,
    name: string,
    description: string,
    manifest: ConnectorManifest,
    version: number
  ) {
    return publishConnectorBuilderProject(
      {
        workspaceId,
        builderProjectId: projectId,
        initialDeclarativeManifest: {
          manifest,
          description,
          spec: {
            documentationUrl: manifest.spec?.documentation_url,
            connectionSpecification: manifest.spec?.connection_specification,
          },
          version,
        },
        name,
      },
      this.requestOptions
    );
  }
}
