import {
  getConnectorBuilderProject,
  listConnectorBuilderProjects,
  updateConnectorBuilderProject,
} from "core/request/AirbyteClient";
import { AirbyteRequestService } from "core/request/AirbyteRequestService";
import { ConnectorManifest } from "core/request/ConnectorManifest";

export class ConnectorBuilderProjectsRequestService extends AirbyteRequestService {
  public list(workspaceId: string) {
    return listConnectorBuilderProjects({ workspaceId }, this.requestOptions);
  }
  public getConnectorBuilderProject(workspaceId: string, builderProjectId: string) {
    return getConnectorBuilderProject({ workspaceId, builderProjectId }, this.requestOptions);
  }
  public updateBuilderProject(
    workspaceId: string,
    builderProjectId: string,
    name: string,
    draftManifest: ConnectorManifest
  ) {
    return updateConnectorBuilderProject(
      { workspaceId, builderProjectId, builderProject: { name, draftManifest } },
      this.requestOptions
    );
  }
}
