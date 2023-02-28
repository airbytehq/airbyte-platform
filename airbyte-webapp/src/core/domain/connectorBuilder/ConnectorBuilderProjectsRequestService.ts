import { createConnectorBuilderProject, listConnectorBuilderProjects } from "core/request/AirbyteClient";
import { AirbyteRequestService } from "core/request/AirbyteRequestService";
import { ConnectorManifest } from "core/request/ConnectorManifest";

export class ConnectorBuilderProjectsRequestService extends AirbyteRequestService {
  public list(workspaceId: string) {
    return listConnectorBuilderProjects({ workspaceId }, this.requestOptions);
  }

  public createBuilderProject(workspaceId: string, name: string, draftManifest?: ConnectorManifest) {
    return createConnectorBuilderProject({ workspaceId, builderProject: { name, draftManifest } }, this.requestOptions);
  }
}
