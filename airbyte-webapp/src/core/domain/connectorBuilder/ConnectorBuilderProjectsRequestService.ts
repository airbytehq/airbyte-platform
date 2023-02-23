import { listConnectorBuilderProjects } from "core/request/AirbyteClient";
import { AirbyteRequestService } from "core/request/AirbyteRequestService";

export class ConnectorBuilderProjectsRequestService extends AirbyteRequestService {
  public list(workspaceId: string) {
    return listConnectorBuilderProjects({ workspaceId }, this.requestOptions);
  }
}
