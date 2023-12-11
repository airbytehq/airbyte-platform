import { AirbyteRequestService } from "core/request/AirbyteRequestService";

import {
  createDestination,
  deleteDestination,
  DestinationCreate,
  DestinationUpdate,
  getDestination,
  listDestinationsForWorkspace,
  updateDestination,
} from "../../request/AirbyteClient";

export class DestinationService extends AirbyteRequestService {
  public get(destinationId: string) {
    return getDestination({ destinationId }, this.requestOptions);
  }

  public list(workspaceId: string) {
    return listDestinationsForWorkspace({ workspaceId }, this.requestOptions);
  }

  public create(body: DestinationCreate) {
    return createDestination(body, this.requestOptions);
  }

  public update(body: DestinationUpdate) {
    return updateDestination(body, this.requestOptions);
  }

  public delete(destinationId: string) {
    return deleteDestination({ destinationId }, this.requestOptions);
  }
}
