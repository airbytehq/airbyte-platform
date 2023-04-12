import { getDestinationDefinitionSpecification, getSpecificationForDestinationId } from "../../request/AirbyteClient";
import { AirbyteRequestService } from "../../request/AirbyteRequestService";

export class DestinationDefinitionSpecificationService extends AirbyteRequestService {
  public get(destinationDefinitionId: string, workspaceId: string) {
    return getDestinationDefinitionSpecification({ destinationDefinitionId, workspaceId }, this.requestOptions);
  }

  public getForDestination(destinationId: string) {
    return getSpecificationForDestinationId({ destinationId }, this.requestOptions);
  }
}
