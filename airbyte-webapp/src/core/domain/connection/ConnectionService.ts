import {
  deleteConnection,
  resetConnection,
  syncConnection,
  getState,
  getStateType,
  ConnectionStream,
  resetConnectionStream,
  createOrUpdateStateSafe,
  ConnectionState,
} from "../../request/AirbyteClient";
import { AirbyteRequestService } from "../../request/AirbyteRequestService";

export class ConnectionService extends AirbyteRequestService {
  public sync(connectionId: string) {
    return syncConnection({ connectionId }, this.requestOptions);
  }

  public reset(connectionId: string) {
    return resetConnection({ connectionId }, this.requestOptions);
  }

  public resetStream(connectionId: string, streams: ConnectionStream[]) {
    return resetConnectionStream({ connectionId, streams }, this.requestOptions);
  }

  public delete(connectionId: string) {
    return deleteConnection({ connectionId }, this.requestOptions);
  }

  public getState(connectionId: string) {
    return getState({ connectionId }, this.requestOptions);
  }

  public getStateType(connectionId: string) {
    return getStateType({ connectionId }, this.requestOptions);
  }

  public createOrUpdateState(connectionId: string, state: ConnectionState) {
    return createOrUpdateStateSafe({ connectionId, connectionState: state }, this.requestOptions);
  }
}
