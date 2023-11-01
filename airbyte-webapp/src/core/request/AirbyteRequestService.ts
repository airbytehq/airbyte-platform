import { ApiCallOptions } from "../api";

/**
 * @deprecated This class will be removed soon and should no longer be used or extended.
 */
export abstract class AirbyteRequestService {
  public readonly requestOptions: ApiCallOptions;

  constructor(requestOptions: ApiCallOptions) {
    this.requestOptions = requestOptions;
  }
}
