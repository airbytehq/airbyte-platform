import { StreamsTablePageObjectBase } from "./StreamsTableContainerPageObject";
import { StreamRowPageObject } from "./StreamRowPageObject";

export class NewStreamsTablePageObject extends StreamsTablePageObjectBase {
  getRow(namespace: string, streamName: string) {
    return new StreamRowPageObject(namespace, streamName);
  }
}
