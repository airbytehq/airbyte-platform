import { AirbyteStreamAndConfiguration } from "core/api/types/AirbyteClient";

/**
 * since AirbyteStreamAndConfiguration don't have a unique identifier
 * we need to compare the name and namespace to determine if they are the same stream
 * @param streamNode
 * @param streamName
 * @param streamNamespace
 */
export const isSameSyncStream = (
  streamNode: AirbyteStreamAndConfiguration,
  streamName: string | undefined,
  streamNamespace: string | undefined
) => streamNode.stream?.name === streamName && streamNode.stream?.namespace === streamNamespace;
