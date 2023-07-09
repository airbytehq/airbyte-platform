import { Connection, SyncCatalogStream, SyncCatalogStreamConfig } from "commands/api/types";

interface ModifySyncCatalogStreamParams {
  connection: Connection;
  namespace: string;
  streamName: string;
  modifyStream?: (stream: SyncCatalogStream) => SyncCatalogStream;
  modifyConfig?: (stream: SyncCatalogStreamConfig) => SyncCatalogStreamConfig;
}

export const modifySyncCatalogStream = ({
  connection,
  namespace,
  streamName,
  modifyConfig,
  modifyStream,
}: ModifySyncCatalogStreamParams): Connection => {
  const streams = connection.syncCatalog.streams.map((stream) => {
    if (stream.stream.namespace === namespace && stream.stream.name === streamName) {
      return {
        ...stream,
        stream: modifyStream?.(stream.stream) ?? stream.stream,
        config: modifyConfig?.(stream.config) ?? stream.config,
      };
    }

    return stream;
  });

  return {
    ...connection,
    syncCatalog: {
      ...connection.syncCatalog,
      streams,
    },
  };
};
