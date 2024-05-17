import {
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  CatalogDiff,
  ConnectionStream,
} from "core/api/types/AirbyteClient";
import { equal } from "core/utils/objects";

const createLookupById = (catalog: AirbyteCatalog) => {
  const getStreamId = (stream: AirbyteStreamAndConfiguration) => {
    return `${stream.stream?.namespace ?? ""}-${stream.stream?.name}`;
  };

  return catalog.streams.reduce<Record<string, AirbyteStreamAndConfiguration>>((agg, stream) => {
    agg[getStreamId(stream)] = stream;
    return agg;
  }, {});
};

export const calculateStreamsToRefresh = (formSyncCatalog: AirbyteCatalog, storedSyncCatalog: AirbyteCatalog) => {
  const lookupConnectionValuesStreamById = createLookupById(storedSyncCatalog);

  return formSyncCatalog.streams.reduce((acc, streamNode) => {
    if (streamNode.config?.selected) {
      const formStream = structuredClone(streamNode);
      const connectionStream = structuredClone(
        lookupConnectionValuesStreamById[`${formStream.stream?.namespace ?? ""}-${formStream.stream?.name}`]
      );

      const syncModeChangesSuggestRefresh =
        formStream.config?.syncMode === "incremental" &&
        connectionStream.config?.syncMode === "full_refresh" &&
        connectionStream.config?.destinationSyncMode === "overwrite";

      const selectedFieldsChangesSuggestRefresh =
        connectionStream.config?.syncMode === "incremental" &&
        formStream.config?.syncMode === "incremental" &&
        !equal(formStream.config?.selectedFields, connectionStream.config?.selectedFields);

      const pkChangeSuggestsRefresh =
        formStream.config?.syncMode === "incremental" &&
        connectionStream.config?.syncMode === "incremental" &&
        !equal(formStream.config?.primaryKey, connectionStream.config?.primaryKey);

      const cursorFieldChangeSuggestsRefresh =
        formStream.config?.syncMode === "incremental" &&
        connectionStream.config?.syncMode === "incremental" &&
        !equal(formStream.config?.cursorField, connectionStream.config?.cursorField);

      if (
        syncModeChangesSuggestRefresh ||
        selectedFieldsChangesSuggestRefresh ||
        pkChangeSuggestsRefresh ||
        cursorFieldChangeSuggestsRefresh
      ) {
        acc.push({
          streamName: streamNode.stream?.name ?? "",
          streamNamespace: streamNode.stream?.namespace,
        });
      }
    }

    return acc;
  }, [] as ConnectionStream[]);
};

export const recommendActionOnConnectionUpdate = ({
  catalogDiff,
  formSyncCatalog,
  storedSyncCatalog,
}: {
  catalogDiff?: CatalogDiff;
  formSyncCatalog: AirbyteCatalog;
  storedSyncCatalog: AirbyteCatalog;
}) => {
  const hasCatalogDiffInEnabledStream = catalogDiff?.transforms.some(({ streamDescriptor }) => {
    const stream = formSyncCatalog.streams.find(
      ({ stream }) => streamDescriptor.name === stream?.name && streamDescriptor.namespace === stream.namespace
    );
    return stream?.config?.selected;
  });

  const hasUserChangesInEnabledStreams = !equal(
    formSyncCatalog.streams.filter((s) => s.config?.selected),
    storedSyncCatalog.streams.filter((s) => s.config?.selected)
  );

  // Using the new function to calculate streams to refresh
  const streamsToRefresh = calculateStreamsToRefresh(formSyncCatalog, storedSyncCatalog);

  const shouldTrackAction = hasUserChangesInEnabledStreams || hasCatalogDiffInEnabledStream;

  return {
    streamsToRefresh,
    shouldTrackAction,
  };
};
