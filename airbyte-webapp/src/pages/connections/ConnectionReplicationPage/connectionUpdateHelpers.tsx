import { AirbyteCatalog, AirbyteStreamAndConfiguration, CatalogDiff } from "core/api/types/AirbyteClient";
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

export const determineRecommendRefresh = (formSyncCatalog: AirbyteCatalog, storedSyncCatalog: AirbyteCatalog) => {
  const lookupConnectionValuesStreamById = createLookupById(storedSyncCatalog);

  return formSyncCatalog.streams.some((streamNode) => {
    if (!streamNode.config?.selected) {
      return false;
    }

    const formStream = structuredClone(streamNode);
    const connectionStream = structuredClone(
      lookupConnectionValuesStreamById[`${formStream.stream?.namespace ?? ""}-${formStream.stream?.name}`]
    );

    const promptBecauseOfSyncModes =
      // if we changed the stream to incremental from full refresh overwrite
      (formStream.config?.syncMode === "incremental" &&
        connectionStream.config?.syncMode === "full_refresh" &&
        connectionStream.config?.destinationSyncMode === "overwrite") ||
      (connectionStream.config?.syncMode === "incremental" &&
        formStream.config?.syncMode === "incremental" &&
        // if it was + is incremental but we change the selected fields, pk, or cursor
        (!equal(formStream.config?.selectedFields, connectionStream.config?.selectedFields) ||
          !equal(formStream.config?.primaryKey, connectionStream.config?.primaryKey) ||
          !equal(formStream.config?.cursorField, connectionStream.config?.cursorField)));

    const promptBecauseOfHashing = !equal(formStream.config?.hashedFields, connectionStream.config?.hashedFields);

    const promptBecauseOfMappingsChanges =
      connectionStream.config?.mappers &&
      connectionStream.config.mappers.length > 0 &&
      !equal(formStream.config?.mappers, connectionStream.config?.mappers);

    return promptBecauseOfSyncModes || promptBecauseOfHashing || promptBecauseOfMappingsChanges;
  });
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
  const shouldRecommendRefresh = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);

  const shouldTrackAction = hasUserChangesInEnabledStreams || hasCatalogDiffInEnabledStream;

  return {
    shouldRecommendRefresh,
    shouldTrackAction,
  };
};
