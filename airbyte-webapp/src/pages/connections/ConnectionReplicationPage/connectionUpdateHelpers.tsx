import {
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  CatalogDiff,
  StreamMapperType,
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

    // todo: once mappers ui is rolled out and we are no longer using this field, this can be removed
    const promptBecauseOfHashing = !equal(formStream.config?.hashedFields, connectionStream.config?.hashedFields);

    // Changes that should recommend a refresh/clear are those that change the destination output catalog
    const promptBecauseOfMappingsChanges =
      formStream.config?.mappers?.some((formStreamMapper) => {
        const storedMapper = connectionStream.config?.mappers?.find((m) => m.id === formStreamMapper.id);

        // return true for any new mapping EXCEPT row filtering ones
        if (!storedMapper) {
          return formStreamMapper.type !== StreamMapperType["row-filtering"];
        }

        // mapping type changes will change the suffix or newFieldName... there's an edge case here where if a user decides to add a mapper that changes a
        // hashed field `apple` to a rename mapping that uses the  `apple_hashed` for the newFieldname... the server would not actually do a refresh/clear
        // given the likelihood of that configuration, this should be sufficient for now, though.
        if (storedMapper.type !== formStreamMapper.type) {
          return true;
        }

        // return true if a hashing or encryption mapping has a new target field
        if (
          (formStreamMapper.type === StreamMapperType.hashing ||
            formStreamMapper.type === StreamMapperType.encryption) &&
          "targetField" in formStreamMapper.mapperConfiguration &&
          "targetField" in storedMapper.mapperConfiguration &&
          formStreamMapper.mapperConfiguration.targetField !== storedMapper.mapperConfiguration.targetField
        ) {
          return true;
        }

        // return true if a field renaming mapping changes either its original or new field name
        if (
          formStreamMapper.type === StreamMapperType["field-renaming"] &&
          (("originalFieldName" in formStreamMapper.mapperConfiguration &&
            "originalFieldName" in storedMapper.mapperConfiguration &&
            formStreamMapper.mapperConfiguration.originalFieldName !==
              storedMapper.mapperConfiguration.originalFieldName) ||
            ("newFieldName" in formStreamMapper.mapperConfiguration &&
              "newFieldName" in storedMapper.mapperConfiguration &&
              formStreamMapper.mapperConfiguration.newFieldName !== storedMapper.mapperConfiguration.newFieldName))
        ) {
          return true;
        }

        return false;
      }) ||
      connectionStream.config?.mappers?.some((storedMapper) => {
        // return true for any removed mapping EXCEPT row filtering ones
        return (
          !formStream.config?.mappers?.find((m) => m.id === storedMapper.id) &&
          storedMapper.type !== StreamMapperType["row-filtering"]
        );
      });

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

  const shouldRecommendRefresh = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);

  const shouldTrackAction = hasUserChangesInEnabledStreams || hasCatalogDiffInEnabledStream;

  return {
    shouldRecommendRefresh,
    shouldTrackAction,
  };
};
