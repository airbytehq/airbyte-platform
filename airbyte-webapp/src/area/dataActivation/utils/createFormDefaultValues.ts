import {
  AirbyteCatalog,
  AirbyteStreamConfiguration,
  ConfiguredStreamMapper,
  FieldRenamingMapperConfiguration,
  StreamMapperType,
} from "core/api/types/AirbyteClient";

import { DataActivationField, DataActivationStream, DataActivationConnectionFormValues } from "../types";

export const createFormDefaultValues = (syncCatalog: AirbyteCatalog): DataActivationConnectionFormValues => {
  return {
    streams: syncCatalog.streams
      .filter((stream) => {
        return stream.config?.selected;
      })
      .map((stream) => {
        if (!stream.stream) {
          throw new Error("Stream is undefined");
        }
        if (!stream.config) {
          throw new Error("Stream config is undefined");
        }
        // Binding to a block scoped variable to help typescript understand that stream.config is defined
        const config = stream.config;
        return {
          sourceStreamDescriptor: {
            name: stream.stream.name,
            namespace: stream.stream.namespace,
          },
          destinationObjectName: stream.config.destinationObjectName || "",
          sourceSyncMode: stream.config.syncMode,
          destinationSyncMode: stream.config.destinationSyncMode,
          fields: inferFieldsFromConfig(stream.config),
          // matchingKeys refers to the destinaiton field names, but primaryKey refers to the source field names. We
          // need to check the content of the renaming mappers to construct the matchingKeys array from the persisted
          // primaryKey.
          matchingKeys:
            stream.config.mappers && stream.config.primaryKey !== undefined
              ? stream.config.mappers
                  .filter(isFieldRenamingMapper)
                  .filter((mapper) => {
                    return config.primaryKey?.flat().includes(mapper.mapperConfiguration.originalFieldName);
                  })
                  .map((mapper) => mapper.mapperConfiguration.newFieldName)
              : null,
          cursorField: stream.config.cursorField ? stream.config.cursorField[0] || null : null,
        };
      }),
  };
};

function inferFieldsFromConfig(config: AirbyteStreamConfiguration): DataActivationField[] {
  if (!config.mappers) {
    return [];
  }

  return config.mappers.filter(isFieldRenamingMapper).map((mapper) => {
    return {
      sourceFieldName: mapper.mapperConfiguration.originalFieldName,
      destinationFieldName: mapper.mapperConfiguration.newFieldName,
    };
  });
}

export const isFieldRenamingMapper = (
  mapping: ConfiguredStreamMapper
): mapping is ConfiguredStreamMapper & { mapperConfiguration: FieldRenamingMapperConfiguration } => {
  return mapping.type === StreamMapperType["field-renaming"];
};

export const EMPTY_FIELD: DataActivationField = {
  sourceFieldName: "",
  destinationFieldName: "",
};

export const EMPTY_STREAM: DataActivationStream = {
  sourceStreamDescriptor: {
    name: "",
    namespace: undefined,
  },
  destinationObjectName: "",
  sourceSyncMode: null,
  destinationSyncMode: null,
  fields: [EMPTY_FIELD],
  matchingKeys: null,
  cursorField: null,
};
