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

        return {
          sourceStreamDescriptor: {
            name: stream.stream.name,
            namespace: stream.stream.namespace,
          },
          destinationObjectName: stream.config.destinationObjectName || "",
          sourceSyncMode: stream.config.syncMode,
          destinationSyncMode: stream.config.destinationSyncMode,
          fields: inferFieldsFromConfig(stream.config),
          primaryKey:
            stream.config.primaryKey && stream.config.primaryKey[0] ? stream.config.primaryKey[0].join(".") : null,
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
  primaryKey: null,
  cursorField: null,
};
