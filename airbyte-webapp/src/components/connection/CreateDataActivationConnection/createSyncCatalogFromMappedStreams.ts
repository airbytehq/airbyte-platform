import { v4 as uuid } from "uuid";

import { AirbyteCatalog, SourceDiscoverSchemaRead } from "core/api/types/AirbyteClient";

import { StreamMappingsFormValuesType } from "./StreamMappings";

export function createSyncCatalogFromMappedStreams(
  mappedStreams: StreamMappingsFormValuesType,
  sourceSchema: SourceDiscoverSchemaRead
): AirbyteCatalog {
  return {
    streams:
      sourceSchema.catalog?.streams.map((stream) => {
        const mappedStream = mappedStreams.streams.find((s) => s.sourceStreamDescriptor.name === stream.stream?.name);
        const selectedFields =
          mappedStream?.fields.map((field) => ({
            fieldPath: field.sourceFieldName.split("."),
            selected: true,
          })) ?? undefined;

        const primaryKey =
          mappedStream?.destinationSyncMode === "append_dedup" ? [mappedStream.primaryKey.split(".")] : undefined;
        const cursorField = mappedStream?.sourceSyncMode === "incremental" ? [mappedStream.cursorField] : undefined;

        return {
          config: {
            ...stream.config,
            destinationObjectName: mappedStream?.destinationObjectName,
            destinationSyncMode: mappedStream?.destinationSyncMode ?? stream.config?.destinationSyncMode ?? "append",
            primaryKey,
            cursorField,
            syncMode: mappedStream?.sourceSyncMode ?? stream.config?.syncMode ?? "full_refresh",
            mappers:
              mappedStream?.fields.map(({ sourceFieldName, destinationFieldName }) => ({
                id: uuid(),
                type: "field-renaming",
                mapperConfiguration: {
                  newFieldName: destinationFieldName,
                  originalFieldName: sourceFieldName,
                },
              })) ?? stream.config?.mappers,
            selected: !!mappedStream,
            selectedFields,
            fieldSelectionEnabled: !!selectedFields,
          },
          stream: {
            ...stream.stream,
            name: mappedStream?.sourceStreamDescriptor.name ?? stream.stream?.name ?? "",
          },
        };
      }) ?? [],
  };
}
