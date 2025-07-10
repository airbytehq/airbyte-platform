import { v4 as uuid } from "uuid";

import { AirbyteCatalog, StreamMapperType } from "core/api/types/AirbyteClient";

import { DataActivationConnectionFormOutput } from "./DataActivationConnectionFormSchema";

export function createSyncCatalogFromFormValues(
  mappedStreams: DataActivationConnectionFormOutput,
  catalog: AirbyteCatalog
): AirbyteCatalog {
  return {
    streams:
      catalog.streams.map((stream) => {
        if (!stream.stream || !stream.config) {
          throw new Error("Stream or stream config is unexpectedly undefined");
        }

        const mappedStream = mappedStreams.streams.find(
          (s) =>
            s.sourceStreamDescriptor.name === stream.stream?.name &&
            s.sourceStreamDescriptor.namespace === stream.stream?.namespace
        );

        // All other streams should be unselected, but otherwise untouched
        if (!mappedStream) {
          return {
            config: {
              ...stream.config,
              selected: false,
            },
            stream: stream.stream,
          };
        }

        const selectedFields =
          mappedStream.fields.map((field) => ({
            fieldPath: field.sourceFieldName.split("."),
          })) ?? [];

        const primaryKey: string[][] = [];
        if (mappedStream.matchingKeys) {
          mappedStream.matchingKeys?.forEach((key) => {
            const sourceFieldName = mappedStream.fields.find((f) => f.destinationFieldName === key)?.sourceFieldName;
            if (sourceFieldName) {
              primaryKey.push([sourceFieldName]);
            }
          });
        }

        const cursorField = mappedStream.sourceSyncMode === "incremental" ? [mappedStream.cursorField] : undefined;
        if (cursorField) {
          selectedFields.push({
            fieldPath: cursorField,
          });
        }

        return {
          config: {
            ...stream.config,
            destinationObjectName: mappedStream.destinationObjectName,
            destinationSyncMode: mappedStream.destinationSyncMode ?? stream.config?.destinationSyncMode ?? "append",
            primaryKey,
            cursorField,
            syncMode: mappedStream.sourceSyncMode ?? stream.config?.syncMode ?? "full_refresh",
            mappers:
              mappedStream.fields.map(({ sourceFieldName, destinationFieldName }) => ({
                id: uuid(),
                type: StreamMapperType["field-renaming"],
                mapperConfiguration: {
                  newFieldName: destinationFieldName,
                  originalFieldName: sourceFieldName,
                },
              })) ?? stream.config?.mappers,
            selected: true,
            selectedFields: selectedFields.length ? selectedFields : undefined,
            fieldSelectionEnabled: selectedFields.length > 0,
          },
          stream: {
            ...stream.stream,
            name: mappedStream.sourceStreamDescriptor.name ?? stream.stream?.name ?? "",
          },
        };
      }) ?? [],
  };
}
