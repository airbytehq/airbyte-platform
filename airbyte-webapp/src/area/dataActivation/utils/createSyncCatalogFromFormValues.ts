import {
  AirbyteCatalog,
  ConfiguredStreamMapper,
  EncryptionMapperRSAConfigurationAlgorithm,
  RowFilteringOperationType,
  StreamMapperType,
} from "core/api/types/AirbyteClient";
import { isNonNullable } from "core/utils/isNonNullable";
import { FilterCondition } from "pages/connections/ConnectionMappingsPage/RowFilteringMapperForm";

import { DataActivationConnectionFormSchema } from "./DataActivationConnectionFormSchema";
import { DataActivationConnectionFormValues, DataActivationField } from "../types";

export function createSyncCatalogFromFormValues(
  formOutput: DataActivationConnectionFormValues,
  catalog: AirbyteCatalog
): AirbyteCatalog {
  const mappedStreams = DataActivationConnectionFormSchema.parse(formOutput);

  return {
    streams:
      catalog.streams.map((stream) => {
        if (!stream.stream || !stream.config) {
          throw new Error("Stream or stream config is unexpectedly undefined");
        }

        const mappers: ConfiguredStreamMapper[] = [];

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
          if (!selectedFields.some((field) => field.fieldPath[0] === cursorField[0])) {
            selectedFields.push({
              fieldPath: cursorField,
            });
          }

          // If the cursor field is not mapped to a destination field, we need to add a field filtering mapper to remove
          // it from the catalog. This is necessary because the cursor field MUST be part of selectedFields for some
          // sources to work, but it will cause an error in DA destinations if it is not mapped to a destination field.
          const cursorFieldName = cursorField[0];

          // If another mapper already maps the cursor to a destination field, we do not want to filter it out of the catalog
          const isCursorMapped = mappedStream.fields.some((field) => field.sourceFieldName === cursorFieldName);

          if (!isCursorMapped) {
            mappers.push({
              type: StreamMapperType["field-filtering"],
              mapperConfiguration: {
                targetField: cursorFieldName,
              },
            });
          }
        }

        // Add all other mappers from the mapped stream
        mappers.push(...createMappers(mappedStream.fields));

        return {
          config: {
            ...stream.config,
            destinationObjectName: mappedStream.destinationObjectName,
            destinationSyncMode: mappedStream.destinationSyncMode ?? stream.config?.destinationSyncMode ?? "append",
            primaryKey,
            cursorField,
            syncMode: mappedStream.sourceSyncMode ?? stream.config?.syncMode ?? "full_refresh",
            mappers,
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

function createMappers(fields: DataActivationField[]): ConfiguredStreamMapper[] {
  const allMappers = fields
    .map((field) => {
      const additionalMappers: ConfiguredStreamMapper[] =
        field.additionalMappers
          ?.map((config) => {
            if (config.type === "row-filtering") {
              return createRowFilteringMapper(field.sourceFieldName, config.condition, config.comparisonValue);
            }
            if (config.type === "hashing") {
              return {
                type: StreamMapperType.hashing,
                mapperConfiguration: {
                  targetField: field.sourceFieldName,
                  method: config.method,
                  fieldNameSuffix: "", // It's important that the suffix is not changed so that subsequent mappers can be applied to the same field
                },
              };
            }
            if (config.type === "encryption") {
              return {
                type: StreamMapperType.encryption,
                mapperConfiguration: {
                  algorithm: EncryptionMapperRSAConfigurationAlgorithm.RSA,
                  targetField: field.sourceFieldName,
                  publicKey: config.publicKey,
                  fieldNameSuffix: "", // It's important that the suffix is not changed so that subsequent mappers can be applied to the same field
                },
              };
            }
            return null;
          })
          .filter(isNonNullable) ?? [];
      const finalRenamingMapper = {
        type: StreamMapperType["field-renaming"],
        mapperConfiguration: {
          newFieldName: field.destinationFieldName,
          originalFieldName: field.sourceFieldName,
        },
      };
      return additionalMappers.concat([finalRenamingMapper]);
    })
    .flat();

  return allMappers;
}

function createRowFilteringMapper(
  fieldName: string,
  condition: FilterCondition,
  comparisonValue: string
): ConfiguredStreamMapper {
  const mapperConfiguration =
    condition === FilterCondition.OUT
      ? {
          conditions: {
            type: RowFilteringOperationType.NOT,
            conditions: [
              {
                type: RowFilteringOperationType.EQUAL,
                fieldName,
                comparisonValue,
              },
            ],
          },
        }
      : {
          conditions: {
            type: RowFilteringOperationType.EQUAL,
            fieldName,
            comparisonValue,
          },
        };

  return {
    type: StreamMapperType["row-filtering"],
    mapperConfiguration,
  };
}
