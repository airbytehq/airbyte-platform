import { z } from "zod";

import { publicKey } from "components/connection/ConnectionForm/schemas/mapperSchema";

import {
  AirbyteCatalog,
  AirbyteStreamConfiguration,
  ConfiguredStreamMapper,
  EncryptionMapperRSAConfiguration,
  EncryptionMapperRSAConfigurationAlgorithm,
  FieldRenamingMapperConfiguration,
  HashingMapperConfigurationMethod,
  StreamMapperType,
} from "core/api/types/AirbyteClient";
import { isNonNullable } from "core/utils/isNonNullable";
import { ToZodSchema } from "core/utils/zod";
import { FilterCondition } from "pages/connections/ConnectionMappingsPage/RowFilteringMapperForm";

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
          fields: getFieldsFromConfig(stream.config),
          matchingKeys: getMatchingKeysFromConfig(stream.config),
          cursorField: stream.config.cursorField ? stream.config.cursorField[0] || null : null,
        };
      }),
  };
};

const hashingMapper = z.object({
  type: z.literal(StreamMapperType.hashing),
  mapperConfiguration: z.object({
    method: z.nativeEnum(HashingMapperConfigurationMethod),
    targetField: z.string(),
    fieldNameSuffix: z.literal(""),
  }),
});

const filterInMapper = z.object({
  type: z.literal(StreamMapperType["row-filtering"]),
  mapperConfiguration: z.object({
    conditions: z.object({
      type: z.literal("EQUAL"),
      fieldName: z.string(),
      comparisonValue: z.string(),
    }),
  }),
});

const filterOutMapper = z.object({
  type: z.literal(StreamMapperType["row-filtering"]),
  mapperConfiguration: z.object({
    conditions: z.object({
      type: z.literal("NOT"),
      conditions: z.array(
        z.object({
          type: z.literal("EQUAL"),
          fieldName: z.string(),
          comparisonValue: z.string(),
        })
      ),
    }),
  }),
});

const encryptionMapper = z.object({
  type: z.literal(StreamMapperType.encryption),
  mapperConfiguration: z.object({
    algorithm: z.nativeEnum(EncryptionMapperRSAConfigurationAlgorithm),
    targetField: z.string().nonempty(),
    publicKey,
    fieldNameSuffix: z.literal(""),
  } satisfies ToZodSchema<EncryptionMapperRSAConfiguration>),
});

function getFieldsFromConfig(config: AirbyteStreamConfiguration): DataActivationField[] {
  const persistedMappers = config.mappers;
  if (!persistedMappers) {
    return [];
  }
  // Field renaming mappers are the basis for the fields we show in the UI
  return persistedMappers.filter(isFieldRenamingMapper).map((fieldRenamingMapper) => {
    // Additional mappers are any mappers that operate on the same field, but do a different operation (e.g. hashing,
    // filtering or encryption). These will be nested under the field renaming mapper in the form UI.
    const additionalMappers = persistedMappers
      .map((mapper) => {
        // Detect and transform hashing mappers for this field
        const hashingMapperResult = hashingMapper.safeParse(mapper);
        if (
          hashingMapperResult.success &&
          hashingMapperResult.data.mapperConfiguration.targetField ===
            fieldRenamingMapper.mapperConfiguration.originalFieldName
        ) {
          return {
            type: "hashing",
            method: hashingMapperResult.data.mapperConfiguration.method,
          } as const;
        }

        // Detect and transform row filtering mappers for this field that filter in rows
        const filterInMapperResult = filterInMapper.safeParse(mapper);
        if (
          filterInMapperResult.success &&
          filterInMapperResult.data.mapperConfiguration.conditions.fieldName ===
            fieldRenamingMapper.mapperConfiguration.originalFieldName
        ) {
          return {
            type: "row-filtering",
            condition: FilterCondition.IN,
            comparisonValue: filterInMapperResult.data.mapperConfiguration.conditions.comparisonValue,
          } as const;
        }

        // Detect and transform row filtering mappers for this field that filter out rows
        const filterOutMapperResult = filterOutMapper.safeParse(mapper);
        if (
          filterOutMapperResult.success &&
          filterOutMapperResult.data.mapperConfiguration.conditions.conditions[0].fieldName ===
            fieldRenamingMapper.mapperConfiguration.originalFieldName
        ) {
          return {
            type: "row-filtering",
            condition: FilterCondition.OUT,
            comparisonValue: filterOutMapperResult.data.mapperConfiguration.conditions.conditions[0].comparisonValue,
          } as const;
        }

        // Detect and transform encryption mappers for this field
        const encryptionMapperResult = encryptionMapper.safeParse(mapper);
        if (
          encryptionMapperResult.success &&
          encryptionMapperResult.data.mapperConfiguration.targetField ===
            fieldRenamingMapper.mapperConfiguration.originalFieldName
        ) {
          return {
            type: "encryption",
            publicKey: encryptionMapperResult.data.mapperConfiguration.publicKey,
          } as const;
        }

        // Any other mappers will be stripped out in the UI
        return null;
      })
      .filter(isNonNullable);

    return {
      sourceFieldName: fieldRenamingMapper.mapperConfiguration.originalFieldName,
      destinationFieldName: fieldRenamingMapper.mapperConfiguration.newFieldName,
      additionalMappers,
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

// We use the primaryKey to persist matchingKeys in a connection. However, the matchingKeys refers to destination field
// names, and the primaryKey refers to source field names. This function derives the matchingKeys from the primaryKey,
// ensuring that every primaryKey field also has a corresponding field renaming mapper in the config.
export function getMatchingKeysFromConfig(config: AirbyteStreamConfiguration): string[] | null {
  if (!config.mappers || !config.primaryKey) {
    return null;
  }

  const fieldRenamingMappers = config.mappers.filter(isFieldRenamingMapper);

  const primaryKeyFields = config.primaryKey.flat();

  if (primaryKeyFields.length === 0) {
    return null;
  }

  // Check that every primaryKey field has a corresponding field renaming mapper
  const allFieldsHaveMapper = primaryKeyFields.every((pkField) =>
    fieldRenamingMappers.some((mapper) => mapper.mapperConfiguration.originalFieldName === pkField)
  );

  // This would indicate a malformed config where we set a primaryKey but did not map all the fields to the destination.
  if (!allFieldsHaveMapper) {
    return null;
  }

  // Map each primaryKey field to its newFieldName, which corresponds to the matchingKeys
  return primaryKeyFields
    .map((pkField) => {
      const mapper = fieldRenamingMappers.find((mapper) => mapper.mapperConfiguration.originalFieldName === pkField);
      return mapper?.mapperConfiguration.newFieldName;
    })
    .filter(isNonNullable);
}
