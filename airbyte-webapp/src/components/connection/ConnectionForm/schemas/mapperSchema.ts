import { z } from "zod";

import {
  RowFilteringOperation,
  StreamMapperType,
  HashingMapperConfigurationMethod,
  FieldRenamingMapperConfiguration,
  HashingMapperConfiguration,
  RowFilteringMapperConfiguration,
  EncryptionMapperAESConfigurationMode,
  EncryptionMapperAESConfigurationPadding,
  EncryptionMapperRSAConfigurationAlgorithm,
  EncryptionMapperRSAConfiguration,
  EncryptionMapperAESConfigurationAlgorithm,
  EncryptionMapperAESConfiguration,
} from "core/api/types/AirbyteClient";
import { ToZodSchema } from "core/utils/zod";

const hashingMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType.hashing),
  mapperConfiguration: z.object({
    fieldNameSuffix: z.string(),
    method: z.nativeEnum(HashingMapperConfigurationMethod),
    targetField: z.string(),
  } satisfies ToZodSchema<HashingMapperConfiguration>),
});

const fieldRenamingMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType["field-renaming"]),
  mapperConfiguration: z.object({
    newFieldName: z.string(),
    originalFieldName: z.string(),
  } satisfies ToZodSchema<FieldRenamingMapperConfiguration>),
});

const rowFilteringMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType["row-filtering"]),
  mapperConfiguration: z.object({
    conditions: z.custom<RowFilteringOperation>(),
  } satisfies ToZodSchema<RowFilteringMapperConfiguration>),
});

const encryptionMapperSchema = z.discriminatedUnion("algorithm", [
  z.object({
    algorithm: z.nativeEnum(EncryptionMapperRSAConfigurationAlgorithm),
    fieldNameSuffix: z.string(),
    publicKey: z.string(),
    targetField: z.string(),
  } satisfies ToZodSchema<EncryptionMapperRSAConfiguration>),
  z.object({
    algorithm: z.nativeEnum(EncryptionMapperAESConfigurationAlgorithm),
    fieldNameSuffix: z.string(),
    key: z.string(),
    mode: z.nativeEnum(EncryptionMapperAESConfigurationMode),
    padding: z.nativeEnum(EncryptionMapperAESConfigurationPadding),
    targetField: z.string(),
  } satisfies ToZodSchema<EncryptionMapperAESConfiguration>),
]);

const encryptionMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType.encryption),
  mapperConfiguration: encryptionMapperSchema,
});

const mapperConfigurationSchema = z.discriminatedUnion("type", [
  hashingMapperConfigurationSchema,
  fieldRenamingMapperConfigurationSchema,
  rowFilteringMapperConfigurationSchema,
  encryptionMapperConfigurationSchema,
]);

export const mapperSchema = z
  .object({
    id: z.string().optional(),
  })
  .and(mapperConfigurationSchema);
