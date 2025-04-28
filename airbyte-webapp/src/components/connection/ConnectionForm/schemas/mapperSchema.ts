import { z } from "zod";

import {
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

/**
 * Hashing
 */
export const hashingMapperConfiguration = z.object({
  fieldNameSuffix: z.string().nonempty("form.empty.error"),
  method: z.nativeEnum(HashingMapperConfigurationMethod),
  targetField: z.string().nonempty("form.empty.error"),
} satisfies ToZodSchema<HashingMapperConfiguration>);

const hashingMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType.hashing),
  mapperConfiguration: hashingMapperConfiguration,
});

/**
 * Field renaming
 */
export const fieldRenamingMapperConfiguration = z.object({
  newFieldName: z.string().nonempty("form.empty.error"),
  originalFieldName: z.string().nonempty("form.empty.error"),
} satisfies ToZodSchema<FieldRenamingMapperConfiguration>);

const fieldRenamingMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType["field-renaming"]),
  mapperConfiguration: fieldRenamingMapperConfiguration,
});

/**
 * Row filtering
 */
const rowFilteringMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType["row-filtering"]),
  mapperConfiguration: z.object({
    conditions: z.discriminatedUnion("type", [
      z.object({
        type: z.literal("EQUAL"),
        fieldName: z.string().nonempty("form.empty.error"),
        comparisonValue: z.string().nonempty("form.empty.error"),
      }),
      z.object({
        type: z.literal("NOT"),
        conditions: z.array(
          z.object({
            type: z.literal("EQUAL"),
            fieldName: z.string().nonempty("form.empty.error"),
            comparisonValue: z.string().nonempty("form.empty.error"),
          })
        ),
      }),
    ]),
  } satisfies ToZodSchema<RowFilteringMapperConfiguration>),
});

/**
 * Encryption
 */
const rsaEncryptionMapperConfigurationSchema = z.object({
  targetField: z.string().nonempty("connections.mappings.error.FIELD_NAME_REQUIRED"),
  algorithm: z.nativeEnum(EncryptionMapperRSAConfigurationAlgorithm),
  fieldNameSuffix: z.string().regex(/^_encrypted$/, "connections.mappings.error.FIELD_NAME_SUFFIX_REQUIRED"),
  publicKey: z.string().regex(/^[0-9a-fA-F]*$/, "connections.mappings.error.INVALID_RSA_PUBLIC_KEY"),
} satisfies ToZodSchema<EncryptionMapperRSAConfiguration>);

const aesEncryptionMapperConfigurationSchema = z.object({
  targetField: z.string().nonempty("connections.mappings.error.FIELD_NAME_REQUIRED"),
  algorithm: z.nativeEnum(EncryptionMapperAESConfigurationAlgorithm),
  fieldNameSuffix: z.literal("_encrypted"),
  key: z.string(),
  mode: z.nativeEnum(EncryptionMapperAESConfigurationMode),
  padding: z.nativeEnum(EncryptionMapperAESConfigurationPadding),
} satisfies ToZodSchema<EncryptionMapperAESConfiguration>);

export const encryptionMapperSchema = z.discriminatedUnion("algorithm", [
  rsaEncryptionMapperConfigurationSchema,
  aesEncryptionMapperConfigurationSchema,
]);

const encryptionMapperConfigurationSchema = z.object({
  type: z.literal(StreamMapperType.encryption),
  mapperConfiguration: encryptionMapperSchema,
});

/**
 * Mapper configuration
 */
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
