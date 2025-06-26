import { useMemo } from "react";
import { z } from "zod";

import {
  NonBreakingChangesPreference,
  SchemaChangeBackfillPreference,
  SyncMode,
  DestinationSyncMode,
  Tag,
  AirbyteStream,
  AirbyteStreamConfiguration,
  SelectedFieldInfo,
} from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { ToZodSchema } from "core/utils/zod";

import { mapperSchema } from "./mapperSchema";
import { namespaceFormatSchema } from "./namespaceDefinitionSchema";
import { scheduleDataSchema } from "./scheduleDataSchema";
import {
  atLeastOneStreamSelectedValidation,
  hashFieldCollisionValidation,
  streamConfigurationValidation,
} from "./validators";

const fieldSchema = z.object({
  fieldPath: z.array(z.string()).optional(),
} satisfies ToZodSchema<SelectedFieldInfo>);

export const tagSchema = z.object({
  tagId: z.string(),
  name: z.string(),
  color: z.string(),
  workspaceId: z.string(),
} satisfies ToZodSchema<Tag>);

/**
 * Zod schema for the stream
 */
const streamZodSchema = z.object({
  name: z.string(),
  jsonSchema: z.record(z.any()).optional(),
  supportedSyncModes: z.array(z.nativeEnum(SyncMode)).optional(),
  sourceDefinedCursor: z.boolean().optional(),
  defaultCursorField: z.array(z.string()).optional(),
  sourceDefinedPrimaryKey: z.array(z.array(z.string())).optional(),
  namespace: z.string().optional(),
  isResumable: z.boolean().optional(),
  isFileBased: z.boolean().optional(),
} satisfies ToZodSchema<AirbyteStream>);

/**
 * Zod schema for the stream configuration
 */
const streamConfigZodSchema = z.object({
  syncMode: z.nativeEnum(SyncMode),
  destinationSyncMode: z.nativeEnum(DestinationSyncMode),
  namespace: z.string().optional(),
  cursorField: z.array(z.string()).optional(),
  selected: z.boolean().optional(),
  suggested: z.boolean().optional(),
  fieldSelectionEnabled: z.boolean().optional(),
  selectedFields: z.array(fieldSchema).optional(),
  hashedFields: z.array(fieldSchema).optional(),
  includeFiles: z.boolean().optional(),
  destinationObjectName: z.string().optional(),
  mappers: z.array(mapperSchema).optional(),
  aliasName: z.string().optional(),
  primaryKey: z.array(z.array(z.string())).optional(),
  minimumGenerationId: z.number().optional(),
  generationId: z.number().optional(),
  syncId: z.number().optional(),
} satisfies ToZodSchema<AirbyteStreamConfiguration>);

/**
 * Zod schema for the stream and configuration
 */
const streamAndConfigurationZodSchema = z.object({
  stream: streamZodSchema,
  config: streamConfigZodSchema,
});

/**
 * Zod schema for the sync catalog
 */
const syncCatalogZodSchema = z.object({
  streams: z
    .array(streamAndConfigurationZodSchema)
    .superRefine(atLeastOneStreamSelectedValidation)
    .superRefine(streamConfigurationValidation)
    .superRefine(hashFieldCollisionValidation),
});

/**
 * Zod version of useConnectionValidationSchema
 */
export const useConnectionValidationZodSchema = () => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  return useMemo(
    () =>
      z
        .object({
          name: z.string().trim().nonempty({ message: "form.empty.error" }),
          prefix: z.string(),
          nonBreakingChangesPreference: allowAutoDetectSchema
            ? z.nativeEnum(NonBreakingChangesPreference)
            : z.nativeEnum(NonBreakingChangesPreference).optional(),
          geography: z.string().optional(),
          syncCatalog: syncCatalogZodSchema,
          notifySchemaChanges: z.boolean().optional(),
          backfillPreference: z.nativeEnum(SchemaChangeBackfillPreference).optional(),
          tags: z.array(tagSchema).optional(),
        })
        .and(scheduleDataSchema)
        .and(namespaceFormatSchema),
    [allowAutoDetectSchema]
  );
};

export const useReplicationConnectionValidationZodSchema = () => {
  return useMemo(
    () =>
      z
        .object({
          syncCatalog: syncCatalogZodSchema,
          prefix: z.string(),
        })
        .and(namespaceFormatSchema),
    []
  );
};
