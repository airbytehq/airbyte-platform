import { z, RefinementCtx } from "zod";

import { AirbyteStreamAndConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { traverseSchemaToField } from "core/domain/catalog";
import { isNonNullable } from "core/utils/isNonNullable";

/**
 * Check that at least one stream is selected(enabled)
 */
export const atLeastOneStreamSelectedValidation = (streams: AirbyteStreamAndConfiguration[], ctx: RefinementCtx) => {
  if (streams?.some(({ config }) => !!config?.selected)) {
    return true;
  }

  ctx.addIssue({
    code: z.ZodIssueCode.custom,
    message: "connectionForm.streams.required",
    fatal: true,
  });

  return false;
};

/**
 * Validate the stream configuration
 */
export const streamConfigurationValidation = (streams: AirbyteStreamAndConfiguration[], ctx: RefinementCtx) => {
  streams.forEach(({ stream, config }) => {
    if (!config?.selected) {
      return;
    }

    const streamIdentifier = `${stream?.name}_${stream?.namespace}`;
    // Check primary key for dedup modes
    if (
      (DestinationSyncMode.append_dedup === config.destinationSyncMode ||
        DestinationSyncMode.overwrite_dedup === config.destinationSyncMode) &&
      config.primaryKey?.length === 0
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "connectionForm.primaryKey.required",
        path: [`${streamIdentifier}.config.primaryKey`],
      });
    }

    // Check cursor field for incremental mode
    if (
      SyncMode.incremental === config.syncMode &&
      !stream?.sourceDefinedCursor &&
      config.cursorField?.filter(Boolean).length === 0 // filter out empty strings
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "connectionForm.cursorField.required",
        path: [`${streamIdentifier}.config.cursorField`],
      });
    }
  });
};

/**
 * Validate that no two streams have a hash field with the same name
 */
export const hashFieldCollisionValidation = (streams: AirbyteStreamAndConfiguration[], ctx: RefinementCtx) => {
  // group all top-level included fields by stream name & namespace
  const selectedFieldNamesByStream = (streams ?? []).reduce<Record<string, Set<string>>>((acc, { stream, config }) => {
    if (!stream || !config?.selected) {
      return acc;
    }

    const namespace = stream.namespace ?? "";
    const name = stream.name ?? "";
    const key = `${namespace}_${name}`;

    const selectedFields = config.selectedFields?.map((field) => field.fieldPath?.join(".")) ?? [];
    const hasSelectedFields = selectedFields.length > 0;

    const traversedFields = traverseSchemaToField(stream.jsonSchema, stream.name);
    const topLevelFields = traversedFields.reduce<Set<string>>((acc, field) => {
      if (field.path.length === 1) {
        if (!hasSelectedFields || selectedFields.includes(field.path[0])) {
          acc.add(field.path[0]);
        }
      }
      return acc;
    }, new Set());

    acc[key] = topLevelFields;
    return acc;
  }, {});

  // check if any included, hashed field within a given stream will conflict
  // with another stream with the same resulting field name
  const hasConflictingStream = streams?.some(({ stream, config }) => {
    if (!config?.selected) {
      // stream isn't selected
      return false;
    }

    const { hashedFields } = config;
    if (!hashedFields?.length) {
      // stream doesn't have hashed fields
      return false;
    }

    const streamName = stream?.name;
    const namespace = stream?.namespace ?? "";
    const selectedFieldNames = selectedFieldNamesByStream[`${namespace}_${streamName}`];

    const resolvedHashedFields = hashedFields.map(({ fieldPath }) => fieldPath?.join(".")).filter(isNonNullable);
    if (
      resolvedHashedFields.some(
        // check if this field is selected and conflicts with another selected field
        (field) => selectedFieldNames.has(field) && selectedFieldNames.has(`${field}_hashed`)
      )
    ) {
      return true;
    }

    return false;
  });

  if (hasConflictingStream) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.streams.hashFieldCollision",
    });
  }
};
