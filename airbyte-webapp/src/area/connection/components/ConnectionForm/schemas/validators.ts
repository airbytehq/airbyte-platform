import get from "lodash/get";
import { FieldError, FieldErrors } from "react-hook-form";
import { z, RefinementCtx } from "zod";

import { AirbyteStreamAndConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { traverseSchemaToField } from "core/domain/catalog";
import { isNonNullable } from "core/utils/isNonNullable";

/**
 * Check that at least one stream is selected(enabled)
 */
export const atLeastOneStreamSelectedValidation = (streams: AirbyteStreamAndConfiguration[], ctx: RefinementCtx) => {
  if (streams?.some(({ config }) => !!config?.selected)) {
    return;
  }

  ctx.addIssue({
    code: z.ZodIssueCode.custom,
    message: "connectionForm.streams.required",
    fatal: true,
  });
};

type StreamConfigFieldErrors = Record<string, FieldError | undefined>;

interface StreamConfigErrorNode {
  config?: StreamConfigFieldErrors;
  [part: string]: StreamConfigErrorNode | StreamConfigFieldErrors | undefined;
}

type StreamConfigErrors = Record<string, StreamConfigErrorNode | undefined>;

// A stream name such as `type.events` also creates a `type` key, but its value is an object, not a string.
const isFieldError = (node: object): node is FieldError => typeof (node as { type?: unknown }).type === "string";

/**
 * Returns the stream configuration errors keyed by `${streamName}_${streamNamespace}`, nested one level under each part
 * of a key that contains a period. react-hook-form keeps them on `syncCatalog.streams` after a full validation, and
 * under `syncCatalog.streams.root` after only the streams array is validated, because the keys are not array indexes.
 * A field error under `root` belongs to the whole array.
 */
export const getStreamConfigErrors = (errors: FieldErrors | undefined): StreamConfigErrors => {
  const { root, ...streamErrors } = ((errors?.syncCatalog as FieldErrors | undefined)?.streams ??
    {}) as StreamConfigErrors & {
    root?: StreamConfigErrors | FieldError;
  };
  const rootStreamErrors = root && !isFieldError(root) ? root : {};
  return { ...streamErrors, ...rootStreamErrors };
};

type StreamConfigField = "primaryKey" | "cursorField";

const hasConfigFieldMessage = (node: unknown, field: StreamConfigField, message: string): boolean => {
  if (typeof node !== "object" || node === null || isFieldError(node)) {
    return false;
  }
  const streamNode = node as StreamConfigErrorNode;
  if (streamNode.config?.[field]?.message === message) {
    return true;
  }
  return Object.values(node).some((child) => hasConfigFieldMessage(child, field, message));
};

/**
 * Returns whether the error of a configuration field of any stream has this message. The search goes below the first
 * level, because react-hook-form nests the error under each part of a stream name or namespace that contains a period.
 */
export const hasStreamConfigFieldErrorMessage = (
  errors: FieldErrors | undefined,
  field: StreamConfigField,
  message: string
) => hasConfigFieldMessage(getStreamConfigErrors(errors), field, message);

/**
 * Returns the error of one configuration field of one stream, from the same two places as `getStreamConfigErrors`. The
 * lookup follows the path, because react-hook-form nests the error under each part of a stream name or namespace that
 * contains a period.
 */
export const getStreamConfigFieldError = (
  errors: FieldErrors | undefined,
  streamIdentifier: string,
  field: StreamConfigField
) => {
  const path: string = `[${streamIdentifier}].config.${field}`;
  return get(errors, `syncCatalog.streams${path}`) ?? get(errors, `syncCatalog.streams.root${path}`);
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
