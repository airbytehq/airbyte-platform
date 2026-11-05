import { ZodNullable, ZodOptional, ZodType } from "zod";

/**
 * Zod parses every element of an array that has no maximum and keeps one issue per failing element, so a huge
 * array can exhaust memory (GHSA-mrhx-c7xc-m3w5). An array schema rejects an oversized input before it parses any
 * element only when `.max()` comes before any refinement. The limit is far above any real list in the webapp.
 */
export const MAX_ARRAY_LENGTH = 100_000;

type IsNullable<T> = Extract<T, null> extends never ? false : true;
type IsOptional<T> = Extract<T, undefined> extends never ? false : true;

/**
 * Converts a TypeScript type to a Zod schema type, preserving nullability and optionality.
 * Explicitly declares optional properties in the schema to track changes when generated
 * interfaces are updated, even though Zod treats missing and undefined properties the same.
 *
 * @template T - The TypeScript type to convert (must be a record type)
 * zod_issue: https://github.com/colinhacks/zod/issues/2807
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ToZodSchema<T extends Record<string, any>> = {
  [K in keyof T]-?: IsNullable<T[K]> extends true
    ? ZodNullable<ZodType<T[K]>>
    : IsOptional<T[K]> extends true
    ? ZodOptional<ZodType<T[K]>>
    : ZodType<T[K]>;
};
