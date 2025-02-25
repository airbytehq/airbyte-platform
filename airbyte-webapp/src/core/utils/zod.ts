import { ZodEffects, ZodNullable, ZodOptional, ZodType, ZodTypeAny } from "zod";

type IsNullable<T> = Extract<T, null> extends never ? false : true;
type IsOptional<T> = Extract<T, undefined> extends never ? false : true;

type ZodWithEffects<T extends ZodTypeAny> = T | ZodEffects<T, unknown, unknown>;

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
    ? ZodWithEffects<ZodNullable<ZodType<T[K]>>>
    : IsOptional<T[K]> extends true
    ? ZodWithEffects<ZodOptional<ZodType<T[K]>>>
    : ZodWithEffects<ZodType<T[K]>>;
};
