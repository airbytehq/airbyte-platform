import { zodResolver } from "@hookform/resolvers/zod";
import { FieldValues, Resolver } from "react-hook-form";
import { z } from "zod";

export function autoSubmitResolver<TSchema extends z.ZodSchema<TFieldValues>, TFieldValues extends FieldValues>(
  schema: TSchema,
  onSubmit: (formValues: TFieldValues) => void
): Resolver<TFieldValues> {
  return async (values, context, options) => {
    try {
      schema.parse(values);
      onSubmit(values);
    } catch (e) {
      if (!(e instanceof z.ZodError)) {
        throw e;
      }
      return zodResolver(schema)(values, context, options);
    }
    return zodResolver(schema)(values, context, options);
  };
}
