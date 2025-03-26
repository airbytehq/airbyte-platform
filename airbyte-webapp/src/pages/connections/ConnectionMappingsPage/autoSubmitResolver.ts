import { yupResolver } from "@hookform/resolvers/yup";
import { FieldValues, Resolver } from "react-hook-form";
import * as yup from "yup";
import { AssertsShape } from "yup/lib/object";

export function autoSubmitResolver<TFieldValues extends FieldValues>(
  schema: yup.SchemaOf<TFieldValues> | ReturnType<typeof yup.lazy<yup.ObjectSchema<TFieldValues>>>,
  onSubmit: (formValues: AssertsShape<TFieldValues>) => void
): Resolver<TFieldValues> {
  return async (values, context, options) => {
    try {
      schema.validateSync(values);
      onSubmit(values);
    } catch (e) {
      if (!(e instanceof yup.ValidationError)) {
        throw e;
      }
      return yupResolver(schema)(values, context, options);
    }
    return yupResolver(schema)(values, context, options);
  };
}
