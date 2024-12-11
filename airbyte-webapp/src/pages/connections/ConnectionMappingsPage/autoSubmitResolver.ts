import { FieldValues, Resolver } from "react-hook-form";
import * as yup from "yup";
import { AssertsShape } from "yup/lib/object";

export function autoSubmitResolver<TFieldValues extends FieldValues>(
  schema: yup.SchemaOf<TFieldValues> | ReturnType<typeof yup.lazy<yup.ObjectSchema<TFieldValues>>>,
  onSubmit: (formValues: AssertsShape<TFieldValues>) => void
): Resolver<TFieldValues> {
  return async (values) => {
    try {
      schema.validateSync(values);
      onSubmit(values);
    } catch (e) {
      if (!(e instanceof yup.ValidationError)) {
        throw e;
      }

      // TODO: parse yup.ValidationError and create a rhf FieldErrors object
      console.log(e);
      return {
        values: {},
        errors: {},
      };
    }
    return {
      values,
      errors: {},
    };
  };
}
