import { FieldError, get, useFormState } from "react-hook-form";

import { useSchemaForm } from "./SchemaForm";

export const useErrorAtPath = (path: string): FieldError | undefined => {
  const { errors, touchedFields } = useFormState();
  const { onlyShowErrorIfTouched } = useSchemaForm();

  const error: FieldError | undefined = get(errors, path);
  const touched = get(touchedFields, path);
  if ((onlyShowErrorIfTouched && !touched) || !error?.message) {
    return undefined;
  }
  return {
    type: error.type,
    message: error.message,
    ref: error.ref,
    types: error.types,
    root: error.root,
  };
};
