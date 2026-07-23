import { useState } from "react";
import { FieldError, useFormContext, useWatch } from "react-hook-form";
import { useUpdateEffect } from "react-use";

import { useSchemaForm } from "./SchemaForm";

export const useErrorAtPath = (path: string): FieldError | undefined => {
  const { onlyShowErrorIfTouched } = useSchemaForm();
  const { getFieldState } = useFormContext();
  const value = useWatch({ name: path });
  const [error, setError] = useState<FieldError | undefined>(undefined);
  const [isTouched, setIsTouched] = useState<boolean>(false);

  useUpdateEffect(() => {
    // getFieldState() doesn't return the updated field state until the next render cycle,
    // so use setTimeout to update the local states then.
    setTimeout(() => {
      const { error, isTouched } = getFieldState(path);
      setError(error);
      setIsTouched(isTouched);
    }, 0);
  }, [value]);

  if ((onlyShowErrorIfTouched && !isTouched) || !error?.message) {
    return undefined;
  }
  if (!error?.message) {
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
