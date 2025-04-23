import { useCallback } from "react";
import { useFormContext, useWatch } from "react-hook-form";

import { useRefsHandler } from "../RefsHandler";
import { useSchemaForm } from "../SchemaForm";
import { AirbyteJsonSchema } from "../utils";

export const useToggleConfig = (path: string, fieldSchema: AirbyteJsonSchema) => {
  const { setValue, clearErrors, resetField } = useFormContext();
  const value = useWatch({ name: path });
  const { getReferenceInfo, handleUnlinkAction } = useRefsHandler();
  const { extractDefaultValuesFromSchema } = useSchemaForm();

  const handleToggle = useCallback(
    (newEnabledState: boolean) => {
      const schema = fieldSchema;
      const defaultValue = extractDefaultValuesFromSchema(schema);
      if (newEnabledState) {
        // Use resetField to ensure the field and all its children are properly reset
        // This avoids the UI showing stale values that aren't in the form state
        resetField(path, { defaultValue });
        setValue(path, defaultValue);
      } else {
        // Get reference info before making changes
        const refInfo = getReferenceInfo(path);

        // For more deterministic behavior, use a more controlled approach:
        // 1. First unlink all affected references if this field has any
        if (refInfo.type !== "none") {
          // Unlink the field
          handleUnlinkAction(path);

          // Give a small delay to ensure React state updates happen in the correct order
          setTimeout(() => {
            // 2. Then uncheck the field
            setValue(path, undefined);
            clearErrors(path);
          }, 0);
        } else {
          // No references, just uncheck the field immediately
          setValue(path, undefined);
          clearErrors(path);
        }
      }
    },
    [
      fieldSchema,
      extractDefaultValuesFromSchema,
      resetField,
      path,
      setValue,
      getReferenceInfo,
      handleUnlinkAction,
      clearErrors,
    ]
  );

  return {
    isEnabled: value !== undefined,
    onToggle: handleToggle,
  };
};
