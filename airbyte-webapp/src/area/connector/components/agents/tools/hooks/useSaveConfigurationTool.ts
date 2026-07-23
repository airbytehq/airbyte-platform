import { useMemo } from "react";
import { useFormContext } from "react-hook-form";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { TOOL_NAMES } from "../toolNames";
import { mergePreservingTouchedFields } from "../utils/mergePreservingTouchedFields";

export const useSaveConfigurationTool = (touchedSecretFields: Set<string>): ClientToolHandler => {
  const { setValue, getValues } = useFormContext();

  return useMemo(
    () => ({
      toolName: TOOL_NAMES.SAVE_DRAFT_CONFIGURATION,
      execute: (args: unknown, sendResult: (result: string) => void) => {
        const { name, configuration } = args as { name?: string; configuration?: Record<string, unknown> };

        if (name) {
          setValue("name", name, { shouldDirty: true, shouldValidate: true });
        }

        if (configuration) {
          // Merge configuration preserving touched fields
          const mergedConfiguration = mergePreservingTouchedFields(
            "connectionConfiguration",
            configuration,
            getValues,
            touchedSecretFields
          );

          // Set the merged configuration
          setValue("connectionConfiguration", mergedConfiguration, { shouldDirty: true, shouldValidate: true });
        }

        sendResult("Draft configuration saved. You can now toggle to the form view to see the values.");
      },
    }),
    [setValue, getValues, touchedSecretFields]
  );
};
