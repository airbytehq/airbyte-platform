import { useMemo } from "react";
import { useFormContext } from "react-hook-form";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { type SecretsMap } from "../../types";
import { replaceSecretsInConfig } from "../../utils/replaceSecretsInConfig";
import { TOOL_NAMES } from "../toolNames";

export interface UseSaveConfigurationToolParams {
  getSecrets: () => SecretsMap;
}

export const useSaveConfigurationTool = ({ getSecrets }: UseSaveConfigurationToolParams): ClientToolHandler => {
  const { setValue } = useFormContext();

  return useMemo(
    () => ({
      toolName: TOOL_NAMES.SAVE_DRAFT_CONFIGURATION,
      execute: (args: unknown, sendResult: (result: string) => void) => {
        const { name, configuration } = args as { name?: string; configuration?: Record<string, unknown> };

        // Get the current secrets at execution time to avoid stale closures
        const secrets = getSecrets();

        // Replace secret IDs with actual values if configuration is provided
        const resolvedConfiguration = configuration
          ? (replaceSecretsInConfig(configuration, secrets) as Record<string, unknown>)
          : undefined;

        // Update form values directly using react-hook-form's setValue
        if (name) {
          setValue("name", name, { shouldDirty: true, shouldValidate: true });
        }

        if (resolvedConfiguration) {
          // Set each field individually to ensure proper form updates
          Object.entries(resolvedConfiguration).forEach(([key, value]) => {
            setValue(`connectionConfiguration.${key}`, value, { shouldDirty: true, shouldValidate: true });
          });
        }

        sendResult("Draft configuration saved. You can now toggle to the form view to see the values.");
      },
    }),
    [setValue, getSecrets]
  );
};
