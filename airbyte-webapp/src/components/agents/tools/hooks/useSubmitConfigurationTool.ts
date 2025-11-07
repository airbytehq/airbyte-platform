import { useMemo } from "react";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { type SecretsMap } from "../../types";
import { replaceSecretsInConfig } from "../../utils/replaceSecretsInConfig";
import { TOOL_NAMES } from "../toolNames";

export interface UseSubmitConfigurationToolParams {
  actorDefinitionId?: string;
  onSubmitSourceStep?: (sourceValues: {
    name: string;
    serviceType: string;
    connectionConfiguration: Record<string, unknown>;
  }) => void;
  getSecrets: () => SecretsMap;
}

export const useSubmitConfigurationTool = ({
  actorDefinitionId,
  onSubmitSourceStep,
  getSecrets,
}: UseSubmitConfigurationToolParams): ClientToolHandler => {
  return useMemo(
    () => ({
      toolName: TOOL_NAMES.SUBMIT_CONFIGURATION,
      execute: (args: unknown) => {
        const { name, configuration } = args as { name: string; configuration: Record<string, unknown> };
        if (configuration && onSubmitSourceStep) {
          // Get the current secrets at execution time to avoid stale closures
          const secrets = getSecrets();
          // Replace secret IDs with actual values
          const resolvedConfiguration = replaceSecretsInConfig(configuration, secrets);

          const sourceValues = {
            name,
            serviceType: actorDefinitionId!,
            connectionConfiguration: resolvedConfiguration as Record<string, unknown>,
          };

          onSubmitSourceStep(sourceValues);
        } else {
          console.error("[useSubmitConfigurationTool] No configuration found or onSubmitSourceStep not provided");
        }
      },
    }),
    [actorDefinitionId, onSubmitSourceStep, getSecrets]
  );
};
