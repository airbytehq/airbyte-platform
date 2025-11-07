import { useMemo } from "react";

import { useTestConnectorCommand } from "core/api";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { type SecretsMap } from "../../types";
import { replaceSecretsInConfig } from "../../utils/replaceSecretsInConfig";
import { TOOL_NAMES } from "../toolNames";

export interface UseCheckConfigurationToolParams {
  actorDefinitionId?: string;
  actorType?: "source" | "destination";
  getSecrets: () => SecretsMap;
}

export const useCheckConfigurationTool = ({
  actorDefinitionId,
  actorType,
  getSecrets,
}: UseCheckConfigurationToolParams): ClientToolHandler => {
  const { testConnector } = useTestConnectorCommand({
    formType: actorType || "source",
  });

  return useMemo(
    () => ({
      toolName: TOOL_NAMES.CHECK_CONFIGURATION,
      execute: async (args: unknown, sendResult) => {
        // Get the current secrets at execution time to avoid stale closures
        const secrets = getSecrets();

        const { configuration } = args as { configuration: Record<string, unknown> };
        if (configuration && actorDefinitionId) {
          // Inject secrets at their respective paths before testing
          const resolvedConfiguration = replaceSecretsInConfig(configuration, secrets);

          try {
            // Test the configuration
            const result = await testConnector({
              name: "Test Configuration",
              serviceType: actorDefinitionId,
              connectionConfiguration: resolvedConfiguration as Record<string, unknown>,
              resourceAllocation: {},
            });

            sendResult(
              JSON.stringify({
                success: true,
                message: "Configuration test passed successfully",
                status: result.status,
              })
            );
          } catch (error) {
            console.error("[useCheckConfigurationTool] Configuration test failed:", error);
            sendResult(
              JSON.stringify({
                success: false,
                message: error instanceof Error ? error.message : "Configuration test failed",
                error: error instanceof Error ? error.toString() : String(error),
              })
            );
          }
        } else {
          console.error(
            "[useCheckConfigurationTool] No configuration or actorDefinitionId provided for check_configuration"
          );
          sendResult(
            JSON.stringify({
              success: false,
              message: "No configuration or actorDefinitionId provided",
            })
          );
        }
      },
    }),
    [actorDefinitionId, getSecrets, testConnector]
  );
};
