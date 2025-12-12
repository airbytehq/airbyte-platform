import { useMemo } from "react";

import { useTestConnectorCommand } from "core/api";
import { ActorType } from "core/api/types/AirbyteClient";
import { type ConnectorFormValues } from "views/Connector/ConnectorForm/types";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { TOOL_NAMES } from "../toolNames";

export interface UseCheckConfigurationToolParams {
  actorDefinitionId?: string;
  actorType?: ActorType;
  getFormValues: () => ConnectorFormValues;
}

export const useCheckConfigurationTool = ({
  actorDefinitionId,
  actorType,
  getFormValues,
}: UseCheckConfigurationToolParams): ClientToolHandler => {
  const { testConnector } = useTestConnectorCommand({
    formType: actorType || "source",
  });

  return useMemo(
    () => ({
      toolName: TOOL_NAMES.CHECK_CONFIGURATION,
      execute: async (_args: unknown, sendResult) => {
        // Get the current form values at execution time
        const formValues = getFormValues();
        const configuration = formValues.connectionConfiguration;

        if (configuration && actorDefinitionId) {
          try {
            // Use form values directly - they contain actual secrets
            const result = await testConnector({
              name: formValues.name || "Test Configuration",
              serviceType: actorDefinitionId,
              connectionConfiguration: configuration as Record<string, unknown>,
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
    [actorDefinitionId, getFormValues, testConnector]
  );
};
