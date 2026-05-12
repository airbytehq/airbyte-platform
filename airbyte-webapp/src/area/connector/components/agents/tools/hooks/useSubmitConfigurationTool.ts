import { useMemo } from "react";

import { type ConnectorFormValues } from "area/connector/components/ConnectorForm/types";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { TOOL_NAMES } from "../toolNames";

export interface UseSubmitConfigurationToolParams {
  actorDefinitionId?: string;
  onSubmitSourceStep?: (sourceValues: {
    name: string;
    serviceType: string;
    connectionConfiguration: Record<string, unknown>;
  }) => void;
  getFormValues: () => ConnectorFormValues;
}

export const useSubmitConfigurationTool = ({
  actorDefinitionId,
  onSubmitSourceStep,
  getFormValues,
}: UseSubmitConfigurationToolParams): ClientToolHandler => {
  return useMemo(
    () => ({
      toolName: TOOL_NAMES.SUBMIT_CONFIGURATION,
      execute: (args: unknown) => {
        const { name: agentName } = (args ?? {}) as { name?: string };
        const formValues = getFormValues();

        if (formValues.connectionConfiguration && onSubmitSourceStep) {
          // Prefer the name supplied by the agent so the user's chosen name
          // from the chat is applied. Fall back to the form value if the agent
          // did not provide one.
          const sourceValues = {
            name: agentName?.trim() || formValues.name,
            serviceType: actorDefinitionId!,
            connectionConfiguration: formValues.connectionConfiguration as Record<string, unknown>,
          };

          onSubmitSourceStep(sourceValues);
        } else {
          console.error("[useSubmitConfigurationTool] No configuration found or onSubmitSourceStep not provided");
        }
      },
    }),
    [actorDefinitionId, onSubmitSourceStep, getFormValues]
  );
};
