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
      execute: (_args: unknown) => {
        // Get the current form values at execution time
        const formValues = getFormValues();

        if (formValues.connectionConfiguration && onSubmitSourceStep) {
          // Use form values directly - they contain actual secrets
          const sourceValues = {
            name: formValues.name,
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
