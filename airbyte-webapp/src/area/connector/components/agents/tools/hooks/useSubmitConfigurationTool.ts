import { useMemo } from "react";

import { type ConnectorCardValues, type ConnectorFormValues } from "area/connector/components/ConnectorForm/types";
import { type ConnectorT } from "core/domain/connector";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { TOOL_NAMES } from "../toolNames";

type AgentConnectorValues = ConnectorCardValues & { setupFlow: "agent" };

export interface UseSubmitConfigurationToolParams<T extends ConnectorT> {
  actorDefinitionId?: string;
  onSubmitSourceStep?: (sourceValues: {
    name: string;
    serviceType: string;
    connectionConfiguration: Record<string, unknown>;
  }) => Promise<void> | void;
  getFormValues: () => ConnectorFormValues;
  getDraftConnector?: () => T | undefined;
  wasDraftCheckSuccessful?: () => boolean;
  onDraftPromoted?: (draft: T, values: AgentConnectorValues) => Promise<void> | void;
}

export const useSubmitConfigurationTool = <T extends ConnectorT>({
  actorDefinitionId,
  onSubmitSourceStep,
  getFormValues,
  getDraftConnector,
  wasDraftCheckSuccessful,
  onDraftPromoted,
}: UseSubmitConfigurationToolParams<T>): ClientToolHandler => {
  return useMemo(
    () => ({
      toolName: TOOL_NAMES.SUBMIT_CONFIGURATION,
      execute: async (args: unknown) => {
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

          if (getDraftConnector) {
            const draft = getDraftConnector();
            if (draft && onDraftPromoted && wasDraftCheckSuccessful?.()) {
              await onDraftPromoted(draft, {
                ...formValues,
                ...sourceValues,
                setupFlow: "agent",
              });
            }
          } else {
            await onSubmitSourceStep(sourceValues);
          }
        } else {
          console.error("[useSubmitConfigurationTool] No configuration found or onSubmitSourceStep not provided");
        }
      },
    }),
    [actorDefinitionId, getDraftConnector, getFormValues, onDraftPromoted, onSubmitSourceStep, wasDraftCheckSuccessful]
  );
};
