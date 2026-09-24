import { type MutableRefObject, useMemo } from "react";

import { type ConnectorCardValues, type ConnectorFormValues } from "area/connector/components/ConnectorForm/types";
import { useTestConnectorCommand } from "core/api";
import { ActorType } from "core/api/types/AirbyteClient";
import { type ConnectorT } from "core/domain/connector";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { TOOL_NAMES } from "../toolNames";

type AgentConnectorValues = ConnectorCardValues & { createAsDraft: true; setupFlow: "agent" };

export interface UseCheckConfigurationToolParams<T extends ConnectorT> {
  actorDefinitionId?: string;
  actorType?: ActorType;
  getFormValues: () => ConnectorFormValues;
  onCheckComplete?: (success: boolean) => void;
  onSaveDraft?: (values: AgentConnectorValues, existingDraft?: T) => Promise<T>;
  draftConnectorRef?: MutableRefObject<T | undefined>;
  draftCheckSucceededRef?: MutableRefObject<boolean>;
}

export const useCheckConfigurationTool = <T extends ConnectorT>({
  actorDefinitionId,
  actorType,
  getFormValues,
  onCheckComplete,
  onSaveDraft,
  draftConnectorRef,
  draftCheckSucceededRef,
}: UseCheckConfigurationToolParams<T>): ClientToolHandler => {
  const { testConnector } = useTestConnectorCommand({
    formType: actorType || "source",
  });

  return useMemo(
    () => ({
      toolName: TOOL_NAMES.CHECK_CONFIGURATION,
      execute: async (_args: unknown, sendResult) => {
        if (draftCheckSucceededRef) {
          draftCheckSucceededRef.current = false;
        }
        // Get the current form values at execution time
        const formValues = getFormValues();
        const configuration = formValues.connectionConfiguration;

        if (configuration && actorDefinitionId) {
          try {
            const connectorValues = {
              ...formValues,
              name: formValues.name || "Test Configuration",
              serviceType: actorDefinitionId,
              createAsDraft: true,
              setupFlow: "agent",
            } as AgentConnectorValues;
            const savedDraft = onSaveDraft ? await onSaveDraft(connectorValues, draftConnectorRef?.current) : undefined;

            if (savedDraft && draftConnectorRef) {
              draftConnectorRef.current = savedDraft;
            }

            const result = savedDraft
              ? await testConnector(undefined, savedDraft)
              : await testConnector(connectorValues);

            if (result.status !== "succeeded") {
              sendResult(
                JSON.stringify({
                  success: false,
                  message: "Configuration test did not complete successfully",
                  status: result.status,
                })
              );
              onCheckComplete?.(false);
              return;
            }

            if (savedDraft && draftCheckSucceededRef) {
              draftCheckSucceededRef.current = true;
            }

            sendResult(
              JSON.stringify({
                success: true,
                message: "Configuration test passed successfully",
                status: result.status,
              })
            );
            onCheckComplete?.(true);
          } catch (error) {
            console.error("[useCheckConfigurationTool] Configuration test failed:", error);
            sendResult(
              JSON.stringify({
                success: false,
                message: error instanceof Error ? error.message : "Configuration test failed",
                error: error instanceof Error ? error.toString() : String(error),
              })
            );
            onCheckComplete?.(false);
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
    [
      actorDefinitionId,
      draftConnectorRef,
      getFormValues,
      onCheckComplete,
      onSaveDraft,
      draftCheckSucceededRef,
      testConnector,
    ]
  );
};
