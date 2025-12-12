import { useCallback, useEffect, useRef, useState } from "react";

import { ActorType } from "core/api/types/AirbyteClient";

import { maskSecrets } from "../../../area/connector/utils/maskSecrets";
import { type ClientTools, useChatMessages } from "../../chat/hooks/useChatMessages";
import { HIDDEN_MESSAGE_PREFIX } from "../../chat/Message";
import { buildConfigurationChangeMessage } from "../utils/buildConfigurationChangeMessage";
import { findChangedFieldPaths } from "../utils/findChangedFieldPaths";
import { isSecretField } from "../utils/isSecretField";

interface UseConnectorSetupAgentStateParams {
  actorType: ActorType;
  actorDefinitionId: string | undefined;
  connectionSpecification: Record<string, unknown> | undefined;
  isAgentView: boolean;
}

export interface SecretInputState {
  isSecretInputActive: boolean;
  secretFieldPath: string[];
  secretFieldName: string | undefined;
  isMultiline: boolean;
  submitSecret: (message: string) => void;
  dismissSecret: (reason?: string) => void;
}

export const useConnectorSetupAgentState = ({
  actorType,
  actorDefinitionId,
  connectionSpecification,
  isAgentView,
}: UseConnectorSetupAgentStateParams) => {
  // Client tools state
  const [clientTools, setClientTools] = useState<ClientTools | null>(null);
  const [secretInputState, setSecretInputState] = useState<SecretInputState>({
    isSecretInputActive: false,
    secretFieldPath: [],
    secretFieldName: undefined,
    isMultiline: false,
    submitSecret: (() => {}) as (message: string) => void,
    dismissSecret: (() => {}) as () => void,
  });

  // Form values tracking
  const getFormValuesRef = useRef<(() => Record<string, unknown>) | null>(null);
  const lastMaskedFormSnapshotRef = useRef<string | null>(null);
  const lastUnmaskedFormSnapshotRef = useRef<string | null>(null);

  // Touched secret fields tracking
  const touchedSecretFieldsRef = useRef<Set<string>>(new Set());

  // Handlers
  const handleClientToolsReady = useCallback((tools: ClientTools) => {
    setClientTools(tools);
  }, []);

  const handleSecretInputStateChange = useCallback((state: SecretInputState) => {
    setSecretInputState(state);
  }, []);

  const handleFormValuesReady = useCallback((getFormValues: () => Record<string, unknown>) => {
    getFormValuesRef.current = getFormValues;
  }, []);

  const addTouchedSecretField = useCallback((path: string) => {
    touchedSecretFieldsRef.current.add(path);
  }, []);

  // Agent integration
  const { messages, sendMessage, isLoading, error, stopGenerating, isStreaming } = useChatMessages({
    endpoint: "/agents/connector_setup/chat",
    agentParams: {
      actor_definition_id: actorDefinitionId,
      actor_type: actorType,
    },
    clientTools: clientTools || {},
  });

  // Helper to create masked snapshot of the form values
  const createMaskedSnapshot = useCallback(
    (formValues: Record<string, unknown>): string => {
      if (!connectionSpecification) {
        return JSON.stringify(formValues);
      }

      const config = formValues.connectionConfiguration as Record<string, unknown> | undefined;
      const maskedFormValues = {
        ...formValues,
        connectionConfiguration: config ? maskSecrets(config, connectionSpecification) : config,
      };

      return JSON.stringify(maskedFormValues);
    },
    [connectionSpecification]
  );

  // Change detection and view switching
  const previousIsAgentViewRef = useRef<boolean>(isAgentView);
  useEffect(() => {
    const previousIsAgentView = previousIsAgentViewRef.current;

    // Switching from form to agent view
    if (isAgentView && !previousIsAgentView && getFormValuesRef.current) {
      const currentFormValues = getFormValuesRef.current();
      const currentMaskedSnapshot = createMaskedSnapshot(currentFormValues);
      const currentUnmaskedSnapshot = JSON.stringify(currentFormValues);
      const lastMaskedSnapshot = lastMaskedFormSnapshotRef.current;
      const lastUnmaskedSnapshot = lastUnmaskedFormSnapshotRef.current;

      // If we have a previous snapshot, check for changes using unmasked snapshots for accuracy
      if (lastUnmaskedSnapshot) {
        const hasChanges = currentUnmaskedSnapshot !== lastUnmaskedSnapshot;

        if (hasChanges) {
          // For LLM message, use masked values (security)
          const currentMaskedValues = JSON.parse(currentMaskedSnapshot) as Record<string, unknown>;
          const previousMaskedValues = JSON.parse(lastMaskedSnapshot!) as Record<string, unknown>;

          // For change detection, use unmasked values (accuracy)
          const currentConfig = (currentFormValues.connectionConfiguration as Record<string, unknown>) || {};
          const previousConfig =
            ((JSON.parse(lastUnmaskedSnapshot) as Record<string, unknown>).connectionConfiguration as Record<
              string,
              unknown
            >) || {};

          const changedPaths = findChangedFieldPaths(previousConfig, currentConfig);

          // Only add secret fields to touched fields - this prevents non-secret fields
          // from being incorrectly preserved when the agent tries to update them
          changedPaths.forEach((path) => {
            // Build full schema with connectionConfiguration wrapper for isSecretField
            const formSchema = {
              type: "object",
              properties: {
                connectionConfiguration: connectionSpecification,
              },
            };

            if (connectionSpecification && isSecretField(path, formSchema)) {
              touchedSecretFieldsRef.current.add(path);
            }
          });

          // Build and send message to agent using masked values
          const messageBody = buildConfigurationChangeMessage({
            currentFormValues: currentMaskedValues,
            previousFormValues: previousMaskedValues,
          });

          sendMessage(`${HIDDEN_MESSAGE_PREFIX}\n${messageBody}`);
        }
      }

      // Update snapshots when switching to agent view
      lastMaskedFormSnapshotRef.current = currentMaskedSnapshot;
      lastUnmaskedFormSnapshotRef.current = currentUnmaskedSnapshot;
    }

    // Switching from agent to form view
    if (!isAgentView && previousIsAgentView) {
      // If agent is waiting for secret input, dismiss it with a message
      if (secretInputState.isSecretInputActive) {
        secretInputState.dismissSecret("Cancelled! I'll add it via form.");
      }

      // Take snapshots when entering form view so we can detect changes later
      if (getFormValuesRef.current) {
        const currentFormValues = getFormValuesRef.current();
        lastMaskedFormSnapshotRef.current = createMaskedSnapshot(currentFormValues);
        lastUnmaskedFormSnapshotRef.current = JSON.stringify(currentFormValues);
      }
    }

    previousIsAgentViewRef.current = isAgentView;
  }, [isAgentView, sendMessage, createMaskedSnapshot, secretInputState, connectionSpecification]);

  return {
    // Client tools
    handleClientToolsReady,

    // Secret input state
    secretInputState,
    handleSecretInputStateChange,

    // Form values tracking
    handleFormValuesReady,

    // Touched secret fields
    touchedSecretFieldsRef,
    addTouchedSecretField,

    // Chat messages
    messages,
    sendMessage,
    isLoading,
    error,
    stopGenerating,
    isStreaming,
  };
};
