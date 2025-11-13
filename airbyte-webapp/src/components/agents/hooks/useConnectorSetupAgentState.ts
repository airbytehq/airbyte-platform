import { useCallback, useEffect, useRef, useState } from "react";

import { maskSecrets } from "../../../area/connector/utils/maskSecrets";
import { type ClientTools, useChatMessages } from "../../chat/hooks/useChatMessages";
import { HIDDEN_MESSAGE_PREFIX } from "../../chat/Message";
import { type SecretsMap } from "../types";
import { buildConfigurationChangeMessage } from "../utils/buildConfigurationChangeMessage";

interface UseConnectorSetupAgentStateParams {
  actorType: "source" | "destination";
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
}

export const useConnectorSetupAgentState = ({
  actorType,
  actorDefinitionId,
  connectionSpecification,
  isAgentView,
}: UseConnectorSetupAgentStateParams) => {
  // Secrets state management
  const [secrets, setSecrets] = useState<SecretsMap>(new Map());
  const secretsRef = useRef<SecretsMap>(secrets);
  useEffect(() => {
    secretsRef.current = secrets;
  }, [secrets]);

  const getSecrets = useCallback(() => secretsRef.current, []);

  // Client tools state
  const [clientTools, setClientTools] = useState<ClientTools | null>(null);
  const [secretInputState, setSecretInputState] = useState<SecretInputState>({
    isSecretInputActive: false,
    secretFieldPath: [],
    secretFieldName: undefined,
    isMultiline: false,
    submitSecret: (() => {}) as (message: string) => void,
  });

  // Form values tracking
  const getFormValuesRef = useRef<(() => Record<string, unknown>) | null>(null);
  const lastFormSnapshotRef = useRef<string | null>(null);

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
      const currentSnapshot = createMaskedSnapshot(currentFormValues);
      const lastSnapshot = lastFormSnapshotRef.current;

      // If we have a previous snapshot, check for changes
      if (lastSnapshot) {
        const hasChanges = currentSnapshot !== lastSnapshot;

        if (hasChanges) {
          const currentMaskedValues = JSON.parse(currentSnapshot) as Record<string, unknown>;
          const previousMaskedValues = JSON.parse(lastSnapshot) as Record<string, unknown>;

          const messageBody = buildConfigurationChangeMessage({
            currentFormValues: currentMaskedValues,
            previousFormValues: previousMaskedValues,
          });

          sendMessage(`${HIDDEN_MESSAGE_PREFIX}\n${messageBody}`);
        }
      }

      // Update snapshot when switching to agent view
      lastFormSnapshotRef.current = currentSnapshot;
    }

    // Switching from agent to form view - update snapshot
    if (!isAgentView && previousIsAgentView && getFormValuesRef.current) {
      const currentFormValues = getFormValuesRef.current();
      lastFormSnapshotRef.current = createMaskedSnapshot(currentFormValues);
    }

    previousIsAgentViewRef.current = isAgentView;
  }, [isAgentView, sendMessage, createMaskedSnapshot]);

  return {
    // Secrets management
    setSecrets,
    getSecrets,

    // Client tools
    handleClientToolsReady,

    // Secret input state
    secretInputState,
    handleSecretInputStateChange,

    // Form values tracking
    handleFormValuesReady,

    // Chat messages
    messages,
    sendMessage,
    isLoading,
    error,
    stopGenerating,
    isStreaming,
  };
};
