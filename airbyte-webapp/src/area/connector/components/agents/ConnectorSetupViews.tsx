import React, { useCallback, useEffect, useState } from "react";

import { MountedViewSwapper } from "components/ui/MountedViewSwapper";

import { ConnectionConfiguration } from "area/connector/types";
import { ActorType } from "core/api/types/AirbyteClient";
import { ConnectorT } from "core/domain/connector";

import { ConnectorSetupAgent } from "./ConnectorSetupAgent";
import { type AgentConnectorValues, ConnectorSetupAgentTools } from "./ConnectorSetupAgentTools";
import { useConnectorSetupAgentState } from "./hooks/useConnectorSetupAgentState";
import { useConnectorSetupAgentAnalytics } from "./useAnalyticsTrackFunctions";

interface AgentSubmitValues {
  name: string;
  serviceType: string;
  connectionConfiguration: ConnectionConfiguration;
  setupFlow: "agent";
}

export interface ConnectorSetupAgentConfig<T extends ConnectorT> {
  actorType: ActorType;
  actorDefinitionId?: string;
  connectionSpecification?: Record<string, unknown>;
  /** False for custom connectors and until the spec loads. The agent view and its tools need the spec. */
  isAvailable: boolean;
  onSubmit: (values: AgentSubmitValues) => Promise<void>;
  onSaveDraft?: (values: AgentConnectorValues, existingDraft?: T) => Promise<T>;
  onDraftPromoted?: (draft: T, values: AgentConnectorValues) => Promise<void> | void;
}

interface ConnectorSetupViewsProps<T extends ConnectorT> {
  /**
   * Leave undefined when the agent-assisted setup experiment is off. No agent code runs then.
   * Pass it while the spec loads, too, so the form view does not remount when the agent becomes available.
   */
  agent?: ConnectorSetupAgentConfig<T>;
  isAgentView: boolean;
  /**
   * Renders the form view. The setup form must render `formAutomation` inside its form context,
   * because the agent's tools read and fill the form.
   */
  renderSetupForm: (formAutomation?: React.ReactNode) => React.ReactNode;
}

/**
 * The content of a new-connector page: the setup form view and, when the agent is available, the agent view.
 * Both views stay mounted, so the agent can read and fill the form while its own view shows.
 */
export const ConnectorSetupViews = <T extends ConnectorT>({
  agent,
  isAgentView,
  renderSetupForm,
}: ConnectorSetupViewsProps<T>) =>
  agent ? (
    <AgentAssistedSetupViews
      {...agent}
      showAgentView={agent.isAvailable && isAgentView}
      renderSetupForm={renderSetupForm}
    />
  ) : (
    <>{renderSetupForm()}</>
  );

interface AgentAssistedSetupViewsProps<T extends ConnectorT> extends ConnectorSetupAgentConfig<T> {
  showAgentView: boolean;
  renderSetupForm: (formAutomation?: React.ReactNode) => React.ReactNode;
}

const AgentAssistedSetupViews = <T extends ConnectorT>({
  actorType,
  actorDefinitionId,
  connectionSpecification,
  isAvailable,
  showAgentView,
  onSubmit,
  onSaveDraft,
  onDraftPromoted,
  renderSetupForm,
}: AgentAssistedSetupViewsProps<T>) => {
  const { trackAgentStarted, trackMessageSent, trackConfigurationChecked, trackConfigurationSubmitted } =
    useConnectorSetupAgentAnalytics();
  const [hasTrackedStart, setHasTrackedStart] = useState(false);

  const onSubmitAgentStep = useCallback(
    async (values: Omit<AgentSubmitValues, "setupFlow">) => {
      try {
        await onSubmit({ ...values, setupFlow: "agent" });
        trackConfigurationSubmitted(actorType, actorDefinitionId, true);
      } catch (error) {
        trackConfigurationSubmitted(actorType, actorDefinitionId, false);
        throw error;
      }
    },
    [onSubmit, actorType, actorDefinitionId, trackConfigurationSubmitted]
  );

  const {
    handleClientToolsReady,
    secretInputState,
    handleSecretInputStateChange,
    oauthState,
    handleOAuthStateChange,
    handleFormValuesReady,
    touchedSecretFieldsRef,
    addTouchedSecretField,
    messages,
    sendMessage,
    isLoading,
    error,
    stopGenerating,
    isStreaming,
  } = useConnectorSetupAgentState({
    actorType,
    actorDefinitionId,
    connectionSpecification,
    isAgentView: showAgentView,
  });

  // Track agent started when first entering agent view
  useEffect(() => {
    if (showAgentView && !hasTrackedStart && actorDefinitionId) {
      trackAgentStarted(actorType, actorDefinitionId);
      setHasTrackedStart(true);
    }
  }, [showAgentView, hasTrackedStart, actorType, actorDefinitionId, trackAgentStarted]);

  // Wrap sendMessage to track analytics (without message content for PII safety)
  const handleSendMessage = useCallback(
    (content: string) => {
      const userMessageCount = messages.filter((msg) => msg.role === "user").length + 1;
      const totalMessageCount = messages.length + 1;
      sendMessage(content);
      trackMessageSent(userMessageCount, totalMessageCount);
    },
    [messages, sendMessage, trackMessageSent]
  );

  const handleCheckComplete = useCallback(
    (success: boolean) => {
      trackConfigurationChecked(actorType, actorDefinitionId, success);
    },
    [trackConfigurationChecked, actorType, actorDefinitionId]
  );

  return (
    <MountedViewSwapper
      isFirstView={showAgentView}
      firstView={
        isAvailable ? (
          <ConnectorSetupAgent
            messages={messages}
            isLoading={isLoading}
            error={error}
            isStreaming={isStreaming}
            onSendMessage={secretInputState.isSecretInputActive ? secretInputState.submitSecret : handleSendMessage}
            onStop={stopGenerating}
            isSecretMode={secretInputState.isSecretInputActive}
            secretFieldPath={secretInputState.secretFieldPath}
            secretFieldName={secretInputState.secretFieldName}
            isMultiline={secretInputState.isMultiline}
            isVisible={showAgentView}
            onDismissSecret={secretInputState.dismissSecret}
            oauthState={oauthState}
          />
        ) : null
      }
      secondView={renderSetupForm(
        isAvailable ? (
          <ConnectorSetupAgentTools<T>
            actorDefinitionId={actorDefinitionId}
            actorType={actorType}
            onSubmitStep={onSubmitAgentStep}
            onClientToolsReady={handleClientToolsReady}
            onSecretInputStateChange={handleSecretInputStateChange}
            onOAuthStateChange={handleOAuthStateChange}
            onFormValuesReady={handleFormValuesReady}
            onCheckComplete={handleCheckComplete}
            onSaveDraft={onSaveDraft}
            onDraftPromoted={onDraftPromoted}
            touchedSecretFieldsRef={touchedSecretFieldsRef}
            addTouchedSecretField={addTouchedSecretField}
          />
        ) : undefined
      )}
    />
  );
};
