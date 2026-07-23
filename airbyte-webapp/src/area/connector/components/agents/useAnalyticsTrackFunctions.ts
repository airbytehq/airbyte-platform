import { useCallback } from "react";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

export const useConnectorSetupAgentAnalytics = () => {
  const analyticsService = useAnalyticsService();

  const trackAgentStarted = useCallback(
    (connectorType: "source" | "destination", connectorDefinitionId?: string) => {
      analyticsService.track(Namespace.CONNECTOR_SETUP_AGENT, Action.CONNECTOR_SETUP_AGENT_STARTED, {
        actionDescription: "Connector Setup Agent started",
        connector_type: connectorType,
        ...(connectorDefinitionId && { connector_definition_id: connectorDefinitionId }),
      });
    },
    [analyticsService]
  );

  const trackMessageSent = useCallback(
    (userMessageCount: number, totalMessageCount: number) => {
      analyticsService.track(Namespace.CONNECTOR_SETUP_AGENT, Action.CONNECTOR_SETUP_AGENT_MESSAGE_SENT, {
        actionDescription: "User message sent in Connector Setup Agent chat",
        user_message_count: userMessageCount,
        total_message_count: totalMessageCount,
      });
    },
    [analyticsService]
  );

  const trackConfigurationChecked = useCallback(
    (connectorType: "source" | "destination", connectorDefinitionId?: string, success?: boolean) => {
      analyticsService.track(Namespace.CONNECTOR_SETUP_AGENT, Action.CONNECTOR_SETUP_AGENT_CONFIGURATION_CHECKED, {
        actionDescription: "Configuration check attempted in Connector Setup Agent",
        connector_type: connectorType,
        ...(connectorDefinitionId && { connector_definition_id: connectorDefinitionId }),
        ...(success !== undefined && { success }),
      });
    },
    [analyticsService]
  );

  const trackConfigurationSubmitted = useCallback(
    (connectorType: "source" | "destination", connectorDefinitionId?: string, success?: boolean) => {
      analyticsService.track(Namespace.CONNECTOR_SETUP_AGENT, Action.CONNECTOR_SETUP_AGENT_CONFIGURATION_SUBMITTED, {
        actionDescription: "Configuration submitted via Connector Setup Agent",
        connector_type: connectorType,
        ...(connectorDefinitionId && { connector_definition_id: connectorDefinitionId }),
        ...(success !== undefined && { success }),
      });
    },
    [analyticsService]
  );

  return {
    trackAgentStarted,
    trackMessageSent,
    trackConfigurationChecked,
    trackConfigurationSubmitted,
  };
};
