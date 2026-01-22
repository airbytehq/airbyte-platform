import { useCallback } from "react";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

export const useAnalyticsTrackFunctions = () => {
  const analyticsService = useAnalyticsService();

  const trackChatInitiated = useCallback(() => {
    analyticsService.track(Namespace.SUPPORT_AGENT_BOT, Action.CHAT_INITIATED, {
      actionDescription: "Support Agent Bot chat initiated",
    });
  }, [analyticsService]);

  return {
    trackChatInitiated,
  };
};
