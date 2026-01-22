import { useCallback } from "react";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

export const useAnalyticsTrackFunctions = () => {
  const analyticsService = useAnalyticsService();

  const trackChatInitiated = useCallback(() => {
    analyticsService.track(Namespace.SUPPORT_AGENT_BOT, Action.CHAT_INITIATED, {
      actionDescription: "Support Agent Bot chat initiated",
    });
  }, [analyticsService]);

  const trackChatLinkClicked = useCallback(
    (linkUrl: string, linkText?: string) => {
      analyticsService.track(Namespace.SUPPORT_AGENT_BOT, Action.CHAT_LINK_CLICKED, {
        actionDescription: "Link clicked in Support Agent Bot chat",
        link_url: linkUrl,
        ...(linkText && { link_text: linkText }),
      });
    },
    [analyticsService]
  );

  return {
    trackChatInitiated,
    trackChatLinkClicked,
  };
};
