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

  const trackMessageSent = useCallback(
    (messageContent: string, userMessageCount: number, totalMessageCount: number) => {
      analyticsService.track(Namespace.SUPPORT_AGENT_BOT, Action.CHAT_MESSAGE_SENT, {
        actionDescription: "User message sent in Support Agent Bot chat",
        message_content: messageContent,
        user_message_count: userMessageCount,
        total_message_count: totalMessageCount,
      });
    },
    [analyticsService]
  );

  const trackTicketCreated = useCallback(
    (ticketId?: string) => {
      analyticsService.track(Namespace.SUPPORT_AGENT_BOT, Action.CHAT_TICKET_CREATED, {
        actionDescription: "Zendesk ticket created via Support Agent Bot",
        ...(ticketId && { ticket_id: ticketId }),
      });
    },
    [analyticsService]
  );

  return {
    trackChatInitiated,
    trackChatLinkClicked,
    trackMessageSent,
    trackTicketCreated,
  };
};
