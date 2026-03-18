import { useCallback, useRef, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation, matchPath } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ChatTextInput } from "components/ui/support/ChatTextInput";
import { Text } from "components/ui/Text";

import { ChatInterfaceBody, ChatInterfaceContainer, ChatInterfaceHeader } from "area/connector/components/chat";
import { useChatMessages } from "area/connector/components/chat/hooks/useChatMessages";
import { MessageList } from "area/connector/components/chat/MessageList";
import { useSupportAgentService } from "cloud/services/supportAgent";
import { useCurrentUser } from "core/services/auth";
import { useFeature, FeatureItem } from "core/services/features";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { RoutePaths, SourcePaths, DestinationPaths } from "pages/routePaths";

import { AutoScrollToggle } from "./AutoScrollToggle";
import styles from "./SupportAgentFloatingButton.module.scss";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";

// Routes where support bot should be hidden to avoid conflict with setup bot or other assistant buttons
const HIDDEN_SUPPORT_BOT_PATHS = [
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Source}/${SourcePaths.SourceNew}`,
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Destination}/${DestinationPaths.DestinationNew}`,
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.ConnectorBuilder}/edit/*`,
];

export const SupportChatPanel: React.FC<{
  workspaceId?: string;
  connectionId?: string;
  isExpanded: boolean;
  setIsExpanded: (value: boolean) => void;
  onClose: () => void;
  onNewConversation: () => void;
}> = ({ workspaceId, connectionId, isExpanded, setIsExpanded, onClose, onNewConversation }) => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();
  const { pathname } = useLocation();
  const { trackChatLinkClicked, trackMessageSent, trackTicketCreated } = useAnalyticsTrackFunctions();
  const [autoScrollEnabled, setAutoScrollEnabled] = useLocalStorage("airbyte_support-chat-autoscroll", true);
  const [threadId, setThreadId] = useLocalStorage("airbyte_support-chat-thread-id", "");
  const trackedTicketIdsRef = useRef<Set<string>>(new Set());

  const handleThreadIdChange = useCallback(
    (newThreadId: string) => {
      setThreadId(newThreadId);
    },
    [setThreadId]
  );

  const { messages, sendMessage, isLoading, error, stopGenerating, isStreaming } = useChatMessages({
    endpoint: "/agents/support/chat",
    prompt:
      "Introduce yourself as an AI support agent and briefly outline your main functions using emojis. Make sure to mention that you can help open a Zendesk ticket with the Airbyte Support team.",
    agentParams: {
      email: user.email,
      current_page_path: pathname,
      ...(workspaceId && { workspace_id: workspaceId }),
      ...(connectionId && { connection_id: connectionId }),
    },
    clientTools: {},
    initialThreadId: threadId || undefined,
    onThreadIdChange: handleThreadIdChange,
  });

  useEffect(() => {
    for (const msg of messages) {
      if (
        msg.role === "tool" &&
        msg.toolResponse?.tool_name === "create_zendesk_ticket" &&
        typeof msg.toolResponse.response === "string" &&
        msg.toolResponse.response.includes("Successfully created Zendesk ticket")
      ) {
        const callId = msg.toolResponse.call_id;
        if (callId && !trackedTicketIdsRef.current.has(callId)) {
          trackedTicketIdsRef.current.add(callId);
          const ticketIdMatch = msg.toolResponse.response.match(/ticket (\d+)/);
          trackTicketCreated(ticketIdMatch?.[1]);
        }
      }
    }
  }, [messages, trackTicketCreated]);

  const handleSendMessage = useCallback(
    (content: string) => {
      const userMessageCount = messages.filter((msg) => msg.role === "user").length + 1;
      const totalMessageCount = messages.length + 1;
      sendMessage(content);
      trackMessageSent(content, userMessageCount, totalMessageCount);
    },
    [messages, sendMessage, trackMessageSent]
  );

  const handleLinkClick = useCallback(
    (url: string, text: string) => {
      trackChatLinkClicked(url, text);
    },
    [trackChatLinkClicked]
  );

  const handleNewConversationClick = useCallback(() => {
    setThreadId("");
    onNewConversation();
  }, [setThreadId, onNewConversation]);

  return (
    <div className={isExpanded ? styles.panelExpanded : styles.panel}>
      <ChatInterfaceContainer className={styles.chatContainer}>
        <ChatInterfaceHeader className={styles.header}>
          <FlexContainer
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            className={styles.headerContent}
          >
            <FlexContainer direction="row" alignItems="center" gap="sm">
              <Icon type="chat" color="magic" />
              <Heading as="h3" size="sm">
                <FormattedMessage id="chat.supportAgent.title" />
              </Heading>
            </FlexContainer>
            <FlexContainer direction="row" gap="xs" alignItems="center">
              <AutoScrollToggle checked={autoScrollEnabled} onChange={setAutoScrollEnabled} />
              <Button
                variant="clear"
                size="xs"
                onClick={handleNewConversationClick}
                icon="reset"
                aria-label={formatMessage({ id: "chat.supportAgent.newConversation" })}
                title={formatMessage({ id: "chat.supportAgent.newConversation" })}
                className={styles.headerButton}
              />
              <Button
                variant="clear"
                size="xs"
                onClick={() => setIsExpanded(!isExpanded)}
                icon={isExpanded ? "shrink" : "expand"}
                aria-label={formatMessage({
                  id: isExpanded ? "chat.supportAgent.compactView" : "chat.supportAgent.expandView",
                })}
                title={formatMessage({
                  id: isExpanded ? "chat.supportAgent.compactView" : "chat.supportAgent.expandView",
                })}
                className={styles.headerButton}
              />
              <Button
                variant="clear"
                size="xs"
                onClick={onClose}
                icon="cross"
                aria-label={formatMessage({ id: "chat.supportAgent.close" })}
                title={formatMessage({ id: "chat.supportAgent.close" })}
                className={styles.headerButton}
              />
            </FlexContainer>
          </FlexContainer>
        </ChatInterfaceHeader>

        {workspaceId && (
          <div className={styles.workspaceIndicator}>
            <Text size="xs" color="grey">
              <FormattedMessage id="chat.supportAgent.workspace" values={{ workspaceId }} />
            </Text>
          </div>
        )}
        <ChatInterfaceBody>
          <MessageList
            messages={messages}
            isLoading={isLoading}
            error={error}
            showAllToolCalls
            isVisible
            onLinkClick={handleLinkClick}
            autoScroll={autoScrollEnabled}
            isRestoredConversation={!!threadId}
          />

          <ChatTextInput onSendMessage={handleSendMessage} onStop={stopGenerating} isStreaming={isStreaming} />
        </ChatInterfaceBody>
      </ChatInterfaceContainer>
    </div>
  );
};

export const SupportAgentFloatingButton: React.FC = () => {
  const supportEnabled = useFeature(FeatureItem.SupportAgentBot);
  const { pathname } = useLocation();
  const { openSupportBot } = useSupportAgentService();

  const shouldHide = !supportEnabled || HIDDEN_SUPPORT_BOT_PATHS.some((path) => !!matchPath(path, pathname));

  if (shouldHide) {
    return null;
  }

  return (
    <Button
      variant="magic"
      size="xs"
      icon="chat"
      iconSize="md"
      type="button"
      onClick={openSupportBot}
      className={styles.button}
    />
  );
};
