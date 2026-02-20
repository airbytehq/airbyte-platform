import classNames from "classnames";
import { useCallback, useRef, useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, matchPath } from "react-router-dom";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ChatTextInput } from "components/ui/support/ChatTextInput";
import { Text } from "components/ui/Text";

import { useCurrentConnectionIdOptional } from "area/connection/utils";
import { ChatInterfaceBody, ChatInterfaceContainer, ChatInterfaceHeader } from "area/connector/components/chat";
import { useChatMessages } from "area/connector/components/chat/hooks/useChatMessages";
import { MessageList } from "area/connector/components/chat/MessageList";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentUser } from "core/services/auth";
import { useFeature, FeatureItem } from "core/services/features";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { RoutePaths, SourcePaths, DestinationPaths } from "pages/routePaths";

import { AutoScrollToggle } from "./AutoScrollToggle";
import styles from "./SupportAgentWidget.module.scss";
import { SupportChatPanelPortal } from "./SupportChatPanelPortal";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
import { useSupportChatPanelState } from "./useSupportChatPanelState";

// Routes where support bot should be hidden to avoid conflict with setup bot or other assistant buttons
const HIDDEN_SUPPORT_BOT_PATHS = [
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Source}/${SourcePaths.SourceNew}`,
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Destination}/${DestinationPaths.DestinationNew}`,
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.ConnectorBuilder}/edit/*`,
];

// Inner component that uses the chat hook - only rendered when widget is open
export const SupportChatPanel: React.FC<{
  workspaceId: string;
  connectionId?: string;
  isExpanded: boolean;
  setIsExpanded: (value: boolean) => void;
  onClose: () => void;
}> = ({ workspaceId, connectionId, isExpanded, setIsExpanded, onClose }) => {
  const user = useCurrentUser();
  const { pathname } = useLocation();
  const { trackChatLinkClicked, trackMessageSent, trackTicketCreated } = useAnalyticsTrackFunctions();
  const [autoScrollEnabled, setAutoScrollEnabled] = useLocalStorage("airbyte_support-chat-autoscroll", true);
  const trackedTicketIdsRef = useRef<Set<string>>(new Set());
  const { messages, sendMessage, isLoading, error, stopGenerating, isStreaming } = useChatMessages({
    endpoint: "/agents/support/chat",
    prompt: "Introduce yourself as an AI support agent and briefly outline your main functions using emojis.",
    agentParams: {
      workspace_id: workspaceId,
      email: user.email,
      current_page_path: pathname,
      ...(connectionId && { connection_id: connectionId }),
    },
    clientTools: {},
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

  // Send message and track
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

  return (
    <div className={isExpanded ? styles.panelExpanded : styles.panel}>
      <ChatInterfaceContainer className={styles.chatContainer}>
        {/* Custom Header */}
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
              <Badge variant="blue">
                <FormattedMessage id="ui.badge.beta" />
              </Badge>
            </FlexContainer>
            <FlexContainer direction="row" gap="xs" alignItems="center">
              <AutoScrollToggle checked={autoScrollEnabled} onChange={setAutoScrollEnabled} />
              <Button
                variant="clear"
                size="xs"
                onClick={() => setIsExpanded(!isExpanded)}
                icon={isExpanded ? "shrink" : "expand"}
                aria-label={isExpanded ? "Compact view" : "Expand view"}
                className={styles.headerButton}
              />
              <Button
                variant="clear"
                size="xs"
                onClick={onClose}
                icon="cross"
                aria-label="Close support chat"
                className={styles.headerButton}
              />
            </FlexContainer>
          </FlexContainer>
        </ChatInterfaceHeader>

        {/* Workspace indicator */}
        <div className={styles.workspaceIndicator}>
          <Text size="xs" color="grey">
            <FormattedMessage id="chat.supportAgent.workspace" values={{ workspaceId }} />
          </Text>
        </div>
        {/* Chat Body */}
        <ChatInterfaceBody>
          <MessageList
            messages={messages}
            isLoading={isLoading}
            error={error}
            showAllToolCalls
            isVisible
            onLinkClick={handleLinkClick}
            autoScroll={autoScrollEnabled}
          />

          <ChatTextInput onSendMessage={handleSendMessage} onStop={stopGenerating} isStreaming={isStreaming} />
        </ChatInterfaceBody>
      </ChatInterfaceContainer>
    </div>
  );
};

export const SupportAgentWidget: React.FC = () => {
  const supportEnabled = useFeature(FeatureItem.SupportAgentBot);
  const workspaceId = useCurrentWorkspaceId();
  const connectionId = useCurrentConnectionIdOptional();
  const { pathname } = useLocation();
  const { isOpen, setIsOpen, isExpanded, setIsExpanded, hasBeenOpened, setHasBeenOpened } = useSupportChatPanelState();

  // Don't render if feature disabled
  if (!supportEnabled) {
    return null;
  }

  // Hide if on connector creation routes or no workspace context
  const shouldHide = !workspaceId || HIDDEN_SUPPORT_BOT_PATHS.some((path) => !!matchPath(path, pathname));

  if (shouldHide) {
    return null;
  }

  return (
    <>
      <Button
        variant="magic"
        size="xs"
        icon="chat"
        iconSize="md"
        type="button"
        onClick={() => {
          setHasBeenOpened(true);
          setIsOpen(true);
        }}
        className={classNames(styles.button, { [styles.hidden]: isOpen })}
      />
      {hasBeenOpened && (
        <SupportChatPanelPortal isOpen={isOpen}>
          <SupportChatPanel
            workspaceId={workspaceId}
            connectionId={connectionId}
            isExpanded={isExpanded}
            setIsExpanded={setIsExpanded}
            onClose={() => setIsOpen(false)}
          />
        </SupportChatPanelPortal>
      )}
    </>
  );
};
