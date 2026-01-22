import classNames from "classnames";
import { useState, useEffect, useCallback } from "react";
import { createPortal } from "react-dom";
import { FormattedMessage } from "react-intl";
import { useLocation, matchPath } from "react-router-dom";

import { ChatInterfaceBody, ChatInterfaceContainer, ChatInterfaceHeader } from "components/chat";
import { useChatMessages } from "components/chat/hooks/useChatMessages";
import { MessageList } from "components/chat/MessageList";
import { ChatTextInput } from "components/support/ChatTextInput";
import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentConnectionIdOptional } from "area/connection/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentUser } from "core/services/auth";
import { useFeature, FeatureItem } from "core/services/features";
import { RoutePaths, SourcePaths, DestinationPaths } from "pages/routePaths";

import styles from "./SupportAgentWidget.module.scss";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";

// Routes where support bot should be hidden to avoid conflict with setup bot
const HIDDEN_SUPPORT_BOT_PATHS = [
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Source}/${SourcePaths.SourceNew}`,
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Destination}/${DestinationPaths.DestinationNew}`,
];

// Inner component that uses the chat hook - only rendered when widget is open
const SupportChatPanel: React.FC<{
  workspaceId: string;
  connectionId?: string;
  isExpanded: boolean;
  setIsExpanded: (value: boolean) => void;
  onClose: () => void;
}> = ({ workspaceId, connectionId, isExpanded, setIsExpanded, onClose }) => {
  const user = useCurrentUser();
  const { trackChatLinkClicked, trackMessageSent } = useAnalyticsTrackFunctions();
  const { messages, sendMessage, isLoading, error, stopGenerating, isStreaming } = useChatMessages({
    endpoint: "/agents/support/chat",
    prompt: "Introduce yourself as an AI support agent and briefly outline your main functions using emojis.",
    agentParams: {
      workspace_id: workspaceId,
      email: user.email,
      ...(connectionId && { connection_id: connectionId }),
    },
    clientTools: {},
  });

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
            <FlexContainer direction="row" gap="xs">
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
  const { trackChatInitiated } = useAnalyticsTrackFunctions();

  // Widget display state
  const [isOpen, setIsOpen] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [hasBeenOpened, setHasBeenOpened] = useState(false);

  // Track chat initiated only once when first opened
  useEffect(() => {
    if (hasBeenOpened) {
      trackChatInitiated();
    }
  }, [hasBeenOpened, trackChatInitiated]);

  // Don't render if feature disabled
  if (!supportEnabled) {
    return null;
  }

  // Hide if on connector creation routes or no workspace context
  const shouldHide = !workspaceId || HIDDEN_SUPPORT_BOT_PATHS.some((path) => !!matchPath(path, pathname));

  return createPortal(
    <div className={classNames(styles.widget, { [styles.hidden]: shouldHide })}>
      {!isOpen && (
        // Collapsed button
        <Button
          variant="magic"
          size="sm"
          onClick={() => {
            setHasBeenOpened(true);
            setIsOpen(true);
          }}
          icon="chat"
          className={styles.button}
        />
      )}
      {hasBeenOpened && (
        // Chat panel - keep mounted to preserve context, hide with CSS when closed
        <div style={{ display: isOpen ? "flex" : "none" }}>
          <SupportChatPanel
            workspaceId={workspaceId}
            connectionId={connectionId}
            isExpanded={isExpanded}
            setIsExpanded={setIsExpanded}
            onClose={() => setIsOpen(false)}
          />
        </div>
      )}
    </div>,
    document.body
  );
};
