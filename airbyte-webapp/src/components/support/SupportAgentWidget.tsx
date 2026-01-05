import { useState } from "react";
import { createPortal } from "react-dom";
import { FormattedMessage } from "react-intl";

import { ChatInterfaceBody, ChatInterfaceContainer, ChatInterfaceHeader } from "components/chat";
import { ChatInput } from "components/chat/ChatInput";
import { useChatMessages } from "components/chat/hooks/useChatMessages";
import { MessageList } from "components/chat/MessageList";
import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useFeature, FeatureItem } from "core/services/features";

import styles from "./SupportAgentWidget.module.scss";

// Inner component that uses the chat hook - only rendered when widget is open
const SupportChatPanel: React.FC<{
  workspaceId: string;
  isExpanded: boolean;
  setIsExpanded: (value: boolean) => void;
  onClose: () => void;
}> = ({ workspaceId, isExpanded, setIsExpanded, onClose }) => {
  const { messages, sendMessage, isLoading, error, stopGenerating, isStreaming } = useChatMessages({
    endpoint: "/agents/support/chat",
    agentParams: {
      workspace_id: workspaceId,
      prompt: "Introduce yourself as an AI assistant and outline your main functions.",
    },
  });

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
              />
              <Button variant="clear" size="xs" onClick={onClose} icon="cross" aria-label="Close support chat" />
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
          <MessageList messages={messages} isLoading={isLoading} error={error} showAllToolCalls isVisible />

          <ChatInput
            onSendMessage={sendMessage}
            onStop={stopGenerating}
            isStreaming={isStreaming}
            disabled={isLoading}
            isVisible
          />
        </ChatInterfaceBody>
      </ChatInterfaceContainer>
    </div>
  );
};

export const SupportAgentWidget: React.FC = () => {
  const supportEnabled = useFeature(FeatureItem.SupportAgentBot);
  const workspaceId = useCurrentWorkspaceId();

  // Widget display state
  const [isOpen, setIsOpen] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [hasBeenOpened, setHasBeenOpened] = useState(false);

  // Don't render if feature disabled or no workspace context
  if (!supportEnabled || !workspaceId) {
    return null;
  }

  return createPortal(
    <div className={styles.widget}>
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
