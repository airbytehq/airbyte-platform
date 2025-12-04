import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { ChatInput } from "./ChatInput";
import styles from "./ChatInterface.module.scss";
import { type ChatMessage } from "./Message";
import { MessageList } from "./MessageList";
import { type ToolCallProps } from "./ToolCallItem";

export interface ChatInterfaceProps {
  messages: ChatMessage[];
  isLoading: boolean;
  error: string | null;
  isStreaming: boolean;
  onSendMessage: (message: string) => void;
  onStop: () => void;
  isSecretMode?: boolean;
  secretFieldPath?: string[];
  secretFieldName?: string;
  isMultiline?: boolean;
  toolComponents?: Record<string, React.ComponentType<ToolCallProps>>;
  showAllToolCalls?: boolean;
}

export const ChatInterface: React.FC<ChatInterfaceProps> = ({
  messages,
  isLoading,
  error,
  isStreaming,
  onSendMessage,
  onStop,
  isSecretMode = false,
  secretFieldPath = [],
  secretFieldName,
  isMultiline = false,
  toolComponents,
  showAllToolCalls = false,
}) => {
  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <FlexContainer direction="row" alignItems="center" gap="sm">
          <Icon type="aiStars" color="magic" size="md" />
          <Heading as="h3" size="sm" className={styles.headerTitle}>
            <FormattedMessage id="connectorSetup.agent.title" />
          </Heading>
          <Badge variant="blue">
            <FormattedMessage id="ui.badge.beta" />
          </Badge>
        </FlexContainer>
        <Text size="sm" color="grey" className={styles.headerDescription}>
          <FormattedMessage id="connectorSetup.agent.description" />
        </Text>
      </div>
      <div className={styles.chatContainer}>
        <MessageList
          messages={messages}
          isLoading={isLoading}
          error={error}
          toolComponents={toolComponents}
          showAllToolCalls={showAllToolCalls}
        />
        <ChatInput
          onSendMessage={onSendMessage}
          onStop={onStop}
          isStreaming={isStreaming}
          disabled={isSecretMode ? false : isLoading}
          isSecretMode={isSecretMode}
          secretFieldPath={secretFieldPath}
          secretFieldName={secretFieldName}
          isMultiline={isMultiline}
        />
      </div>
    </div>
  );
};
