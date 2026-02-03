import React, { useEffect, useRef } from "react";
import { FormattedMessage } from "react-intl";

import { Message, type ChatMessage } from "./Message";
import styles from "./MessageList.module.scss";
import { type ToolCallProps } from "./ToolCallItem";
import { TypingIndicator } from "./TypingIndicator";

interface MessageListProps {
  messages: ChatMessage[];
  isLoading: boolean;
  error?: string | null;
  toolComponents?: Record<string, React.ComponentType<ToolCallProps>>;
  showAllToolCalls?: boolean;
  isVisible?: boolean;
  onLinkClick?: (url: string, text: string) => void;
  autoScroll?: boolean;
}

export const MessageList: React.FC<MessageListProps> = ({
  messages,
  isLoading,
  error,
  toolComponents,
  showAllToolCalls = false,
  isVisible,
  onLinkClick,
  autoScroll = true,
}) => {
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const hasStreamingAssistant = messages.some((m) => m.isStreaming);

  const scrollToBottom = (behavior: ScrollBehavior = "smooth") => {
    messagesEndRef.current?.scrollIntoView({ behavior });
  };

  useEffect(() => {
    if (autoScroll) {
      scrollToBottom();
    }
  }, [messages, isLoading, autoScroll]);

  useEffect(() => {
    if (isVisible && autoScroll) {
      scrollToBottom("auto");
    }
  }, [isVisible, autoScroll]);

  if (messages.length === 0 && !isLoading) {
    return (
      <div className={styles.emptyState}>
        <div className={styles.emptyStateContent}>
          <h3 className={styles.emptyStateTitle}>
            <FormattedMessage id="chat.empty.title" />
          </h3>
          <p className={styles.emptyStateText}>
            <FormattedMessage id="chat.empty.subtitle" />
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className={styles.messageList}>
      <div className={styles.messages}>
        {messages.map((message) => (
          <Message
            key={message.id}
            message={message}
            toolComponents={toolComponents}
            showAllToolCalls={showAllToolCalls}
            showThinkingIndicator
            onLinkClick={onLinkClick}
          />
        ))}

        {isLoading && !hasStreamingAssistant && <TypingIndicator />}

        {error && (
          <div className={styles.error}>
            <FormattedMessage id="chat.error" values={{ error }} />
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>
    </div>
  );
};
