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
}

export const MessageList: React.FC<MessageListProps> = ({
  messages,
  isLoading,
  error,
  toolComponents,
  showAllToolCalls = false,
  isVisible,
}) => {
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const hasStreamingAssistant = messages.some((m) => m.isStreaming);

  const scrollToBottom = (behavior: ScrollBehavior = "smooth") => {
    messagesEndRef.current?.scrollIntoView({ behavior });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isLoading]);

  useEffect(() => {
    if (isVisible) {
      scrollToBottom("auto");
    }
  }, [isVisible]);

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
