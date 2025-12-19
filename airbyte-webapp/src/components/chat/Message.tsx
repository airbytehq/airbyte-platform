import React from "react";
import { FormattedMessage } from "react-intl";

import styles from "./Message.module.scss";
import { SafeMarkdown } from "./SafeMarkdown";
import { StreamingIndicator } from "./StreamingIndicator";
import { ToolCallItem, type ToolCallProps } from "./ToolCallItem";

export interface ToolCall {
  tool_name: string;
  args: string | Record<string, unknown> | null;
  call_id: string;
}

export interface ToolResponse {
  tool_name: string;
  response: unknown;
  call_id: string;
}

export const HIDDEN_MESSAGE_PREFIX = "[HIDDEN_INTERNAL_MESSAGE]";

export interface ChatMessage {
  id: string;
  content: string;
  role: "user" | "assistant" | "tool";
  timestamp: Date;
  isStreaming?: boolean;
  toolCall?: ToolCall;
  toolResponse?: ToolResponse;
}

interface MessageProps {
  message: ChatMessage;
  toolComponents?: Record<string, React.ComponentType<ToolCallProps>>;
  showAllToolCalls?: boolean;
  showStreamingIndicator?: boolean;
}

export const Message: React.FC<MessageProps> = ({
  message,
  toolComponents,
  showAllToolCalls = false,
  showStreamingIndicator = false,
}) => {
  const { content, role, isStreaming, toolCall, toolResponse } = message;

  // Hide messages with the hidden prefix
  if (content.startsWith(HIDDEN_MESSAGE_PREFIX)) {
    return null;
  }

  // current tool‐message branch: ensure we don't fall back to a missing i18n key
  if (role === "tool") {
    if (!toolCall) {
      return null; // or render an error placeholder
    }

    // Check if there's a custom renderer for this tool
    const CustomRenderer = toolComponents?.[toolCall.tool_name];

    // If there's no custom renderer and showAllToolCalls is false, don't render anything
    if (!CustomRenderer && !showAllToolCalls) {
      return null;
    }

    return (
      <div className={`${styles.message} ${styles["message--tool"]}`}>
        <div className={styles.messageContent}>
          {CustomRenderer ? (
            <CustomRenderer toolCall={toolCall} toolResponse={toolResponse} />
          ) : (
            <ToolCallItem toolCall={toolCall} toolResponse={toolResponse} />
          )}
        </div>
      </div>
    );
  }

  if (content === "") {
    return null;
  }

  return (
    <div className={`${styles.message} ${styles[`message--${role}`]}`}>
      <div className={styles.messageContent}>
        <div className={styles.messageHeader}>
          <span className={styles.messageRole}>
            <FormattedMessage id={`chat.message.${role}`} />
          </span>
        </div>
        <div className={styles.messageText}>
          {isStreaming && !content && showStreamingIndicator ? (
            <StreamingIndicator />
          ) : (
            <>
              <SafeMarkdown content={content} />
              {isStreaming && <span className={styles.cursor} />}
            </>
          )}
        </div>
      </div>
    </div>
  );
};
