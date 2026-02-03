import React, { useId, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ToolCall, ToolResponse } from "./Message";
import styles from "./ToolCall.module.scss";

export interface ToolCallProps {
  toolCall: ToolCall;
  toolResponse?: ToolResponse;
}

export const ToolCallItem: React.FC<ToolCallProps> = ({ toolCall, toolResponse }) => {
  const intl = useIntl();
  const [isExpanded, setIsExpanded] = useState(false);
  const contentId = useId();

  const formatArgs = (args: string | Record<string, unknown> | null): string => {
    if (args === null) {
      return "";
    }
    if (typeof args === "string") {
      return args;
    }
    return JSON.stringify(args, null, 2);
  };

  return (
    <div className={styles.toolCall}>
      <div className={styles.toolCallHeader}>
        <div className={styles.toolCallTitle}>
          <span className={styles.toolCallIcon}>🔧</span>
          <span className={styles.toolCallName}>
            <FormattedMessage id="chat.toolCall.label" />: {toolCall.tool_name}
          </span>
        </div>
        <button
          type="button"
          aria-label={intl.formatMessage({
            id: isExpanded ? "chat.toolCall.collapse" : "chat.toolCall.expand",
          })}
          aria-expanded={isExpanded}
          aria-controls={contentId}
          className={styles.toggleButton}
          onClick={() => setIsExpanded((prev) => !prev)}
        >
          {isExpanded ? "-" : "+"}
        </button>
      </div>
      {isExpanded && (
        <div id={contentId}>
          <div className={styles.toolCallArgs}>
            <div className={styles.toolCallArgsLabel}>
              <FormattedMessage id="chat.toolCall.args" />:
            </div>
            <pre className={styles.toolCallArgsContent}>{formatArgs(toolCall.args)}</pre>
          </div>
          {toolResponse && (
            <div className={styles.toolResponse}>
              <div className={styles.toolResponseLabel}>
                <FormattedMessage id="chat.toolCall.response" />:
              </div>
              <pre className={styles.toolResponseContent}>
                {typeof toolResponse.response === "string"
                  ? toolResponse.response
                  : JSON.stringify(toolResponse.response, null, 2)}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
};
