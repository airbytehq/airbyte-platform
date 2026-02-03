import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import styles from "./CheckConfigurationToolRenderer.module.scss";
import { SimpleMessageToolRenderer } from "./SimpleMessageToolRenderer";
import { type ToolCallProps } from "../../../chat/ToolCallItem";
import { parseToolArgs } from "../utils/parseToolArgs";

interface CheckConfigurationResponse {
  success: boolean;
  message: string;
  status?: string;
  error?: string;
}

export const CheckConfigurationToolRenderer: React.FC<ToolCallProps> = ({ toolCall, toolResponse }) => {
  const [isExpanded, setIsExpanded] = useState(false);

  // Parse the response (reuse parseToolArgs since it handles string/object parsing)
  const parsedResponse = toolResponse?.response
    ? parseToolArgs<CheckConfigurationResponse>(toolResponse.response as string | Record<string, unknown> | null)
    : null;

  // If still pending (no response), show simple loading message
  if (!toolResponse) {
    return (
      <SimpleMessageToolRenderer
        toolCall={toolCall}
        toolResponse={toolResponse}
        inProgressMessage="Checking that your configuration is valid..."
        completedMessage=""
      />
    );
  }

  return (
    <div className={styles.checkConfigurationTool}>
      <div className={styles.header}>
        <div className={styles.titleSection}>
          <span className={styles.icon}>
            {parsedResponse?.success ? "✅" : parsedResponse?.success === false ? "❌" : "🔄"}
          </span>
          <div className={styles.titleContent}>
            <div className={styles.toolName}>Configuration Check</div>
            {parsedResponse && (
              <div className={parsedResponse.success ? styles.statusSuccess : styles.statusError}>
                {parsedResponse.message}
              </div>
            )}
          </div>
        </div>
        <button
          type="button"
          className={styles.toggleButton}
          onClick={() => setIsExpanded((prev) => !prev)}
          aria-expanded={isExpanded}
        >
          {isExpanded ? "−" : "+"}
        </button>
      </div>

      {isExpanded && (
        <div className={styles.expandedContent}>
          {parsedResponse && (
            <div className={styles.section}>
              <div className={styles.sectionTitle}>
                <FormattedMessage id="chat.toolCall.checkConfig.testResult" />
              </div>
              <div className={styles.sectionContent}>
                <div className={styles.resultStatus}>
                  Status:{" "}
                  <span className={parsedResponse.success ? styles.successText : styles.errorText}>
                    {parsedResponse.success ? "Passed" : "Failed"}
                  </span>
                </div>
                {parsedResponse.status && (
                  <div className={styles.resultDetail}>Connection Status: {parsedResponse.status}</div>
                )}
                {parsedResponse.error && (
                  <div className={styles.errorDetail}>
                    <strong>Error:</strong>
                    <pre className={styles.errorBlock}>{parsedResponse.error}</pre>
                  </div>
                )}
              </div>
            </div>
          )}

          {!parsedResponse && toolResponse && (
            <div className={styles.section}>
              <div className={styles.sectionTitle}>Raw Response</div>
              <div className={styles.sectionContent}>
                <pre className={styles.codeBlock}>
                  {typeof toolResponse.response === "string"
                    ? toolResponse.response
                    : JSON.stringify(toolResponse.response, null, 2)}
                </pre>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};
