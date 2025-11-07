import React, { useState, useRef, useEffect } from "react";
import { useIntl } from "react-intl";
import { useToggle } from "react-use";

import { Message } from "components/ui/Message";

import styles from "./ChatInput.module.scss";
import { Icon } from "../ui/Icon";

interface ChatInputProps {
  onSendMessage: (message: string) => void;
  onStop?: () => void;
  isStreaming?: boolean;
  disabled?: boolean;
  isSecretMode?: boolean;
  secretFieldPath?: string[];
  secretFieldName?: string;
  isMultiline?: boolean;
}

export const ChatInput: React.FC<ChatInputProps> = ({
  onSendMessage,
  onStop,
  isStreaming = false,
  disabled = false,
  isSecretMode = false,
  secretFieldPath = [],
  secretFieldName,
  isMultiline = false,
}) => {
  const [message, setMessage] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const { formatMessage } = useIntl();
  const [isSecretVisible, toggleSecretVisibility] = useToggle(false);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (message.trim() && !disabled) {
      onSendMessage(message.trim());
      setMessage("");
      resizeTextarea();
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  const resizeTextarea = () => {
    const textarea = textareaRef.current;
    if (textarea) {
      textarea.style.height = "auto";
      textarea.style.height = `${Math.min(textarea.scrollHeight, 120)}px`;
    }
  };

  useEffect(() => {
    resizeTextarea();
  }, [message]);

  const placeholder = isSecretMode
    ? formatMessage(
        {
          id: "chat.input.secretPlaceholder",
          defaultMessage: "Enter value for {fieldName} (sensitive - not sent to AI)",
        },
        { fieldName: secretFieldName || secretFieldPath.join(".") }
      )
    : formatMessage({ id: "chat.input.placeholder" });

  return (
    <div className={styles.chatInput}>
      <form className={styles.form} onSubmit={handleSubmit}>
        {isSecretMode && (
          <Message
            className={styles.secretModeMessage}
            text={formatMessage({
              id: "chat.input.secretMode",
              defaultMessage:
                "Secure input: Any credentials you enter will be stored securely, and not sent to the LLM.",
            })}
          />
        )}
        <div className={styles.inputContainer}>
          {isSecretMode && isMultiline && !isSecretVisible ? (
            <button
              type="button"
              className={styles.secretToggleButton}
              onClick={toggleSecretVisibility}
              disabled={disabled}
            >
              <Icon type="eye" size="sm" />
              <span className={styles.secretToggleText}>
                {formatMessage({
                  id: "chat.input.secretToggle",
                  defaultMessage: "Click to enter sensitive value",
                })}
              </span>
            </button>
          ) : isSecretMode && !isMultiline ? (
            <input
              type={isSecretVisible ? "text" : "password"}
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={placeholder}
              disabled={disabled}
              className={styles.textarea}
              autoComplete="off"
              data-1p-ignore
              data-lpignore="true"
              data-bwignore="true"
              data-form-type="other"
              name="secret-input-field"
            />
          ) : (
            <textarea
              ref={textareaRef}
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={placeholder}
              disabled={disabled}
              className={styles.textarea}
              rows={1}
              autoComplete={isSecretMode ? "off" : undefined}
              data-1p-ignore={isSecretMode ? true : undefined}
              data-lpignore={isSecretMode ? "true" : undefined}
              data-bwignore={isSecretMode ? "true" : undefined}
              data-form-type={isSecretMode ? "other" : undefined}
              name={isSecretMode ? "secret-input-field" : undefined}
            />
          )}
          {isSecretMode && !isMultiline && (
            <button
              type="button"
              className={styles.secretHideButton}
              onClick={toggleSecretVisibility}
              disabled={disabled}
            >
              <Icon type={isSecretVisible ? "eyeSlash" : "eye"} size="sm" />
            </button>
          )}
          {isSecretMode && isSecretVisible && isMultiline && (
            <button
              type="button"
              className={styles.secretHideButton}
              onClick={toggleSecretVisibility}
              disabled={disabled}
            >
              <Icon type="eyeSlash" size="sm" />
            </button>
          )}
          {isStreaming ? (
            <button
              type="button"
              onClick={onStop}
              className={styles.stopButton}
              aria-label={formatMessage({ id: "chat.input.stop" })}
            >
              <Icon type="stopFilled" size="sm" />
            </button>
          ) : (
            <button
              type="submit"
              disabled={!message.trim() || disabled}
              className={styles.sendButton}
              aria-label={formatMessage({ id: "chat.input.send" })}
            >
              <Icon type="chevronRight" size="sm" />
            </button>
          )}
        </div>
      </form>
    </div>
  );
};
