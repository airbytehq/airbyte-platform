import React, { useState, useRef, useEffect } from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";

import styles from "./ChatTextInput.module.scss";

interface ChatTextInputProps {
  onSendMessage: (message: string) => void;
  onStop?: () => void;
  isStreaming?: boolean;
  disabled?: boolean;
  placeholder?: string;
}

export const ChatTextInput: React.FC<ChatTextInputProps> = ({
  onSendMessage,
  onStop,
  isStreaming = false,
  disabled = false,
  placeholder,
}) => {
  const [message, setMessage] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const { formatMessage } = useIntl();

  const handleSubmit = () => {
    if (message.trim() && !disabled) {
      onSendMessage(message.trim());
      setMessage("");
      resizeTextarea();
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
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

  // Auto-focus textarea when streaming stops
  useEffect(() => {
    if (!isStreaming) {
      textareaRef.current?.focus();
    }
  }, [isStreaming]);

  return (
    <div className={styles.chatTextInput}>
      <div className={styles.inputContainer}>
        <textarea
          ref={textareaRef}
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={
            placeholder ||
            formatMessage({
              id: isStreaming ? "chat.input.placeholderStreaming" : "chat.input.placeholder",
            })
          }
          disabled={disabled}
          className={styles.textarea}
          rows={1}
          name="chat-text-input"
          data-testid="chat-text-input"
        />
        {isStreaming ? (
          <Button
            type="button"
            onClick={onStop}
            className={styles.button}
            aria-label={formatMessage({ id: "chat.input.stop" })}
            icon="stopFilled"
            variant="secondary"
          />
        ) : (
          <Button
            type="button"
            onClick={handleSubmit}
            disabled={!message.trim() || disabled}
            className={styles.button}
            aria-label={formatMessage({ id: "chat.input.send" })}
            icon="cursor"
            iconSize="sm"
            variant={!message.trim() || disabled ? "clear" : "primary"}
          />
        )}
      </div>
      <div className={styles.helperText}>{formatMessage({ id: "chat.input.helper" })}</div>
    </div>
  );
};
