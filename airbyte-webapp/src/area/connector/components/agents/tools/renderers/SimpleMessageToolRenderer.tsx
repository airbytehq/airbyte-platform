import React from "react";

import styles from "./SimpleMessageToolRenderer.module.scss";
import { type ToolCallProps } from "../../../chat/ToolCallItem";

interface SimpleMessageToolRendererProps extends ToolCallProps {
  inProgressMessage?: string;
  completedMessage: string;
  isSuccess?: boolean; // Whether the completed state is a success (default: true)
}

export const SimpleMessageToolRenderer: React.FC<SimpleMessageToolRendererProps> = ({
  toolResponse,
  inProgressMessage,
  completedMessage,
  isSuccess = true,
}) => {
  const isInProgress = !toolResponse;

  // If in progress and no inProgressMessage is provided, don't render anything
  if (isInProgress && !inProgressMessage) {
    return null;
  }

  const message = isInProgress ? inProgressMessage : completedMessage;
  // Show different icons for in-progress, success, and failure
  const icon = isInProgress ? "⋯" : isSuccess ? "✓" : "✗";

  return (
    <div className={`${styles.simpleMessageTool} ${!isInProgress && !isSuccess ? styles.failed : ""}`}>
      <span className={styles.icon}>{icon}</span>
      <span className={styles.message}>{message}</span>
    </div>
  );
};
