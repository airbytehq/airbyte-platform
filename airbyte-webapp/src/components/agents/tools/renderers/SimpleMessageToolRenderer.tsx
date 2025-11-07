import React from "react";

import styles from "./SimpleMessageToolRenderer.module.scss";
import { type ToolCallProps } from "../../../chat/ToolCallItem";

interface SimpleMessageToolRendererProps extends ToolCallProps {
  inProgressMessage?: string;
  completedMessage: string;
}

export const SimpleMessageToolRenderer: React.FC<SimpleMessageToolRendererProps> = ({
  toolResponse,
  inProgressMessage,
  completedMessage,
}) => {
  const isInProgress = !toolResponse;

  // If in progress and no inProgressMessage is provided, don't render anything
  if (isInProgress && !inProgressMessage) {
    return null;
  }

  const message = isInProgress ? inProgressMessage : completedMessage;
  const icon = isInProgress ? "⋯" : "✓";

  return (
    <div className={styles.simpleMessageTool}>
      <span className={styles.icon}>{icon}</span>
      <span className={styles.message}>{message}</span>
    </div>
  );
};
