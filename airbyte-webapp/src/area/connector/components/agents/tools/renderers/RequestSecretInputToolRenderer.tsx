import React from "react";

import { SimpleMessageToolRenderer } from "./SimpleMessageToolRenderer";
import { type ToolCallProps } from "../../../chat/ToolCallItem";
import { parseToolArgs } from "../utils/parseToolArgs";

export const RequestSecretInputToolRenderer: React.FC<ToolCallProps> = (props) => {
  const parsedArgs = parseToolArgs<{ field_name?: string; field_path?: string[] }>(props.toolCall?.args || null);

  // Extract field_name, falling back to field_path joined with dots
  const fieldName = parsedArgs?.field_name || parsedArgs?.field_path?.join(".") || "the field";

  return (
    <SimpleMessageToolRenderer
      {...props}
      inProgressMessage={`Waiting for you to provide a value for ${fieldName}...`}
      completedMessage={`Set secret value for ${fieldName}`}
    />
  );
};
