import React from "react";

import { SimpleMessageToolRenderer } from "./SimpleMessageToolRenderer";
import { type ToolCallProps } from "../../../chat/ToolCallItem";

export const GetConnectorDocumentationToolRenderer: React.FC<ToolCallProps> = (props) => {
  return (
    <SimpleMessageToolRenderer
      {...props}
      inProgressMessage="Retrieving connector documentation..."
      completedMessage="Retrieved connector documentation"
    />
  );
};
