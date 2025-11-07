import React from "react";

import { SimpleMessageToolRenderer } from "./SimpleMessageToolRenderer";
import { type ToolCallProps } from "../../../chat/ToolCallItem";

export const SubmitConfigurationToolRenderer: React.FC<ToolCallProps> = (props) => {
  return (
    <SimpleMessageToolRenderer
      {...props}
      inProgressMessage="Saving your connector..."
      completedMessage="Connector saved"
    />
  );
};
