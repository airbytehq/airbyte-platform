import React from "react";

import { SimpleMessageToolRenderer } from "./SimpleMessageToolRenderer";
import { type ToolCallProps } from "../../../chat/ToolCallItem";

export const SaveDraftConfigurationToolRenderer: React.FC<ToolCallProps> = (props) => {
  return <SimpleMessageToolRenderer {...props} completedMessage="Draft configuration updated" />;
};
