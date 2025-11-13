import React from "react";

import { CheckConfigurationToolRenderer } from "./tools/renderers/CheckConfigurationToolRenderer";
import { GetConnectorDocumentationToolRenderer } from "./tools/renderers/GetConnectorDocumentationToolRenderer";
import { RequestSecretInputToolRenderer } from "./tools/renderers/RequestSecretInputToolRenderer";
import { SaveDraftConfigurationToolRenderer } from "./tools/renderers/SaveDraftConfigurationToolRenderer";
import { SubmitConfigurationToolRenderer } from "./tools/renderers/SubmitConfigurationToolRenderer";
import { TOOL_NAMES } from "./tools/toolNames";
import { ChatInterface, type ChatInterfaceProps } from "../chat/ChatInterface";
import { type ToolCallProps } from "../chat/ToolCallItem";

const toolComponents: Record<string, React.ComponentType<ToolCallProps>> = {
  [TOOL_NAMES.CHECK_CONFIGURATION]: CheckConfigurationToolRenderer,
  [TOOL_NAMES.GET_CONNECTOR_DOCUMENTATION]: GetConnectorDocumentationToolRenderer,
  [TOOL_NAMES.REQUEST_SECRET_INPUT]: RequestSecretInputToolRenderer,
  [TOOL_NAMES.SAVE_DRAFT_CONFIGURATION]: SaveDraftConfigurationToolRenderer,
  [TOOL_NAMES.SUBMIT_CONFIGURATION]: SubmitConfigurationToolRenderer,
};

export type ConnectorSetupAgentProps = ChatInterfaceProps;

export const ConnectorSetupAgent: React.FC<ConnectorSetupAgentProps> = (props) => {
  return <ChatInterface {...props} toolComponents={toolComponents} showAllToolCalls={false} />;
};
