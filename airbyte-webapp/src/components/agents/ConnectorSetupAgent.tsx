import React from "react";

import { type OAuthState } from "./hooks/useConnectorSetupAgentState";
import { OAuthProvider } from "./OAuthContext";
import { CheckConfigurationToolRenderer } from "./tools/renderers/CheckConfigurationToolRenderer";
import { GetConnectorDocumentationToolRenderer } from "./tools/renderers/GetConnectorDocumentationToolRenderer";
import { RequestOAuthToolRenderer } from "./tools/renderers/RequestOAuthToolRenderer";
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
  [TOOL_NAMES.REQUEST_OAUTH_AUTHENTICATION]: RequestOAuthToolRenderer,
  [TOOL_NAMES.SAVE_DRAFT_CONFIGURATION]: SaveDraftConfigurationToolRenderer,
  [TOOL_NAMES.SUBMIT_CONFIGURATION]: SubmitConfigurationToolRenderer,
};

export interface ConnectorSetupAgentProps extends ChatInterfaceProps {
  oauthState?: OAuthState;
}

export const ConnectorSetupAgent: React.FC<ConnectorSetupAgentProps> = ({ oauthState, ...props }) => {
  const defaultOAuthState: OAuthState = {
    isOAuthPendingUserAction: false,
    startOAuth: () => {},
    cancelOAuth: () => {},
  };

  return (
    <OAuthProvider value={oauthState ?? defaultOAuthState}>
      <ChatInterface {...props} toolComponents={toolComponents} showAllToolCalls={false} />
    </OAuthProvider>
  );
};
