import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

import { type OAuthState } from "./hooks/useConnectorSetupAgentState";
import { OAuthProvider } from "./OAuthContext";
import { CheckConfigurationToolRenderer } from "./tools/renderers/CheckConfigurationToolRenderer";
import { GetConnectorDocumentationToolRenderer } from "./tools/renderers/GetConnectorDocumentationToolRenderer";
import { RequestOAuthToolRenderer } from "./tools/renderers/RequestOAuthToolRenderer";
import { RequestSecretInputToolRenderer } from "./tools/renderers/RequestSecretInputToolRenderer";
import { SaveDraftConfigurationToolRenderer } from "./tools/renderers/SaveDraftConfigurationToolRenderer";
import { SubmitConfigurationToolRenderer } from "./tools/renderers/SubmitConfigurationToolRenderer";
import { TOOL_NAMES } from "./tools/toolNames";
import { ChatInterfaceBody, ChatInterfaceContainer, ChatInterfaceHeader } from "../chat";
import { ChatInput } from "../chat/ChatInput";
import { type ChatMessage } from "../chat/Message";
import { MessageList } from "../chat/MessageList";
import { type ToolCallProps } from "../chat/ToolCallItem";

const toolComponents: Record<string, React.ComponentType<ToolCallProps>> = {
  [TOOL_NAMES.CHECK_CONFIGURATION]: CheckConfigurationToolRenderer,
  [TOOL_NAMES.GET_CONNECTOR_DOCUMENTATION]: GetConnectorDocumentationToolRenderer,
  [TOOL_NAMES.REQUEST_SECRET_INPUT]: RequestSecretInputToolRenderer,
  [TOOL_NAMES.REQUEST_OAUTH_AUTHENTICATION]: RequestOAuthToolRenderer,
  [TOOL_NAMES.SAVE_DRAFT_CONFIGURATION]: SaveDraftConfigurationToolRenderer,
  [TOOL_NAMES.SUBMIT_CONFIGURATION]: SubmitConfigurationToolRenderer,
};

export interface ConnectorSetupAgentProps {
  messages: ChatMessage[];
  isLoading: boolean;
  error: string | null;
  isStreaming: boolean;
  onSendMessage: (message: string) => void;
  onStop: () => void;
  isSecretMode?: boolean;
  secretFieldPath?: string[];
  secretFieldName?: string;
  isMultiline?: boolean;
  isVisible?: boolean;
  onDismissSecret?: () => void;
  oauthState?: OAuthState;
}

export const ConnectorSetupAgent: React.FC<ConnectorSetupAgentProps> = ({
  messages,
  isLoading,
  error,
  isStreaming,
  onSendMessage,
  onStop,
  isSecretMode = false,
  secretFieldPath = [],
  secretFieldName,
  isMultiline = false,
  isVisible = true,
  onDismissSecret,
  oauthState,
}) => {
  const defaultOAuthState: OAuthState = {
    isOAuthPendingUserAction: false,
    startOAuth: () => {},
    cancelOAuth: () => {},
  };

  return (
    <OAuthProvider value={oauthState ?? defaultOAuthState}>
      <ChatInterfaceContainer>
        <ChatInterfaceHeader>
          <FlexContainer direction="row" alignItems="center" gap="sm">
            <Icon type="aiStars" color="magic" size="md" />
            <Heading as="h3" size="sm">
              <FormattedMessage id="connectorSetup.agent.title" />
            </Heading>
            <Badge variant="blue">
              <FormattedMessage id="ui.badge.beta" />
            </Badge>
          </FlexContainer>
          <Box mt="sm">
            <Text size="sm" color="grey">
              <FormattedMessage
                id="connectorSetup.agent.description"
                values={{
                  privacy: (privacy: React.ReactNode) => (
                    <ExternalLink href={links.privacyLink} variant="primary">
                      {privacy}
                    </ExternalLink>
                  ),
                }}
              />
            </Text>
          </Box>
        </ChatInterfaceHeader>
        <ChatInterfaceBody>
          <MessageList
            messages={messages}
            isLoading={isLoading}
            error={error}
            toolComponents={toolComponents}
            showAllToolCalls={false}
            isVisible={isVisible}
          />
          <ChatInput
            onSendMessage={onSendMessage}
            onStop={onStop}
            isStreaming={isStreaming}
            disabled={isSecretMode ? false : isLoading}
            isSecretMode={isSecretMode}
            secretFieldPath={secretFieldPath}
            secretFieldName={secretFieldName}
            isMultiline={isMultiline}
            isVisible={isVisible}
            onDismissSecret={onDismissSecret}
          />
        </ChatInterfaceBody>
      </ChatInterfaceContainer>
    </OAuthProvider>
  );
};
