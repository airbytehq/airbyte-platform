import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import styles from "./RequestOAuthToolRenderer.module.scss";
import { SimpleMessageToolRenderer } from "./SimpleMessageToolRenderer";
import { type ToolCallProps } from "../../../chat/ToolCallItem";
import { useOAuthContext } from "../../OAuthContext";
import { type OAuthToolResponse } from "../hooks/useRequestOAuthTool";
import { parseToolArgs } from "../utils/parseToolArgs";

export const RequestOAuthToolRenderer: React.FC<ToolCallProps> = (props) => {
  const { formatMessage } = useIntl();
  const oauthContext = useOAuthContext();
  const parsedArgs = parseToolArgs<{ provider_name?: string }>(props.toolCall?.args || null);

  // Extract provider_name for display
  const providerName = parsedArgs?.provider_name || "the OAuth provider";

  // If we have a response, show the result
  if (props.toolResponse?.response) {
    // Parse the response to determine success or failure
    let isSuccess = true;
    try {
      // Response can be a string (JSON) or already parsed object
      const rawResponse = props.toolResponse.response;
      const response: OAuthToolResponse =
        typeof rawResponse === "string" ? JSON.parse(rawResponse) : (rawResponse as OAuthToolResponse);
      isSuccess = response.success === true;
    } catch (_e) {
      // If we can't parse, assume success (backward compatibility)
      isSuccess = true;
    }

    const completedMessage = isSuccess
      ? formatMessage({ id: "connectorSetup.oauth.success" }, { providerName })
      : formatMessage({ id: "connectorSetup.oauth.notCompleted" }, { providerName });

    return (
      <SimpleMessageToolRenderer
        {...props}
        inProgressMessage=""
        completedMessage={completedMessage}
        isSuccess={isSuccess}
      />
    );
  }

  // If OAuth is pending user action, show the button
  if (oauthContext?.isOAuthPendingUserAction) {
    return (
      <div className={styles.oauthPrompt}>
        <FlexContainer direction="column" gap="sm">
          <span className={styles.promptText}>
            <FormattedMessage id="connectorSetup.oauth.clickToAuthenticate" values={{ providerName }} />
          </span>
          <FlexContainer gap="sm">
            <Button size="xs" onClick={oauthContext.startOAuth}>
              <FormattedMessage id="connectorSetup.oauth.authenticateButton" values={{ providerName }} />
            </Button>
            <Button size="xs" variant="secondary" onClick={oauthContext.cancelOAuth}>
              <FormattedMessage id="connectorSetup.oauth.cancelButton" />
            </Button>
          </FlexContainer>
        </FlexContainer>
      </div>
    );
  }

  // Default: show waiting message (OAuth flow has been started) with cancel option
  return (
    <div className={styles.oauthPrompt}>
      <FlexContainer direction="column" gap="sm">
        <span className={styles.promptText}>
          <FormattedMessage id="connectorSetup.oauth.waitingForAuthentication" values={{ providerName }} />
        </span>
        <FlexContainer gap="sm" alignItems="center">
          <Button size="xs" variant="secondary" onClick={oauthContext?.cancelOAuth}>
            <FormattedMessage id="connectorSetup.oauth.cancelButton" />
          </Button>
          <span className={styles.hintText}>
            <FormattedMessage id="connectorSetup.oauth.cancelHint" />
          </span>
        </FlexContainer>
      </FlexContainer>
    </div>
  );
};
