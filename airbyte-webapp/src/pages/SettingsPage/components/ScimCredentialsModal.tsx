import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { ScimIdpProvider } from "core/api/types/AirbyteClient";

import styles from "./ScimCredentialsModal.module.scss";

export interface ScimCredentialsModalProps {
  scimBaseUrl: string;
  token: string;
  idpProvider: ScimIdpProvider;
  onComplete: () => void;
}

// The token is shown exactly once, so it's displayed middle-truncated for readability while
// `CopyButton` still copies the full, untruncated value.
const TOKEN_VISIBLE_HEAD_LENGTH = 19;
const TOKEN_VISIBLE_TAIL_LENGTH = 4;

const truncateMiddle = (value: string, headLength: number, tailLength: number): string =>
  value.length <= headLength + tailLength + 1 ? value : `${value.slice(0, headLength)}…${value.slice(-tailLength)}`;

/**
 * One-time SCIM credential display. Rendered as an `openModal` `content` component (see
 * `ScimSettingsCard`) with `preventCancel: true` - there is no X/ESC/backdrop
 * exit, only the "Copied and done" footer button, which stays disabled until the bearer token
 * specifically has been copied. The base URL is recoverable later; the token is not, so copying
 * it alone doesn't unlock the exit.
 */
export const ScimCredentialsModal: React.FC<ScimCredentialsModalProps> = ({
  scimBaseUrl,
  token,
  idpProvider,
  onComplete,
}) => {
  const { formatMessage } = useIntl();
  const [hasCopiedToken, setHasCopiedToken] = useState(false);
  // If the clipboard is blocked (browser permissions, policy), the copy confirmation can never
  // arrive and the modal has no other exit - reveal the full token for manual copying and unlock
  // the exit instead.
  const [tokenCopyFailed, setTokenCopyFailed] = useState(false);

  return (
    <>
      <ModalBody className={styles.body}>
        <FlexContainer direction="column" gap="lg">
          <Text color="grey" className={styles.description}>
            <FormattedMessage id="settings.organizationSettings.scim.enable.modal.description" />
          </Text>

          <FlexContainer direction="column" gap="sm" data-testid="scim-base-url-field">
            <Text size="sm" bold>
              <FormattedMessage id="settings.organizationSettings.scim.enable.modal.scimBaseUrl" />
            </Text>
            <FlexContainer className={styles.fieldBox} justifyContent="space-between" alignItems="center" gap="sm">
              <Text className={styles.fieldValue} title={scimBaseUrl}>
                {scimBaseUrl}
              </Text>
              <CopyButton
                content={scimBaseUrl}
                variant="secondary"
                title={formatMessage({ id: "settings.organizationSettings.scim.enable.modal.copyScimBaseUrl" })}
              />
            </FlexContainer>
          </FlexContainer>

          <FlexContainer direction="column" gap="sm" data-testid="bearer-token-field">
            <Text size="sm" bold>
              <FormattedMessage id="settings.organizationSettings.scim.enable.modal.bearerToken" />
            </Text>
            <FlexContainer className={styles.fieldBox} justifyContent="space-between" alignItems="center" gap="sm">
              {/* No `title` here, unlike the base URL above: it would put the untruncated token in the
                  DOM regardless of the visual truncation, where session-replay tooling can capture it. */}
              <Text className={styles.fieldValue} data-testid="bearer-token-value">
                {tokenCopyFailed ? token : truncateMiddle(token, TOKEN_VISIBLE_HEAD_LENGTH, TOKEN_VISIBLE_TAIL_LENGTH)}
              </Text>
              <CopyButton
                content={token}
                variant="secondary"
                title={formatMessage({ id: "settings.organizationSettings.scim.enable.modal.copyBearerToken" })}
                onCopy={() => setHasCopiedToken(true)}
                onCopyError={() => setTokenCopyFailed(true)}
              />
            </FlexContainer>
            {tokenCopyFailed && (
              <Message
                type="warning"
                text={formatMessage({ id: "settings.organizationSettings.scim.enable.modal.copyError" })}
              />
            )}
          </FlexContainer>

          {idpProvider === ScimIdpProvider.okta && (
            <Message
              type="info"
              text={formatMessage({ id: "settings.organizationSettings.scim.enable.modal.oktaNote" })}
            />
          )}
        </FlexContainer>
      </ModalBody>
      <ModalFooter>
        <FlexContainer justifyContent="flex-end">
          <Button type="button" disabled={!hasCopiedToken && !tokenCopyFailed} onClick={onComplete}>
            <FormattedMessage id="settings.organizationSettings.scim.enable.modal.cta" />
          </Button>
        </FlexContainer>
      </ModalFooter>
    </>
  );
};
