import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./InviteUsersHint.module.scss";
import { useInviteUsers } from "../useInviteUsers";

export interface InviteUsersHintProps {
  connectorType: "source" | "destination";
}

export const InviteUsersHint: React.FC<InviteUsersHintProps> = ({ connectorType }) => {
  const { formatMessage } = useIntl();
  const { showInviteUsers, openInviteUsersModal } = useInviteUsers();

  if (!showInviteUsers) {
    return null;
  }

  return (
    <FlexContainer alignItems="center" justifyContent="center" className={styles.container}>
      <Text size="sm" data-testid="inviteUsersHint">
        <FormattedMessage
          id="inviteUsersHint.message"
          values={{
            connector: formatMessage({ id: `connector.${connectorType}` }).toLowerCase(),
          }}
        />
      </Text>
      <Button variant="secondary" data-testid="inviteUsersHint-cta" onClick={openInviteUsersModal}>
        <FormattedMessage id="inviteUsersHint.cta" />
      </Button>
    </FlexContainer>
  );
};
