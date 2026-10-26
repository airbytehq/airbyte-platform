import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./InviteUsersButton.module.scss";
import { useInviteUsers } from "../useInviteUsers";

export const InviteUsersButton: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const { formatMessage } = useIntl();
  const { showInviteUsers, openInviteUsersModal } = useInviteUsers();

  if (!showInviteUsers) {
    return null;
  }

  return (
    <Tooltip
      control={
        <Button
          className={styles.button}
          variant="secondary"
          icon="user"
          iconSize="sm"
          aria-label={formatMessage({ id: "inviteUsersHint.cta" })}
          onClick={openInviteUsersModal}
          disabled={disabled}
          type="button"
        />
      }
      placement="bottom"
      cursor={disabled ? "not-allowed" : undefined}
    >
      <FormattedMessage id="inviteUsersHint.cta" />
    </Tooltip>
  );
};
