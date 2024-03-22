import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";

import styles from "./InviteUsersHint.module.scss";
import { InviteUsersModal } from "../InviteUsersModal";

export interface InviteUsersHintProps {
  connectorType: "source" | "destination";
}

export const InviteUsersHint: React.FC<InviteUsersHintProps> = ({ connectorType }) => {
  const { formatMessage } = useIntl();
  const inviteUsersHintVisible = useFeature(FeatureItem.ShowInviteUsersHint);
  const { workspaceId } = useCurrentWorkspace();
  const canInviteUsers = useIntent("UpdateWorkspacePermissions", { workspaceId });
  const { openModal } = useModalService();

  if (!inviteUsersHintVisible || !canInviteUsers) {
    return null;
  }

  const onOpenInviteUsersModal = () =>
    openModal<void>({
      title: formatMessage({ id: "modals.addUser.title" }),
      content: ({ onComplete, onCancel }) => (
        <InviteUsersModal invitedFrom={connectorType} onSubmit={onComplete} onCancel={onCancel} />
      ),
      size: "md",
    });

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
      <Button variant="secondary" data-testid="inviteUsersHint-cta" onClick={onOpenInviteUsersModal}>
        <FormattedMessage id="inviteUsersHint.cta" />
      </Button>
    </FlexContainer>
  );
};
