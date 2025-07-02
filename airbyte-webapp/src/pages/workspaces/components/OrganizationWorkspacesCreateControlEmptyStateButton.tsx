import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";

import { useListDataplaneGroups } from "core/api";
import { useModalService } from "hooks/services/Modal";

import { CreateWorkspaceModal } from "./OrganizationWorkspacesCreateControl";
import { useOrganizationsToCreateWorkspaces } from "./useOrganizationsToCreateWorkspaces";

const OrganizationWorkspacesCreateControlEmptyStateButton: React.FC = () => {
  const { organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();
  const dataplaneGroups = useListDataplaneGroups();
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();

  if (organizationsToCreateIn.length === 0) {
    return null;
  }

  const handleButtonClick = () => {
    openModal({
      title: formatMessage({ id: "workspaces.create.title" }),
      content: ({ onCancel }) => <CreateWorkspaceModal dataplaneGroups={dataplaneGroups} onCancel={onCancel} />,
    });
  };

  return (
    <Button onClick={handleButtonClick} variant="secondary" data-testid="workspaces.createANewWorkspace" size="sm">
      <FormattedMessage id="workspaces.createANewWorkspace" />
    </Button>
  );
};

export default OrganizationWorkspacesCreateControlEmptyStateButton;
