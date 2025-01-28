import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Separator } from "components/ui/Separator";

import { useCurrentWorkspace } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";
import { DeleteWorkspace } from "pages/SettingsPage/components/DeleteWorkspace";

import { UpdateWorkspaceSettingsForm } from "./components/UpdateWorkspaceSettingsForm";

export const WorkspaceSettingsView: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE);

  const { workspaceId } = useCurrentWorkspace();
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });
  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.workspace.general.title" />
      </Heading>
      <UpdateWorkspaceSettingsForm />
      {canDeleteWorkspace && (
        <>
          <Separator />
          <FlexContainer direction="column">
            <Heading as="h2" size="sm">
              <FormattedMessage id="settings.general.danger" />
            </Heading>
            <FlexContainer>
              <DeleteWorkspace />
            </FlexContainer>
          </FlexContainer>
        </>
      )}
    </FlexContainer>
  );
};
