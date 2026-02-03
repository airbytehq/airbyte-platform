import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Separator } from "components/ui/Separator";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { DeleteWorkspace } from "pages/SettingsPage/components/DeleteWorkspace";
import { TagsTable } from "pages/SettingsPage/Workspace/components/TagsTable";

import { UpdateWorkspaceSettingsForm } from "./components/UpdateWorkspaceSettingsForm";

export const WorkspaceSettingsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE);
  const canDeleteWorkspace = useGeneratedIntent(Intent.DeleteWorkspace);

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer alignItems="center" wrap="wrap">
        <FlexItem grow>
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.workspace.general.title" />
          </Heading>
        </FlexItem>
        <CopyButton
          content={workspaceId}
          variant="clear"
          iconPosition="right"
          title={formatMessage({ id: "settings.workspaceId.copy" })}
        >
          <FormattedMessage id="settings.workspaceId" values={{ id: workspaceId }} />
        </CopyButton>
      </FlexContainer>
      <UpdateWorkspaceSettingsForm />
      <Separator />
      <TagsTable />
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
