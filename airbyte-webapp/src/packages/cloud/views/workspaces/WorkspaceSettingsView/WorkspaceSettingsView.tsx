import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Separator } from "components/ui/Separator";

import { useCurrentWorkspace } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import {
  DeleteCloudWorkspace,
  UpdateCloudWorkspaceName,
} from "pages/SettingsPage/pages/AccessManagementPage/components";
import WorkspaceAccessManagementSection from "pages/SettingsPage/pages/AccessManagementPage/WorkspaceAccessManagementSection";

export const WorkspaceSettingsView: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE);

  const { workspaceId } = useCurrentWorkspace();
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);
  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.workspace.general.title" />
      </Heading>
      <UpdateCloudWorkspaceName />

      {isAccessManagementEnabled && (
        <>
          <Separator />
          <FlexContainer direction="column" gap="xl">
            <WorkspaceAccessManagementSection />
          </FlexContainer>
        </>
      )}
      {canDeleteWorkspace && (
        <>
          <Separator />
          <FlexContainer direction="column">
            <Heading as="h2" size="sm">
              <FormattedMessage id="settings.general.danger" />
            </Heading>
            <FlexContainer>
              <DeleteCloudWorkspace />
            </FlexContainer>
          </FlexContainer>
        </>
      )}
    </FlexContainer>
  );
};
