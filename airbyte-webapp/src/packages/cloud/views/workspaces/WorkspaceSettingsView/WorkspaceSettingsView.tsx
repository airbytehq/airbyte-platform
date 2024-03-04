import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useCurrentWorkspace } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import WorkspaceAccessManagementSection from "pages/SettingsPage/pages/AccessManagementPage/WorkspaceAccessManagementSection";

import { DeleteCloudWorkspace, UpdateCloudWorkspaceName } from "./components";

export const WorkspaceSettingsView: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE);

  const { workspaceId } = useCurrentWorkspace();
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);
  return (
    <FlexContainer direction="column" gap="xl">
      <Box>
        <Heading as="h2" size="md">
          <FormattedMessage id="settings.members" />
        </Heading>
      </Box>
      <Card>
        <UpdateCloudWorkspaceName />
      </Card>
      {isAccessManagementEnabled && (
        <Card>
          <FlexContainer direction="column" gap="xl">
            <WorkspaceAccessManagementSection />
          </FlexContainer>
        </Card>
      )}
      {canDeleteWorkspace && (
        <Card>
          <FlexContainer direction="column">
            <Heading as="h3" size="sm">
              <FormattedMessage id="settings.general.danger" />
            </Heading>
            <FlexContainer>
              <DeleteCloudWorkspace />
            </FlexContainer>
          </FlexContainer>
        </Card>
      )}
    </FlexContainer>
  );
};
