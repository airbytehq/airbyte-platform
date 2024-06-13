import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { UpdateWorkspaceNameForm } from "area/workspace/components/UpdateWorkspaceNameForm";
import { useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";

import { DeleteWorkspace } from "./components/DeleteWorkspace";
import WorkspaceAccessManagementSection from "./pages/AccessManagementPage/WorkspaceAccessManagementSection";

export const GeneralWorkspaceSettingsPage = () => {
  const { workspaceId } = useCurrentWorkspace();
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);

  return (
    <FlexContainer direction="column" gap="xl">
      <Box>
        <Heading as="h1" size="md">
          <FormattedMessage id="settings.members" />
        </Heading>
      </Box>
      <UpdateWorkspaceNameForm />

      {isAccessManagementEnabled && (
        <FlexContainer direction="column" gap="xl">
          <WorkspaceAccessManagementSection />
        </FlexContainer>
      )}
      {canDeleteWorkspace && (
        <FlexContainer direction="column">
          <Heading as="h3" size="sm">
            <FormattedMessage id="settings.general.danger" />
          </Heading>
          <FlexContainer>
            <DeleteWorkspace />
          </FlexContainer>
        </FlexContainer>
      )}
    </FlexContainer>
  );
};
