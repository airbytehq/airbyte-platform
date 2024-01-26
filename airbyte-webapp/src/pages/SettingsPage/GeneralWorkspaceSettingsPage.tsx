import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { UpdateWorkspaceNameForm } from "area/workspace/components/UpdateWorkspaceNameForm";
import { useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";

import { DeleteWorkspace } from "./components/DeleteWorkspace";
import WorkspaceAccessManagementSection from "./pages/AccessManagementPage/WorkspaceAccessManagementSection";

export const GeneralWorkspaceSettingsPage = () => {
  const { workspaceId } = useCurrentWorkspace();
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });
  const updatedOrganizationsUI = useExperiment("settings.organizationsUpdates", false);
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);

  return (
    <FlexContainer direction="column" gap="xl">
      <Box p="xl">
        <Heading as="h2" size="md">
          <FormattedMessage id="settings.generalSettings" />
        </Heading>
      </Box>
      <Card>
        <Box p="xl">
          <UpdateWorkspaceNameForm />
        </Box>
      </Card>

      {isAccessManagementEnabled && updatedOrganizationsUI && (
        <Card>
          <Box p="xl">
            <FlexContainer direction="column" gap="xl">
              <WorkspaceAccessManagementSection />
            </FlexContainer>
          </Box>
        </Card>
      )}
      {canDeleteWorkspace && (
        <Card>
          <Box p="xl">
            <FlexContainer direction="column">
              <Heading as="h3" size="sm">
                <FormattedMessage id="settings.general.danger" />
              </Heading>
              <FlexContainer>
                <DeleteWorkspace />
              </FlexContainer>
            </FlexContainer>
          </Box>
        </Card>
      )}
    </FlexContainer>
  );
};
