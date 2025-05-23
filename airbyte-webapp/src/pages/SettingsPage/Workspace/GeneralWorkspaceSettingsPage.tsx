import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { UpdateWorkspaceSettingsForm } from "area/workspace/components/UpdateWorkspaceSettingsForm";
import { useCurrentWorkspace } from "core/api";
import { useFeature, FeatureItem } from "core/services/features";
import { useIntent } from "core/utils/rbac";

import { TagsTable } from "./components/TagsTable";
import { DeleteWorkspace } from "../components/DeleteWorkspace";

export const GeneralWorkspaceSettingsPage = () => {
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const { workspaceId } = useCurrentWorkspace();
  const canViewWorkspaceSettings = useIntent("ViewWorkspaceSettings", { workspaceId });
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });

  return (
    <FlexContainer direction="column" gap="xl">
      <Box>
        <Heading as="h1" size="md">
          <FormattedMessage id="settings.workspace.general.title" />
        </Heading>
      </Box>
      {canViewWorkspaceSettings && multiWorkspaceUI && <UpdateWorkspaceSettingsForm />}
      <TagsTable />
      {canViewWorkspaceSettings && multiWorkspaceUI && canDeleteWorkspace && (
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
