import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { UpdateWorkspaceSettingsForm } from "area/workspace/components/UpdateWorkspaceSettingsForm";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { TagsTable } from "./components/TagsTable";
import { DeleteWorkspace } from "../components/DeleteWorkspace";

export const GeneralWorkspaceSettingsPage = () => {
  const canViewWorkspaceSettings = useGeneratedIntent(Intent.ViewWorkspaceSettings);
  const canDeleteWorkspace = useGeneratedIntent(Intent.DeleteWorkspace);
  const showOrganizationUI = useFeature(FeatureItem.OrganizationUI);

  return (
    <FlexContainer direction="column" gap="xl">
      <Box>
        <Heading as="h1" size="md">
          <FormattedMessage id="settings.workspace.general.title" />
        </Heading>
      </Box>
      {canViewWorkspaceSettings && <UpdateWorkspaceSettingsForm />}
      <TagsTable />
      {showOrganizationUI && canDeleteWorkspace && (
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
