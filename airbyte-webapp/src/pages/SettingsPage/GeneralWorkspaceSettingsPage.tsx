import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { UpdateWorkspaceNameForm } from "area/workspace/components/UpdateWorkspaceNameForm";
import { useCurrentWorkspace } from "core/api";
import { useIntent } from "core/utils/rbac";

import { DeleteWorkspace } from "./components/DeleteWorkspace";

export const GeneralWorkspaceSettingsPage = () => {
  const { workspaceId } = useCurrentWorkspace();
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });

  return (
    <FlexContainer direction="column">
      <Card title={<FormattedMessage id="settings.generalSettings" />}>
        <Box p="xl">
          <UpdateWorkspaceNameForm />
        </Box>
      </Card>
      {canDeleteWorkspace && <DeleteWorkspace />}
    </FlexContainer>
  );
};
