import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { UpdateWorkspaceNameForm } from "area/workspace/components/UpdateWorkspaceNameForm";

import { DeleteWorkspace } from "./components/DeleteWorkspace";

export const GeneralWorkspaceSettingsPage = () => {
  return (
    <FlexContainer direction="column">
      <Card title={<FormattedMessage id="settings.generalSettings" />}>
        <Box p="xl">
          <UpdateWorkspaceNameForm />
        </Box>
      </Card>
      <DeleteWorkspace />
    </FlexContainer>
  );
};
