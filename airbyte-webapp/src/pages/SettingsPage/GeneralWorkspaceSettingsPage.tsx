import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";

import { UpdateWorkspaceNameForm } from "area/workspace/components/UpdateWorkspaceNameForm";

export const GeneralWorkspaceSettingsPage = () => {
  return (
    <Card title={<FormattedMessage id="settings.generalSettings" />}>
      <Box p="xl">
        <UpdateWorkspaceNameForm />
      </Box>
    </Card>
  );
};
