import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { UpdateWorkspaceNameForm } from "area/workspace/components/UpdateWorkspaceNameForm";
import { useCurrentWorkspace } from "core/api";
import { useIntent } from "core/utils/rbac";

import { DeleteWorkspace } from "./components/DeleteWorkspace";

export const GeneralWorkspaceSettingsPage = () => {
  const { workspaceId } = useCurrentWorkspace();
  const { formatMessage } = useIntl();
  const canDeleteWorkspace = useIntent("DeleteWorkspace", { workspaceId });

  return (
    <FlexContainer direction="column">
      <Card title={formatMessage({ id: "settings.generalSettings" })} titleWithBottomBorder>
        <UpdateWorkspaceNameForm />
      </Card>
      {canDeleteWorkspace && <DeleteWorkspace />}
    </FlexContainer>
  );
};
