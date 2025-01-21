import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import WorkspaceAccessManagementSection from "../pages/AccessManagementPage/WorkspaceAccessManagementSection";

export const WorkspaceMembersPage = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACEMEMBERS);

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer alignItems="center" wrap="wrap">
        <FlexItem grow>
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.workspace.members.title" />
          </Heading>
        </FlexItem>
      </FlexContainer>
      <WorkspaceAccessManagementSection />
    </FlexContainer>
  );
};
