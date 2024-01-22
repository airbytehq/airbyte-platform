import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { AddUserControl } from "./components/AddUserControl";
import { useNextGetWorkspaceAccessUsers } from "./components/useGetAccessManagementData";
import { WorkspaceUsersTable } from "./WorkspaceUsersTable";

const NextWorkspaceAccessManagementPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE_ACCESS_MANAGEMENT);
  const accessData = useNextGetWorkspaceAccessUsers();
  const usersWithAccess = accessData.workspace?.users ?? [];
  const usersToAdd = accessData.workspace?.usersToAdd ?? [];

  const showAddUsersButton = usersToAdd && usersToAdd.length > 0;

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer justifyContent="space-between" alignItems="baseline">
        <Box pl="sm">
          <Heading as="h3" size="sm">
            <FormattedMessage id="resource.workspace" />
          </Heading>
        </Box>
        {showAddUsersButton && <AddUserControl usersToAdd={usersToAdd} />}
      </FlexContainer>
      {usersWithAccess && usersWithAccess.length > 0 ? (
        <WorkspaceUsersTable users={usersWithAccess} />
      ) : (
        <Box py="xl" pl="lg">
          <Text color="grey" italicized>
            <FormattedMessage id="settings.accessManagement.noUsers" />
          </Text>
        </Box>
      )}
    </FlexContainer>
  );
};

export default NextWorkspaceAccessManagementPage;
