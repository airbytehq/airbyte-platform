import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useListUsersInOrganization } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { OrganizationUsersTable } from "./OrganizationUsersTable";

const NextOrganizationAccessManagementPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_ACCESS_MANAGEMENT);
  const workspace = useCurrentWorkspace();
  const organizationUsers = useListUsersInOrganization(workspace.organizationId ?? "").users;

  return (
    <FlexContainer direction="column" gap="xl">
      <Box pl="sm">
        <Heading as="h3" size="sm">
          <FormattedMessage id="resource.organization" />
        </Heading>
      </Box>
      {organizationUsers && organizationUsers.length > 0 ? (
        <OrganizationUsersTable users={organizationUsers} />
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
export default NextOrganizationAccessManagementPage;
