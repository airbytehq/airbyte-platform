import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { OrganizationUserRead, WorkspaceUserRead } from "core/api/types/AirbyteClient";

import { AccessManagementTable } from "./AccessManagementTable";
import { AddUserControl } from "./AddUserControl";
import { ResourceType, tableTitleDictionary } from "./useGetAccessManagementData";

interface AccessManagementSectionProps {
  users?: WorkspaceUserRead[] | OrganizationUserRead[];
  usersToAdd?: OrganizationUserRead[];
  tableResourceType: ResourceType;
  pageResourceType: ResourceType;
  pageResourceName: string;
}

/**
 * @deprecated will be removed when RBAC UI v2 is turned on
 */
export const AccessManagementSection: React.FC<AccessManagementSectionProps> = ({
  users,
  usersToAdd,
  tableResourceType,
  pageResourceType,
  pageResourceName,
}) => {
  const showAddUsersButton = usersToAdd && usersToAdd.length > 0;

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer justifyContent="space-between" alignItems="baseline">
        <Box pl="sm">
          <Heading as="h3" size="sm">
            <FormattedMessage id={tableTitleDictionary[tableResourceType]} />
          </Heading>
        </Box>
        {showAddUsersButton && (
          // the empty array is not a possible state, but is required to prevent a type error... will be cleaner when types are moved over in full
          <AddUserControl usersToAdd={usersToAdd} />
        )}
      </FlexContainer>
      {users && users.length > 0 ? (
        <AccessManagementTable
          users={users}
          tableResourceType={tableResourceType}
          pageResourceName={pageResourceName}
          pageResourceType={pageResourceType}
        />
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
