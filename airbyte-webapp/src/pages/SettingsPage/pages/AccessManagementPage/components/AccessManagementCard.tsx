import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { OrganizationUserRead, WorkspaceUserRead } from "core/api/types/AirbyteClient";

import { AccessManagementTable } from "./AccessManagementTable";
import { AddUserControl } from "./AddUserControl";
import { ResourceType, tableTitleDictionary } from "./useGetAccessManagementData";

interface AccessManagementCardProps {
  users: OrganizationUserRead[] | WorkspaceUserRead[];
  usersToAdd: OrganizationUserRead[];
  tableResourceType: ResourceType;
  pageResourceType: ResourceType;
  pageResourceName: string;
}
export const AccessManagementCard: React.FC<AccessManagementCardProps> = ({
  users,
  usersToAdd,
  tableResourceType,
  pageResourceType,
  pageResourceName,
}) => {
  return (
    <Card
      title={
        <FlexContainer justifyContent="space-between" alignItems="baseline">
          <FormattedMessage id={tableTitleDictionary[tableResourceType]} />
          {usersToAdd.length > 0 && <AddUserControl usersToAdd={usersToAdd} />}
        </FlexContainer>
      }
    >
      {!users || users.length === 0 ? (
        <Box py="xl" pl="lg">
          <Text color="grey" italicized>
            <FormattedMessage id="settings.accessManagement.noUsers" />
          </Text>
        </Box>
      ) : (
        <AccessManagementTable
          users={users}
          tableResourceType={tableResourceType}
          pageResourceName={pageResourceName}
          pageResourceType={pageResourceType}
        />
      )}
    </Card>
  );
};
