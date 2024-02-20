import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace } from "core/api";
import { OrganizationUserRead, WorkspaceUserRead } from "core/api/types/AirbyteClient";
import { useIntent } from "core/utils/rbac";

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
  tableResourceType,
  pageResourceType,
  pageResourceName,
}) => {
  const { workspaceId } = useCurrentWorkspace();
  const organizationInfo = useCurrentOrganizationInfo();
  const canAddUsers = useIntent("UpdateWorkspacePermissions", { workspaceId });
  const canListOrganizationMembers = useIntent("ListOrganizationMembers", {
    organizationId: organizationInfo?.organizationId,
  });

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer justifyContent="space-between" alignItems="baseline">
        <Box pl="sm">
          <Heading as="h3" size="sm">
            <FormattedMessage id={tableTitleDictionary[tableResourceType]} />
          </Heading>
        </Box>
        {canAddUsers && canListOrganizationMembers && tableResourceType === "workspace" && <AddUserControl />}
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
