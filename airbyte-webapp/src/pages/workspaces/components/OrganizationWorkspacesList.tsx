import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex/FlexContainer";
import { Icon } from "components/ui/Icon";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { NoWorkspacePermissionsContent } from "area/workspace/components/NoWorkspacesPermissionWarning";
import { useOrganization } from "core/api";

import OrganizationWorkspaceItem from "./OrganizationWorkspaceItem";
import OrganizationWorkspacesCreateControlEmptyStateButton from "./OrganizationWorkspacesCreateControlEmptyStateButton";
import styles from "../OrganizationWorkspacesPage.module.scss";

interface OrganizationWorkspacesListProps {
  workspaces: Array<{ workspaceId: string; name: string }>;
  isLoading: boolean;
  filterUnhealthyWorkspaces?: boolean;
  filterRunningSyncs?: boolean;
  showNoWorkspacesPermission?: boolean;
  showNoWorkspacesYet?: boolean;
  showNoWorkspacesFound?: boolean;
}

export const OrganizationWorkspacesList: React.FC<OrganizationWorkspacesListProps> = ({
  workspaces,
  isLoading,
  filterUnhealthyWorkspaces = false,
  filterRunningSyncs = false,
  showNoWorkspacesPermission = false,
  showNoWorkspacesYet = false,
  showNoWorkspacesFound = false,
}) => {
  const currentOrganizationId = useCurrentOrganizationId();
  const currentOrganization = useOrganization(currentOrganizationId);

  if (isLoading) {
    return (
      <Box py="2xl" className={styles.workspacesPage__loadingSpinner}>
        <LoadingSpinner />
      </Box>
    );
  }

  if (showNoWorkspacesPermission) {
    return <NoWorkspacePermissionsContent organizations={[currentOrganization]} />;
  }

  if (showNoWorkspacesYet) {
    return (
      <FlexContainer direction="column" alignItems="center" justifyContent="flex-start">
        <Box mb="sm" mt="xl">
          <FlexContainer alignItems="center" justifyContent="center" className={styles.emptyStateIconBackground}>
            <Icon type="grid" size="xl" className={styles.emptyStateIcon} />
          </FlexContainer>
        </Box>
        <Box mb="sm">
          <Text size="md" color="grey500">
            <FormattedMessage id="workspaces.noWorkspacesYet" />
          </Text>
        </Box>
        <OrganizationWorkspacesCreateControlEmptyStateButton />
      </FlexContainer>
    );
  }

  if (showNoWorkspacesFound) {
    return (
      <Box mt="xl">
        <FlexContainer direction="column" alignItems="center" justifyContent="flex-start">
          <Text size="md">
            <FormattedMessage id="workspaces.noWorkspaces" />
          </Text>
        </FlexContainer>
      </Box>
    );
  }

  return (
    <Box>
      {workspaces.map((workspace) => (
        <OrganizationWorkspaceItem
          key={workspace.workspaceId}
          workspace={workspace}
          filterUnhealthyWorkspaces={filterUnhealthyWorkspaces}
          filterRunningSyncs={filterRunningSyncs}
        />
      ))}
    </Box>
  );
};

export default OrganizationWorkspacesList;
