import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useListWorkspaceAccessUsers } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";

import { AddUserControl } from "./components/AddUserControl";
import styles from "./WorkspaceAccessManagementSection.module.scss";
import { WorkspaceUsersTable } from "./WorkspaceUsersTable";

const SEARCH_PARAM = "search";

const WorkspaceAccessManagementSection: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE_ACCESS_MANAGEMENT);
  const workspace = useCurrentWorkspace();
  const canViewOrgMembers = useIntent("ListOrganizationMembers", { organizationId: workspace.organizationId });
  const canUpdateWorkspacePermissions = useIntent("UpdateWorkspacePermissions", { workspaceId: workspace.workspaceId });

  const { usersWithAccess } = useListWorkspaceAccessUsers(workspace.workspaceId);

  const [searchParams, setSearchParams] = useSearchParams();
  const filterParam = searchParams.get("search");
  const [userFilter, setUserFilter] = React.useState(filterParam ?? "");
  const debouncedUserFilter = useDeferredValue(userFilter);

  useEffect(() => {
    if (debouncedUserFilter) {
      searchParams.set(SEARCH_PARAM, debouncedUserFilter);
    } else {
      searchParams.delete(SEARCH_PARAM);
    }
    setSearchParams(searchParams);
  }, [debouncedUserFilter, searchParams, setSearchParams]);

  const filteredUsersWithAccess = usersWithAccess.filter((user) => {
    return (
      user.userName?.toLowerCase().includes(filterParam?.toLowerCase() ?? "") ||
      user.userEmail?.toLowerCase().includes(filterParam?.toLowerCase() ?? "")
    );
  });

  return (
    <FlexContainer direction="column" gap="md">
      <FlexContainer justifyContent="space-between" alignItems="baseline">
        <Text size="lg">
          <FormattedMessage id="settings.accessManagement.members" />
        </Text>
      </FlexContainer>
      <FlexContainer justifyContent="space-between" alignItems="center">
        <FlexItem className={styles.searchInputWrapper}>
          <SearchInput value={userFilter} onChange={(e) => setUserFilter(e.target.value)} />
        </FlexItem>
        {canViewOrgMembers && canUpdateWorkspacePermissions && <AddUserControl />}
      </FlexContainer>
      {filteredUsersWithAccess && filteredUsersWithAccess.length > 0 ? (
        <WorkspaceUsersTable users={filteredUsersWithAccess} />
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

export default WorkspaceAccessManagementSection;
