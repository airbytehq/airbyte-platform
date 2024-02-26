import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace, useListWorkspaceAccessUsers } from "core/api";
import { useIntent } from "core/utils/rbac";
import { FirebaseInviteUserButton } from "packages/cloud/views/workspaces/WorkspaceSettingsView/components/FirebaseInviteUserButton";

import { AddUserControl } from "./components/AddUserControl";
import styles from "./WorkspaceAccessManagementSection.module.scss";
import { WorkspaceUsersTable } from "./WorkspaceUsersTable";

const SEARCH_PARAM = "search";

const WorkspaceAccessManagementSection: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const organization = useCurrentOrganizationInfo();
  const canViewOrgMembers = useIntent("ListOrganizationMembers", { organizationId: organization?.organizationId });
  const canUpdateWorkspacePermissions = useIntent("UpdateWorkspacePermissions", { workspaceId: workspace.workspaceId });

  const usersWithAccess = useListWorkspaceAccessUsers(workspace.workspaceId).usersWithAccess;

  const [searchParams, setSearchParams] = useSearchParams();
  const filterParam = searchParams.get("search");
  const [userFilter, setUserFilter] = React.useState(filterParam ?? "");
  const debouncedUserFilter = useDeferredValue(userFilter);

  const isWorkspaceInOrg = organization?.sso && canUpdateWorkspacePermissions && canViewOrgMembers;
  const showFirebaseInviteButton = !organization?.sso && canUpdateWorkspacePermissions;

  useEffect(() => {
    if (debouncedUserFilter) {
      searchParams.set(SEARCH_PARAM, debouncedUserFilter);
    } else {
      searchParams.delete(SEARCH_PARAM);
    }
    setSearchParams(searchParams);
  }, [debouncedUserFilter, searchParams, setSearchParams]);

  const filteredUsersWithAccess = (usersWithAccess ?? []).filter((user) => {
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
        {showFirebaseInviteButton && <FirebaseInviteUserButton />}
        {isWorkspaceInOrg && <AddUserControl />}
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
