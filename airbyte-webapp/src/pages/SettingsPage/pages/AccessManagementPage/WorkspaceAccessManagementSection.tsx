import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { AddUserControl } from "./components/AddUserControl";
import { useNextGetWorkspaceAccessUsers } from "./components/useGetAccessManagementData";
import styles from "./WorkspaceAccessManagementSection.module.scss";
import { WorkspaceUsersTable } from "./WorkspaceUsersTable";

const SEARCH_PARAM = "search";

const WorkspaceAccessManagementSection: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE_ACCESS_MANAGEMENT);
  const accessData = useNextGetWorkspaceAccessUsers();
  const usersWithAccess = accessData.workspace?.users ?? [];
  const usersToAdd = accessData.workspace?.usersToAdd ?? [];
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
      user.name?.toLowerCase().includes(filterParam?.toLowerCase() ?? "") ||
      user.email?.toLowerCase().includes(filterParam?.toLowerCase() ?? "")
    );
  });

  const showAddUsersButton = usersToAdd && usersToAdd.length > 0;
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
        {showAddUsersButton && <AddUserControl usersToAdd={usersToAdd} />}
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
