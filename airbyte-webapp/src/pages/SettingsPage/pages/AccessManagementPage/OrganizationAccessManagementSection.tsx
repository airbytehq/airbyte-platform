import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useListUsersInOrganization } from "core/api";

import styles from "./OrganizationAccessManagementSection.module.scss";
import { OrganizationUsersTable } from "./OrganizationUsersTable";

const SEARCH_PARAM = "search";

export const OrganizationAccessManagementSection: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const organizationUsers = useListUsersInOrganization(workspace.organizationId ?? "").users;
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

  const filteredUsersWithAccess = organizationUsers.filter((user) => {
    return (
      user.name?.toLowerCase().includes(filterParam?.toLowerCase() ?? "") ||
      user.email?.toLowerCase().includes(filterParam?.toLowerCase() ?? "")
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
      </FlexContainer>
      {filteredUsersWithAccess && filteredUsersWithAccess.length > 0 ? (
        <OrganizationUsersTable users={filteredUsersWithAccess} />
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
