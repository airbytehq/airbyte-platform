import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace, useListUsersInOrganization } from "core/api";
import { isCloudApp } from "core/utils/app";
import { links } from "core/utils/links";

import styles from "./OrganizationAccessManagementSection.module.scss";
import { OrganizationUsersTable } from "./OrganizationUsersTable";

const SEARCH_PARAM = "search";

export const OrganizationAccessManagementSection: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const organization = useCurrentOrganizationInfo();
  const { users } = useListUsersInOrganization(workspace.organizationId);

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

  const filteredUsersWithAccess = users.filter((user) => {
    return (
      user.name?.toLowerCase().includes(filterParam?.toLowerCase() ?? "") ||
      user.email?.toLowerCase().includes(filterParam?.toLowerCase() ?? "")
    );
  });

  return (
    <FlexContainer direction="column" gap="md">
      <FlexContainer justifyContent="space-between" alignItems="baseline">
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.accessManagement.members" />
        </Heading>
      </FlexContainer>
      <FlexContainer justifyContent="space-between" alignItems="center">
        <FlexItem className={styles.searchInputWrapper}>
          <SearchInput value={userFilter} onChange={(e) => setUserFilter(e.target.value)} />
        </FlexItem>
        <Text size="md">
          {organization?.sso && (
            <Badge variant="blue">
              <FlexContainer gap="xs" alignItems="center">
                <Icon type="check" size="xs" />
                <Text size="sm">
                  <FormattedMessage id="settings.accessManagement.ssoEnabled" />
                </Text>
              </FlexContainer>
            </Badge>
          )}
          {!organization?.sso && isCloudApp() && (
            <ExternalLink href={links.contactSales}>
              <FormattedMessage id="settings.accessManagement.enableSso" />
            </ExternalLink>
          )}
        </Text>
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
