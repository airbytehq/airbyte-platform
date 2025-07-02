import capitalize from "lodash/capitalize";
import React, { useDeferredValue, useState, Suspense } from "react";
import { useIntl } from "react-intl";
import { useToggle } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useListWorkspacesInOrganization, useOrganization, useListUsersInOrganization } from "core/api";
import { useWebappConfig } from "core/config";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { OrganizationWorkspacesCreateControl } from "./components/OrganizationWorkspacesCreateControl";
import { OrganizationWorkspacesList } from "./components/OrganizationWorkspacesList";
import { useOrganizationsToCreateWorkspaces } from "./components/useOrganizationsToCreateWorkspaces";
import styles from "./OrganizationWorkspacesPage.module.scss";

export const WORKSPACE_LIST_LENGTH = 50;

const WorkspacesContent: React.FC = () => {
  const [searchValue, setSearchValue] = useState("");
  const [filterUnhealthyWorkspaces, setFilterUnhealthyWorkspaces] = useToggle(false);
  const [filterRunningSyncs, setFilterRunningSyncs] = useToggle(false);
  const { organizationsMemberOnly } = useOrganizationsToCreateWorkspaces();
  const deferredSearchValue = useDeferredValue(searchValue);
  const currentOrganizationId = useCurrentOrganizationId();
  const organization = useOrganization(currentOrganizationId);
  const { edition } = useWebappConfig();
  const { users } = useListUsersInOrganization(currentOrganizationId);
  const { formatMessage } = useIntl();

  const { workspaces } = useListWorkspacesInOrganization({ organizationId: currentOrganizationId });
  const showNoWorkspacesYet = workspaces.length === 0;

  // Filter workspaces based on search value
  const filteredWorkspaces = workspaces.filter((workspace) =>
    workspace.name.toLowerCase().includes(deferredSearchValue.toLowerCase())
  );

  /**
   * Check if we should show the "You don't have permission to anything" message, if:
   * - User is only a member of the current organization
   */
  const showNoWorkspacesPermission = organizationsMemberOnly.some(
    (org) => org.organizationId === organization.organizationId
  );

  return (
    <div className={styles.background}>
      <FlexContainer direction="column" alignItems="center">
        <div className={styles.content}>
          <FlexContainer justifyContent="space-between" alignItems="center" className={styles.headerRow}>
            <Box>
              <Heading as="h1" size="lg">
                {organization.organizationName}
              </Heading>
              <Text color="grey400" size="sm" className={styles.metaInfo}>
                {capitalize(edition)} &bull; {formatMessage({ id: "organization.members" }, { count: users.length })}
              </Text>
            </Box>
            <Box pb="lg">
              <OrganizationWorkspacesCreateControl disabled={showNoWorkspacesPermission} />
            </Box>
          </FlexContainer>
          <Box pb="sm">
            <SearchInput
              value={searchValue}
              onChange={(e) => setSearchValue(e.target.value)}
              data-testid="workspaces-page-search"
            />
          </Box>
          {!showNoWorkspacesYet && (
            <FlexContainer gap="md" alignItems="center" className={styles.filterButtonsRow}>
              <Button
                variant={filterUnhealthyWorkspaces ? "primary" : "secondary"}
                type="button"
                onClick={setFilterUnhealthyWorkspaces}
              >
                {formatMessage({ id: "organization.unhealthyWorkspaces" })}
              </Button>
              <Button
                variant={filterRunningSyncs ? "primary" : "secondary"}
                type="button"
                onClick={setFilterRunningSyncs}
              >
                {formatMessage({ id: "organization.runningSyncs" })}
              </Button>
            </FlexContainer>
          )}
          <Box pb="2xl">
            <OrganizationWorkspacesList
              workspaces={filteredWorkspaces}
              isLoading={false}
              filterUnhealthyWorkspaces={filterUnhealthyWorkspaces}
              filterRunningSyncs={filterRunningSyncs}
              showNoWorkspacesPermission={showNoWorkspacesPermission}
              showNoWorkspacesYet={showNoWorkspacesYet}
              showNoWorkspacesFound={filteredWorkspaces.length === 0 && searchValue.length > 0}
            />
          </Box>
        </div>
      </FlexContainer>
    </div>
  );
};

export const OrganizationWorkspacesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.WORKSPACES);

  return (
    <Suspense
      fallback={
        <Box py="2xl" className={styles.workspacesPage__loadingSpinner}>
          <LoadingSpinner />
        </Box>
      }
    >
      <WorkspacesContent />
    </Suspense>
  );
};

export default OrganizationWorkspacesPage;
