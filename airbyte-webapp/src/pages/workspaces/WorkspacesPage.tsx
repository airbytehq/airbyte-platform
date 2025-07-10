import { useMutation } from "@tanstack/react-query";
import React, { useDeferredValue, useState } from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/HeadTitle";
import AirbyteLogo from "components/illustrations/airbyte-logo.svg?react";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { NoWorkspacePermissionsContent } from "area/workspace/components/NoWorkspacesPermissionWarning";
import { useListWorkspacesInfinite } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAuthService } from "core/services/auth";

import { useOrganizationsToCreateWorkspaces } from "./components/useOrganizationsToCreateWorkspaces";
import { WorkspacesCreateControl } from "./components/WorkspacesCreateControl";
import WorkspacesList from "./components/WorkspacesList";
import styles from "./WorkspacesPage.module.scss";

export const WORKSPACE_LIST_LENGTH = 50;

export const WorkspacesPage: React.FC = () => {
  const { isLoading: isLogoutLoading, mutateAsync: handleLogout } = useMutation(() => logout?.() ?? Promise.resolve());
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const [searchValue, setSearchValue] = useState("");
  const { organizationsMemberOnly, organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();
  const deferredSearchValue = useDeferredValue(searchValue);

  const {
    data: workspacesData,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
    isFetching,
    isLoading,
  } = useListWorkspacesInfinite(WORKSPACE_LIST_LENGTH, deferredSearchValue);

  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];

  const { logout } = useAuthService();

  /**
   * Check if we should show the "You don't have permission to anything" message, if:
   * - We're not currently still loading workspaces (i.e. we're not yet knowing if the user has access to any workspace potentially)
   * - User has no permissions to create a workspace (otherwise user could just create a workspace)
   * - No workspaces have been found (i.e. user doesn't have access to any workspace) while the search value was empty. Otherwise simply
   *   the search query couldn't have found any matching workspaces.
   * - Make sure `searchValue` and `deferredSearchValue` are the same, so we don't show the message after clearing out a query (that had no matches)
   *   but before the new query is triggered (and isFetching would be `true`) due to the debounce effect in starting the next query.
   */
  const showNoWorkspacesContent =
    !isFetching &&
    !organizationsToCreateIn.length &&
    !workspaces.length &&
    searchValue.length === 0 &&
    searchValue === deferredSearchValue;

  return (
    <>
      <HeadTitle titles={[{ id: "workspaces.title" }]} />
      <div className={styles.content}>
        <FlexContainer justifyContent="space-between">
          <AirbyteLogo className={styles.workspacesPage__logo} />
          {logout && (
            <Button variant="clear" onClick={() => handleLogout()} isLoading={isLogoutLoading}>
              <FormattedMessage id="settings.accountSettings.logoutText" />
            </Button>
          )}
        </FlexContainer>
        <FlexContainer justifyContent="center">
          <Heading as="h1" size="lg">
            <FormattedMessage id="workspaces.title" />
          </Heading>
        </FlexContainer>
        {showNoWorkspacesContent ? (
          <NoWorkspacePermissionsContent organizations={organizationsMemberOnly} />
        ) : (
          <>
            <Box py="xl">
              <Text align="center">
                <FormattedMessage id="workspaces.subtitle" />
              </Text>
            </Box>
            <Box pb="xl">
              <SearchInput value={searchValue} onChange={setSearchValue} data-testid="workspaces-page-search" />
            </Box>
            <Box pb="lg">
              <WorkspacesCreateControl />
            </Box>
            <Box pb="2xl">
              <WorkspacesList
                workspaces={workspaces}
                isLoading={isLoading}
                fetchNextPage={fetchNextPage}
                hasNextPage={hasNextPage}
              />
              {isFetchingNextPage ? (
                <Box py="2xl" className={styles.workspacesPage__loadingSpinner}>
                  <LoadingSpinner />
                </Box>
              ) : null}
            </Box>
          </>
        )}
      </div>
    </>
  );
};
