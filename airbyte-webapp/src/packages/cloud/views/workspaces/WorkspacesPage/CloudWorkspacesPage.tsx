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
import { useListCloudWorkspacesInfinite } from "core/api/cloud";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { useOrganizationsToCreateWorkspaces } from "pages/workspaces/components/useOrganizationsToCreateWorkspaces";
import WorkspacesList from "pages/workspaces/components/WorkspacesList";
import { WORKSPACE_LIST_LENGTH } from "pages/workspaces/WorkspacesPage";

import { CloudWorkspacesCreateControl } from "./CloudWorkspacesCreateControl";
import styles from "./CloudWorkspacesPage.module.scss";

export const CloudWorkspacesPageInner: React.FC = () => {
  const { isLoading: isLogoutLoading, mutateAsync: handleLogout } = useMutation(() => logout?.() ?? Promise.resolve());
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const [searchValue, setSearchValue] = useState("");
  const deferredSearchValue = useDeferredValue(searchValue);

  const {
    data: workspacesData,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
    isFetching,
    isLoading,
  } = useListCloudWorkspacesInfinite(WORKSPACE_LIST_LENGTH, deferredSearchValue);

  const { organizationsMemberOnly, organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();

  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];

  const { logout } = useAuthService();

  /**
   * Check if we should show the "You don't have permission to anything" message, if:
   * - We're not currently still loading workspaces (i.e. we're not yet knowing if the user has access to any workspace potentially)
   * - User has no permissions to create a workspace in any of those organizations (otherwise user could just create a workspace)
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
    <div className={styles.cloudWorkspacesPage__container}>
      <FlexContainer justifyContent="space-between">
        <AirbyteLogo className={styles.cloudWorkspacesPage__logo} />
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
            <SearchInput value={searchValue} onChange={(e) => setSearchValue(e.target.value)} />
          </Box>
          <Box pb="lg">
            <CloudWorkspacesCreateControl />
          </Box>
          <Box pb="2xl">
            <WorkspacesList
              workspaces={workspaces}
              isLoading={isLoading}
              fetchNextPage={fetchNextPage}
              hasNextPage={hasNextPage}
            />
            {isFetchingNextPage ? (
              <Box py="2xl" className={styles.cloudWorkspacesPage__loadingSpinner}>
                <LoadingSpinner />
              </Box>
            ) : null}
          </Box>
        </>
      )}
    </div>
  );
};

export const CloudWorkspacesPage = () => {
  return (
    <>
      <HeadTitle titles={[{ id: "workspaces.title" }]} />
      <CloudWorkspacesPageInner />
    </>
  );
};
