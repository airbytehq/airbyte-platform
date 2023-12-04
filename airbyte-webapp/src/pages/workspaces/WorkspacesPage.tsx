import { useMutation } from "@tanstack/react-query";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useDebounce } from "react-use";

import { HeadTitle } from "components/common/HeadTitle";
import AirbyteLogo from "components/illustrations/airbyte-logo.svg?react";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { PageHeader } from "components/ui/PageHeader";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { NoWorkspacePermissionsContent } from "area/workspace/components/NoWorkspacesPermissionWarning";
import { useCreateWorkspace, useListWorkspacesInfinite } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAuthService } from "core/services/auth";

import { useOrganizationsToCreateWorkspaces } from "./components/useOrganizationsToCreateWorkspaces";
import { WorkspacesCreateControl } from "./components/WorkspacesCreateControl";
import WorkspacesList from "./components/WorkspacesList";
import styles from "./WorkspacesPage.module.scss";

export const WORKSPACE_LIST_LENGTH = 50;

const WorkspacesPage: React.FC = () => {
  const { isLoading: isLogoutLoading, mutateAsync: handleLogout } = useMutation(() => logout?.() ?? Promise.resolve());
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const [searchValue, setSearchValue] = useState("");
  const [isSearchEmpty, setIsSearchEmpty] = useState(true);
  const { organizationsMemberOnly, organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();
  const [debouncedSearchValue, setDebouncedSearchValue] = useState("");

  const {
    data: workspacesData,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
    isFetching,
    isLoading,
  } = useListWorkspacesInfinite(WORKSPACE_LIST_LENGTH, debouncedSearchValue);

  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];

  const { mutateAsync: createWorkspace } = useCreateWorkspace();
  const { logout } = useAuthService();

  const showNoWorkspacesContent = !isFetching && !organizationsToCreateIn.length && !workspaces.length && isSearchEmpty;

  useDebounce(
    () => {
      setDebouncedSearchValue(searchValue);
      setIsSearchEmpty(searchValue === "");
    },
    250,
    [searchValue]
  );

  return (
    <>
      <HeadTitle titles={[{ id: "workspaces.title" }]} />
      <Box px="lg" className={styles.brandingHeader}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <AirbyteLogo width={110} />
          {logout && (
            <Button variant="clear" onClick={() => handleLogout()} isLoading={isLogoutLoading}>
              <FormattedMessage id="settings.accountSettings.logoutText" />
            </Button>
          )}
        </FlexContainer>
      </Box>
      <PageHeader
        leftComponent={
          <FlexContainer direction="column" alignItems="flex-start" justifyContent="flex-start">
            <FlexContainer direction="row" gap="none">
              <Heading as="h1" size="md">
                <FormattedMessage id="workspaces.title" />
              </Heading>
              <InfoTooltip>
                <Text inverseColor>
                  <FormattedMessage id="workspaces.subtitle" />
                </Text>
              </InfoTooltip>
            </FlexContainer>
          </FlexContainer>
        }
      />
      <Box py="2xl" className={styles.content}>
        {showNoWorkspacesContent ? (
          <NoWorkspacePermissionsContent organizations={organizationsMemberOnly} />
        ) : (
          <>
            <Box pb="xl">
              <SearchInput value={searchValue} onChange={(e) => setSearchValue(e.target.value)} />
            </Box>
            <Box pb="lg">
              <WorkspacesCreateControl createWorkspace={createWorkspace} />
            </Box>
            <WorkspacesList
              workspaces={workspaces}
              isLoading={isLoading}
              fetchNextPage={fetchNextPage}
              hasNextPage={hasNextPage}
            />
            {isFetchingNextPage && (
              <Box py="2xl">
                <LoadingSpinner />
              </Box>
            )}
          </>
        )}
      </Box>
    </>
  );
};

export default WorkspacesPage;
