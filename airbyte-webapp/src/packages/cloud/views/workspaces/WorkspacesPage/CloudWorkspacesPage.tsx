import { useMutation } from "@tanstack/react-query";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useDebounce } from "react-use";

import { ReactComponent as AirbyteLogo } from "components/illustrations/airbyte-logo.svg";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCreateCloudWorkspace, useListCloudWorkspacesInfinite } from "core/api/cloud";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import WorkspacesList from "pages/workspaces/components/WorkspacesList";
import { WORKSPACE_LIST_LENGTH } from "pages/workspaces/WorkspacesPage";

import { CloudWorkspacesCreateControl } from "./CloudWorkspacesCreateControl";
import styles from "./CloudWorkspacesPage.module.scss";

export const CloudWorkspacesPage: React.FC = () => {
  const { isLoading, mutateAsync: handleLogout } = useMutation(() => logout?.() ?? Promise.resolve());
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const [searchValue, setSearchValue] = useState("");

  const {
    data: workspacesData,
    refetch,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
  } = useListCloudWorkspacesInfinite(WORKSPACE_LIST_LENGTH, searchValue);

  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];

  const { mutateAsync: createWorkspace } = useCreateCloudWorkspace();
  const { logout } = useAuthService();

  useDebounce(
    () => {
      setSearchValue(searchValue);
      refetch();
    },
    250,
    [searchValue]
  );

  return (
    <div className={styles.container}>
      <FlexContainer justifyContent="space-between">
        <AirbyteLogo className={styles.logo} />
        {logout && (
          <Button variant="clear" onClick={() => handleLogout()} isLoading={isLoading}>
            <FormattedMessage id="settings.accountSettings.logoutText" />
          </Button>
        )}
      </FlexContainer>
      <Heading as="h1" size="lg" centered>
        <FormattedMessage id="workspaces.title" />
      </Heading>
      <Box py="xl">
        <Text align="center">
          <FormattedMessage id="workspaces.subtitle" />
        </Text>
      </Box>
      <Box pb="xl">
        <SearchInput value={searchValue} onChange={(e) => setSearchValue(e.target.value)} />
      </Box>
      <Box pb="lg">
        <CloudWorkspacesCreateControl createWorkspace={createWorkspace} />
      </Box>
      <Box pb="2xl">
        <WorkspacesList workspaces={workspaces} fetchNextPage={fetchNextPage} hasNextPage={hasNextPage} />
        {isFetchingNextPage && (
          <Box py="2xl" className={styles.loadingSpinner}>
            <LoadingSpinner />
          </Box>
        )}
      </Box>
    </div>
  );
};
