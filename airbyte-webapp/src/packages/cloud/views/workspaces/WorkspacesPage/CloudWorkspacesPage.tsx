import { useMutation } from "@tanstack/react-query";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useDebounce } from "react-use";

import AirbyteLogo from "components/illustrations/airbyte-logo.svg?react";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useListCloudWorkspacesInfinite } from "core/api/cloud";
import { OrganizationRead } from "core/request/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { useOrganizationsToCreateWorkspaces } from "pages/workspaces/components/useOrganizationsToCreateWorkspaces";
import WorkspacesList from "pages/workspaces/components/WorkspacesList";
import { WORKSPACE_LIST_LENGTH } from "pages/workspaces/WorkspacesPage";

import { CloudWorkspacesCreateControl } from "./CloudWorkspacesCreateControl";
import styles from "./CloudWorkspacesPage.module.scss";
import OctaviaThinking from "./octavia-thinking-no-gears.svg?react";

export const CloudWorkspacesPage: React.FC = () => {
  const { isLoading, mutateAsync: handleLogout } = useMutation(() => logout?.() ?? Promise.resolve());
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const [searchValue, setSearchValue] = useState("");
  const [isSearchEmpty, setIsSearchEmpty] = useState(true);

  const {
    data: workspacesData,
    refetch,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
    isFetching,
  } = useListCloudWorkspacesInfinite(WORKSPACE_LIST_LENGTH, searchValue);

  const { organizationsMemberOnly, organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();

  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];

  const { logout } = useAuthService();

  const showNoWorkspacesContent =
    !isFetching &&
    !organizationsToCreateIn.length &&
    organizationsMemberOnly.length > 0 &&
    !workspaces.length &&
    isSearchEmpty;

  useDebounce(
    () => {
      refetch();
      setIsSearchEmpty(searchValue === "");
    },
    250,
    [searchValue]
  );

  return (
    <div className={styles.cloudWorkspacesPage__container}>
      <FlexContainer justifyContent="space-between">
        <AirbyteLogo className={styles.cloudWorkspacesPage__logo} />
        {logout && (
          <Button variant="clear" onClick={() => handleLogout()} isLoading={isLoading}>
            <FormattedMessage id="settings.accountSettings.logoutText" />
          </Button>
        )}
      </FlexContainer>
      <Heading as="h1" size="lg" centered>
        <FormattedMessage id="workspaces.title" />
      </Heading>
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
            <WorkspacesList workspaces={workspaces} fetchNextPage={fetchNextPage} hasNextPage={hasNextPage} />
            {isFetchingNextPage && (
              <Box py="2xl" className={styles.cloudWorkspacesPage__loadingSpinner}>
                <LoadingSpinner />
              </Box>
            )}
          </Box>
        </>
      )}
    </div>
  );
};

const NoWorkspacePermissionsContent: React.FC<{ organizations: OrganizationRead[] }> = ({ organizations }) => {
  return (
    <Box m="2xl" p="2xl" data-testid="noWorkspacePermissionsBanner">
      <FlexContainer direction="column" gap="2xl">
        <OctaviaThinking className={styles.cloudWorkspacesPage__illustration} />
        <div>
          <Box pb="md">
            <Text size="md" align="center" bold>
              <FormattedMessage id="workspaces.noPermissions" />
            </Text>
          </Box>
          <Text size="md" align="center" color="grey">
            <FormattedMessage
              id="workspaces.noPermissions.moreInformation"
              values={{
                adminEmail: organizations[0].email,
                lnk: (...lnk: React.ReactNode[]) => (
                  <ExternalLink href={`mailto:${organizations[0].email}`}>{lnk}</ExternalLink>
                ),
              }}
            />
          </Text>
        </div>
      </FlexContainer>
    </Box>
  );
};
