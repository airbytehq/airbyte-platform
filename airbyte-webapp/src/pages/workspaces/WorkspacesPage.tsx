import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useDebounce } from "react-use";

import { HeadTitle } from "components/common/HeadTitle";
import { ReactComponent as AirbyteLogo } from "components/illustrations/airbyte-logo.svg";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { PageHeader } from "components/ui/PageHeader";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useCreateWorkspace, useListWorkspacesInfinite } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { WorkspacesCreateControl } from "./components/WorkspacesCreateControl";
import WorkspacesList from "./components/WorkspacesList";
import styles from "./WorkspacesPage.module.scss";

export const WORKSPACE_LIST_LENGTH = 50;

const WorkspacesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const [searchValue, setSearchValue] = useState("");

  const {
    data: workspacesData,
    refetch,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
  } = useListWorkspacesInfinite(WORKSPACE_LIST_LENGTH, searchValue);

  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];

  const { mutateAsync: createWorkspace } = useCreateWorkspace();

  useDebounce(
    () => {
      refetch();
    },
    250,
    [searchValue]
  );

  return (
    <>
      <HeadTitle titles={[{ id: "workspaces.title" }]} />
      <Box px="lg" className={styles.brandingHeader}>
        <AirbyteLogo width={110} />
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
        <Box pb="xl">
          <SearchInput value={searchValue} onChange={(e) => setSearchValue(e.target.value)} />
        </Box>
        <Box pb="lg">
          <WorkspacesCreateControl createWorkspace={createWorkspace} />
        </Box>
        <WorkspacesList workspaces={workspaces} fetchNextPage={fetchNextPage} hasNextPage={hasNextPage} />
        {isFetchingNextPage && (
          <Box py="2xl" className={styles.loadingSpinner}>
            <LoadingSpinner />
          </Box>
        )}
      </Box>
    </>
  );
};

export default WorkspacesPage;
