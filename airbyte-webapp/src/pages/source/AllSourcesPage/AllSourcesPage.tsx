import React, { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { statusFilterOptions } from "components/EntityTable/utils";
import { HeadTitle } from "components/HeadTitle";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ListBox } from "components/ui/ListBox";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { PageGridContainer } from "components/ui/PageGridContainer";
import { PageHeader } from "components/ui/PageHeader";
import { ScrollParent } from "components/ui/ScrollParent";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { ActorTable } from "area/connector/components/ActorTable";
import { SourceLimitReachedModal } from "area/workspace/components/SourceLimitReachedModal";
import { useCurrentWorkspaceLimits } from "area/workspace/utils/useCurrentWorkspaceLimits";
import { useFilters, useSourceList } from "core/api";
import { ActorListSortKey, ActorStatus, SourceRead, SourceReadList } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";

import styles from "./AllSourcesPage.module.scss";
import { SourcePaths } from "../../routePaths";

export const AllSourcesPage: React.FC = () => {
  // Rough estimate of page size, assuming ~45px height per row
  const [pageSize] = useState(() => Math.ceil(window.innerHeight / 45));
  const navigate = useNavigate();
  useTrackPage(PageTrackingCodes.SOURCE_LIST);
  const canCreateSource = useGeneratedIntent(Intent.CreateOrEditConnector);
  const { limits, sourceLimitReached } = useCurrentWorkspaceLimits();
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();

  const [{ search, status }, setFilterValue] = useFilters<{ search: string; status: ActorStatus | null }>({
    search: "",
    status: null,
  });

  const [sortKey, setSortKey] = React.useState<ActorListSortKey>("actorName_asc");
  const query = useSourceList({
    pageSize,
    filters: { searchTerm: search, states: status ? [status] : undefined },
    sortKey,
  });

  const infiniteSources = useMemo<SourceReadList>(
    () => ({
      sources: query.data?.pages?.flatMap<SourceRead>((page) => page.sources) ?? [],
    }),
    [query.data]
  );

  const onCreateSource = () => {
    if (sourceLimitReached && limits) {
      openModal({
        title: formatMessage({ id: "workspaces.sourceLimitReached.title" }),
        content: () => <SourceLimitReachedModal sourceCount={limits.sources.current} />,
      });
    } else {
      navigate(`${SourcePaths.SelectSourceNew}`);
    }
  };

  const anyFiltersActive: boolean = search.length > 0 || status !== null;

  if (!query.isLoading && !anyFiltersActive && infiniteSources.sources.length === 0) {
    return <Navigate to={SourcePaths.SelectSourceNew} />;
  }

  return (
    <>
      <HeadTitle titles={[{ id: "admin.sources" }]} />
      <PageGridContainer>
        <PageHeader
          className={styles.pageHeader}
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="sidebar.sources" />
            </Heading>
          }
          endComponent={
            <Button disabled={!canCreateSource} icon="plus" onClick={onCreateSource} size="sm" data-id="new-source">
              <FormattedMessage id="sources.newSource" />
            </Button>
          }
        />
        <ScrollParent props={{ className: styles.pageBody }}>
          <Card noPadding className={styles.card}>
            <div className={styles.filters}>
              <Box p="lg">
                <FlexContainer justifyContent="flex-start" direction="column">
                  <FlexItem grow>
                    <SearchInput
                      value={search}
                      onChange={(value) => setFilterValue("search", value)}
                      debounceTimeout={300}
                    />
                  </FlexItem>
                  <FlexContainer gap="sm" alignItems="center">
                    <FlexItem>
                      <ListBox
                        optionTextAs="span"
                        options={statusFilterOptions}
                        selectedValue={status}
                        onSelect={(value) => setFilterValue("status", value)}
                      />
                    </FlexItem>
                  </FlexContainer>
                </FlexContainer>
              </Box>
            </div>
            <div className={styles.table}>
              <ActorTable
                actorReadList={infiniteSources}
                hasNextPage={!!query.hasNextPage}
                fetchNextPage={() => !query.isFetchingNextPage && query.fetchNextPage()}
                sortKey={sortKey}
                setSortKey={setSortKey}
              />
              {query.isLoading && (
                <Box p="xl">
                  <FlexContainer justifyContent="center" alignItems="center">
                    <LoadingSpinner />
                    <Text>
                      <FormattedMessage id="tables.sources.loading" />
                    </Text>
                  </FlexContainer>
                </Box>
              )}
              {anyFiltersActive && !query.isLoading && infiniteSources.sources.length === 0 && (
                <Box p="xl">
                  <FlexContainer justifyContent="center" alignItems="center">
                    <Text color="grey">
                      <FormattedMessage id="tables.sources.noMatchingSources" />
                    </Text>
                  </FlexContainer>
                </Box>
              )}
              {query.isFetchingNextPage && (
                <Box p="xl">
                  <FlexContainer justifyContent="center" alignItems="center">
                    <LoadingSpinner />
                    <Text>
                      <FormattedMessage id="tables.sources.loadingMore" />
                    </Text>
                  </FlexContainer>
                </Box>
              )}
            </div>
          </Card>
        </ScrollParent>
      </PageGridContainer>
    </>
  );
};
