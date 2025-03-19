import React, { useDeferredValue, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { ImplementationTable } from "components/EntityTable";
import { filterBySearchEntityTableData, getEntityTableData, statusFilterOptions } from "components/EntityTable/utils";
import { HeadTitle } from "components/HeadTitle";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ListBox } from "components/ui/ListBox";
import { PageGridContainer } from "components/ui/PageGridContainer";
import { PageHeader } from "components/ui/PageHeader";
import { ScrollParent } from "components/ui/ScrollParent";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { SourceLimitReachedModal } from "area/workspace/components/SourceLimitReachedModal";
import { useCurrentWorkspaceLimits } from "area/workspace/utils/useCurrentWorkspaceLimits";
import { useConnectionList, useFilters, useSourceList } from "core/api";
import { SourceRead } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";

import styles from "./AllSourcesPage.module.scss";
import { SourcePaths } from "../../routePaths";

const AllSourcesPageInner: React.FC<{ sources: SourceRead[] }> = ({ sources }) => {
  const navigate = useNavigate();
  useTrackPage(PageTrackingCodes.SOURCE_LIST);
  const canCreateSource = useGeneratedIntent(Intent.CreateOrEditConnector);
  const connectionList = useConnectionList({ sourceId: sources.map(({ sourceId }) => sourceId) });
  const connections = connectionList?.connections ?? [];
  const data = getEntityTableData(sources, connections, "source");
  const { limits, sourceLimitReached } = useCurrentWorkspaceLimits();
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();

  const [{ search, status }, setFilterValue] = useFilters<{ search: string; status: string | null }>({
    search: "",
    status: null,
  });
  const debouncedSearchFilter = useDeferredValue(search);

  const filteredSources = useMemo(
    () => filterBySearchEntityTableData(debouncedSearchFilter, status, data),
    [data, debouncedSearchFilter, status]
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

  return sources.length ? (
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
                    <SearchInput value={search} onChange={({ target: { value } }) => setFilterValue("search", value)} />
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
              <ImplementationTable
                data={filteredSources}
                entity="source"
                emptyPlaceholder={
                  <Text bold color="grey" align="center">
                    <FormattedMessage id="tables.sources.filters.empty" />
                  </Text>
                }
              />
            </div>
          </Card>
        </ScrollParent>
      </PageGridContainer>
    </>
  ) : (
    <Navigate to={SourcePaths.SelectSourceNew} />
  );
};

const AllSourcesPage: React.FC = () => {
  const { sources } = useSourceList();
  return sources.length ? <AllSourcesPageInner sources={sources} /> : <Navigate to={SourcePaths.SelectSourceNew} />;
};

export default AllSourcesPage;
