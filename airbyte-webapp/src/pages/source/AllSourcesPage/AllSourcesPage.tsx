import React, { useDeferredValue, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { ImplementationTable } from "components/EntityTable";
import { filterBySearchEntityTableData, getEntityTableData, statusFilterOptions } from "components/EntityTable/utils";
import { HeadTitle } from "components/HeadTitle";
import { MainPageWithScroll } from "components/MainPageWithScroll";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ListBox } from "components/ui/ListBox";
import { PageHeader } from "components/ui/PageHeader";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useConnectionList, useCurrentWorkspace, useFilters, useSourceList } from "core/api";
import { SourceRead } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";

import styles from "./AllSourcesPage.module.scss";
import { SourcePaths } from "../../routePaths";

const AllSourcesPageInner: React.FC<{ sources: SourceRead[] }> = ({ sources }) => {
  const navigate = useNavigate();
  useTrackPage(PageTrackingCodes.SOURCE_LIST);
  const onCreateSource = () => navigate(`${SourcePaths.SelectSourceNew}`);
  const { workspaceId } = useCurrentWorkspace();
  const canCreateSource = useIntent("CreateSource", { workspaceId });
  const connectionList = useConnectionList({ sourceId: sources.map(({ sourceId }) => sourceId) });
  const connections = connectionList?.connections ?? [];
  const data = getEntityTableData(sources, connections, "source");

  const [{ search, status }, setFilterValue] = useFilters<{ search: string; status: string | null }>({
    search: "",
    status: null,
  });
  const debouncedSearchFilter = useDeferredValue(search);

  const filteredSources = useMemo(
    () => filterBySearchEntityTableData(debouncedSearchFilter, status, data),
    [data, debouncedSearchFilter, status]
  );

  return sources.length ? (
    <MainPageWithScroll
      softScrollEdge={false}
      headTitle={<HeadTitle titles={[{ id: "admin.sources" }]} />}
      pageTitle={
        <PageHeader
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
      }
    >
      <Card noPadding className={styles.card}>
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
        <ImplementationTable data={filteredSources} entity="source" />
        {filteredSources.length === 0 && (
          <Box pt="xl" pb="lg">
            <Text bold color="grey" align="center">
              <FormattedMessage id="tables.sources.filters.empty" />
            </Text>
          </Box>
        )}
      </Card>
    </MainPageWithScroll>
  ) : (
    <Navigate to={SourcePaths.SelectSourceNew} />
  );
};

const AllSourcesPage: React.FC = () => {
  const { sources } = useSourceList();
  return sources.length ? <AllSourcesPageInner sources={sources} /> : <Navigate to={SourcePaths.SelectSourceNew} />;
};

export default AllSourcesPage;
