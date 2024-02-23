import React, { useDeferredValue, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll";
import { ImplementationTable } from "components/EntityTable";
import { filterBySearchEntityTableData, getEntityTableData } from "components/EntityTable/utils";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { PageHeader } from "components/ui/PageHeader";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useConnectionList, useCurrentWorkspace, useSourceList } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";

import styles from "./AllSourcesPage.module.scss";
import { SourcePaths } from "../../routePaths";

const AllSourcesPage: React.FC = () => {
  const navigate = useNavigate();
  useTrackPage(PageTrackingCodes.SOURCE_LIST);
  const onCreateSource = () => navigate(`${SourcePaths.SelectSourceNew}`);
  const { workspaceId } = useCurrentWorkspace();
  const canCreateSource = useIntent("CreateSource", { workspaceId });
  const { sources } = useSourceList();
  const connectionList = useConnectionList({ sourceId: sources.map(({ sourceId }) => sourceId) });
  const connections = connectionList?.connections ?? [];
  const data = getEntityTableData(sources, connections, "source");

  const [searchFilter, setSearchFilter] = useState<string>("");
  const debouncedSearchFilter = useDeferredValue(searchFilter);

  const filteredSources = useMemo(
    () => filterBySearchEntityTableData(debouncedSearchFilter, data),
    [data, debouncedSearchFilter]
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
            <Button
              disabled={!canCreateSource}
              icon={<Icon type="plus" />}
              onClick={onCreateSource}
              size="sm"
              data-id="new-source"
            >
              <FormattedMessage id="sources.newSource" />
            </Button>
          }
        />
      }
    >
      <Card noPadding className={styles.card}>
        <Box p="lg">
          <SearchInput value={searchFilter} onChange={({ target: { value } }) => setSearchFilter(value)} />
        </Box>
        <ImplementationTable data={debouncedSearchFilter ? filteredSources : data} entity="source" />
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

export default AllSourcesPage;
