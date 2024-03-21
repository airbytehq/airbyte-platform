import React, { useDeferredValue, useMemo } from "react";
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
import { PageHeader } from "components/ui/PageHeader";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useConnectionList, useCurrentWorkspace, useDestinationList, useFilters } from "core/api";
import { DestinationRead } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";

import styles from "./AllDestinationsPage.module.scss";
import { DestinationPaths } from "../../routePaths";

const AllDestinationsPageInner: React.FC<{ destinations: DestinationRead[] }> = ({ destinations }) => {
  const navigate = useNavigate();
  useTrackPage(PageTrackingCodes.DESTINATION_LIST);

  const onCreateDestination = () => navigate(`${DestinationPaths.SelectDestinationNew}`);
  const { workspaceId } = useCurrentWorkspace();
  const canCreateDestination = useIntent("CreateDestination", { workspaceId });

  const connectionList = useConnectionList({ destinationId: destinations.map(({ destinationId }) => destinationId) });
  const connections = connectionList?.connections ?? [];
  const data = getEntityTableData(destinations, connections, "destination");

  const [{ search }, setFilterValue] = useFilters<{ search: string }>({ search: "" });
  const debouncedSearchFilter = useDeferredValue(search);

  const filteredDestinations = useMemo(
    () => filterBySearchEntityTableData(debouncedSearchFilter, data),
    [data, debouncedSearchFilter]
  );

  return destinations.length ? (
    <MainPageWithScroll
      softScrollEdge={false}
      headTitle={<HeadTitle titles={[{ id: "admin.destinations" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="sidebar.destinations" />
            </Heading>
          }
          endComponent={
            <Button
              disabled={!canCreateDestination}
              icon="plus"
              onClick={onCreateDestination}
              size="sm"
              data-id="new-destination"
            >
              <FormattedMessage id="destinations.newDestination" />
            </Button>
          }
        />
      }
    >
      <Card noPadding className={styles.card}>
        <Box p="lg">
          <SearchInput value={search} onChange={({ target: { value } }) => setFilterValue("search", value)} />
        </Box>
        <ImplementationTable data={debouncedSearchFilter ? filteredDestinations : data} entity="destination" />
        {filteredDestinations.length === 0 && (
          <Box pt="xl" pb="lg">
            <Text bold color="grey" align="center">
              <FormattedMessage id="tables.destinations.filters.empty" />
            </Text>
          </Box>
        )}
      </Card>
    </MainPageWithScroll>
  ) : (
    <Navigate to={DestinationPaths.SelectDestinationNew} />
  );
};

export const AllDestinationsPage: React.FC = () => {
  const { destinations } = useDestinationList();
  return destinations.length ? (
    <AllDestinationsPageInner destinations={destinations} />
  ) : (
    <Navigate to={DestinationPaths.SelectDestinationNew} />
  );
};
