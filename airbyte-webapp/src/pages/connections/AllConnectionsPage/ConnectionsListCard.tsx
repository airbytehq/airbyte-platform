import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { ConnectionsGraph, LookbackControl } from "area/connection/components/ConnectionsGraph";
import { LookbackWindow } from "area/connection/components/ConnectionsGraph/lookbackConfiguration";
import { WebBackendConnectionListItem, WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";

import { ConnectionsFilters, FilterValues } from "./ConnectionsFilters";
import styles from "./ConnectionsListCard.module.scss";
import ConnectionsTable from "./ConnectionsTable";

interface ConnectionsListCardProps {
  connections: WebBackendConnectionListItem[];
  fetchNextPage: () => void;
  hasNextPage: boolean;
  filterValues: FilterValues;
  setFilterValue: (key: keyof FilterValues, value: string | null) => void;
  resetFilters: () => void;
  tagFilters: string[];
  setTagFilters: (tagFilters: string[]) => void;
  sortKey: WebBackendConnectionListSortKey;
  setSortKey: (sortState: WebBackendConnectionListSortKey) => void;
  isLoading?: boolean;
}

export const ConnectionsListCard: React.FC<ConnectionsListCardProps> = ({
  connections,
  fetchNextPage,
  hasNextPage,
  filterValues,
  setFilterValue,
  resetFilters,
  tagFilters,
  setTagFilters,
  sortKey,
  setSortKey,
  isLoading,
}) => {
  const [graphLookback, setGraphLookback] = useState<LookbackWindow>("7d");

  return (
    <Card noPadding className={styles.connections}>
      <div className={styles.filters}>
        <ConnectionsFilters
          searchFilter={filterValues.search}
          setSearchFilter={(search) => setFilterValue("search", search)}
          filterValues={filterValues}
          setFilterValue={setFilterValue}
          resetFilters={resetFilters}
          tagFilters={tagFilters}
          setTagFilters={setTagFilters}
        />
      </div>
      <Box px="lg">
        <FlexContainer justifyContent="flex-end">
          <Box pb="md">
            <LookbackControl selected={graphLookback} setSelected={setGraphLookback} />
          </Box>
        </FlexContainer>
        <ConnectionsGraph lookback={graphLookback} />
      </Box>
      <div className={styles.table}>
        <ConnectionsTable
          connections={connections}
          variant="white"
          sortKey={sortKey}
          setSortKey={setSortKey}
          hasNextPage={hasNextPage}
          fetchNextPage={fetchNextPage}
        />
        {hasNextPage && !isLoading && (
          <Box p="xl">
            <FlexContainer justifyContent="center" alignItems="center">
              <LoadingSpinner />
              <Text>
                <FormattedMessage id="tables.connections.loadingMore" />
              </Text>
            </FlexContainer>
          </Box>
        )}
      </div>
      {connections.length === 0 && !isLoading && (
        <Box pt="xl" pb="lg">
          <Text bold color="grey" align="center">
            <FormattedMessage id="tables.connections.filters.empty" />
          </Text>
        </Box>
      )}
      {isLoading && (
        <Box p="xl">
          <FlexContainer justifyContent="center" alignItems="center">
            <LoadingSpinner />
            <Text>
              <FormattedMessage id="tables.connections.loading" />
            </Text>
          </FlexContainer>
        </Box>
      )}
    </Card>
  );
};
