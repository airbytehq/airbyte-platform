import { useDeferredValue, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionsGraph, LookbackControl } from "area/connection/components/ConnectionsGraph";
import { LookbackWindow } from "area/connection/components/ConnectionsGraph/lookbackConfiguration";
import { useConnectionList, useFilters } from "core/api";
import { useExperiment } from "hooks/services/Experiment";

import { ConnectionsFilters, FilterValues } from "./ConnectionsFilters";
import styles from "./ConnectionsListCard.module.scss";
import {
  isConnectionFailed,
  isConnectionEnabled,
  isConnectionPaused,
  isConnectionRunning,
} from "./ConnectionsSummary/ConnectionsSummary";
import ConnectionsTable from "./ConnectionsTable";

export const ConnectionsListCard = () => {
  const [graphLookback, setGraphLookback] = useState<LookbackWindow>("7d");
  const showConnectionsGraph = useExperiment("connection.connectionsGraph");
  const connectionList = useConnectionList();
  const connections = useMemo(() => connectionList?.connections ?? [], [connectionList?.connections]);
  const [tagFilters, setTagFilters] = useState<string[]>([]);
  const tagFilterSet = useMemo(() => new Set(tagFilters), [tagFilters]);

  const [filterValues, setFilterValue, resetFilters] = useFilters<FilterValues>({
    search: "",
    state: null,
    status: null,
    source: null,
    destination: null,
  });
  const debouncedSearchFilter = useDeferredValue(filterValues.search);

  const filteredConnections = useMemo(() => {
    return connections.filter((connection) => {
      if (filterValues.state) {
        const isEnabled = isConnectionEnabled(connection);

        if (filterValues.state === "enabled" && !isEnabled) {
          return false;
        } else if (filterValues.state === "disabled" && isEnabled) {
          return false;
        }
      }

      if (filterValues.status) {
        const isPaused = isConnectionPaused(connection);
        const isRunning = isConnectionRunning(connection);
        const isFailed = isConnectionFailed(connection);
        if (filterValues.status === "paused" && !isPaused) {
          return false;
        } else if (filterValues.status === "running" && (!isRunning || isPaused)) {
          return false;
        } else if (filterValues.status === "failed" && (!isFailed || isRunning || isPaused)) {
          return false;
        } else if (filterValues.status === "healthy" && (isRunning || isPaused || isFailed)) {
          return false;
        }
      }

      if (filterValues.source && filterValues.source !== connection.source.sourceDefinitionId) {
        return false;
      }

      if (filterValues.destination && filterValues.destination !== connection.destination.destinationDefinitionId) {
        return false;
      }

      if (debouncedSearchFilter) {
        const searchValue = debouncedSearchFilter.toLowerCase();

        const sourceName = connection.source.sourceName.toLowerCase();
        const destinationName = connection.destination.destinationName.toLowerCase();
        const connectionName = connection.name.toLowerCase();
        const sourceDefinitionName = connection.source.name.toLowerCase();
        const destinationDefinitionName = connection.destination.name.toLowerCase();
        if (
          !sourceName.includes(searchValue) &&
          !destinationName.includes(searchValue) &&
          !connectionName.includes(searchValue) &&
          !sourceDefinitionName.includes(searchValue) &&
          !destinationDefinitionName.includes(searchValue)
        ) {
          return false;
        }
      }

      return true;
    });
  }, [
    connections,
    debouncedSearchFilter,
    filterValues.status,
    filterValues.state,
    filterValues.source,
    filterValues.destination,
  ]);

  const filteredConnectionsByTags = useMemo(() => {
    if (tagFilterSet.size === 0) {
      return filteredConnections;
    }

    return filteredConnections.filter((connection) => {
      return connection.tags.some((tag) => tagFilterSet.has(tag.tagId));
    });
  }, [filteredConnections, tagFilterSet]);

  return (
    <Card noPadding className={styles.connections}>
      <div className={styles.filters}>
        <ConnectionsFilters
          connections={connections}
          searchFilter={filterValues.search}
          setSearchFilter={(search) => setFilterValue("search", search)}
          filterValues={filterValues}
          setFilterValue={setFilterValue}
          resetFilters={() => {
            resetFilters();
            setTagFilters([]);
          }}
          tagFilters={tagFilters}
          setTagFilters={setTagFilters}
        />
      </div>
      {showConnectionsGraph && (
        <Box px="lg">
          <FlexContainer justifyContent="flex-end">
            <Box pb="md">
              <LookbackControl selected={graphLookback} setSelected={setGraphLookback} />
            </Box>
          </FlexContainer>
          <ConnectionsGraph lookback={graphLookback} connections={filteredConnectionsByTags} />
        </Box>
      )}
      <div className={styles.table}>
        <ConnectionsTable connections={filteredConnectionsByTags} variant="white" />
      </div>
      {filteredConnections.length === 0 && (
        <Box pt="xl" pb="lg">
          <Text bold color="grey" align="center">
            <FormattedMessage id="tables.connections.filters.empty" />
          </Text>
        </Box>
      )}
    </Card>
  );
};
