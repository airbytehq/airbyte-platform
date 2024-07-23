import { useDeferredValue, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Text } from "components/ui/Text";

import { useConnectionList, useFilters } from "core/api";

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
  const connectionList = useConnectionList();
  const connections = useMemo(() => connectionList?.connections ?? [], [connectionList?.connections]);

  const [filterValues, setFilterValue, resetFilters] = useFilters<FilterValues>({
    search: "",
    state: null,
    status: null,
    source: null,
    destination: null,
  });
  const debouncedSearchFilter = useDeferredValue(filterValues.search);

  const filteredConnections = useMemo(() => {
    const statusFilter = filterValues.status;
    const stateFilter = filterValues.state;
    const sourceFilter = filterValues.source;
    const destinationFilter = filterValues.destination;

    return connections.filter((connection) => {
      if (stateFilter) {
        const isEnabled = isConnectionEnabled(connection);

        if (stateFilter === "enabled" && !isEnabled) {
          return false;
        } else if (stateFilter === "disabled" && isEnabled) {
          return false;
        }
      }

      if (statusFilter) {
        const isPaused = isConnectionPaused(connection);
        const isRunning = isConnectionRunning(connection);
        const isFailed = isConnectionFailed(connection);
        if (statusFilter === "paused" && !isPaused) {
          return false;
        } else if (statusFilter === "running" && (!isRunning || isPaused)) {
          return false;
        } else if (statusFilter === "failed" && (!isFailed || isRunning || isPaused)) {
          return false;
        } else if (statusFilter === "healthy" && (isRunning || isPaused || isFailed)) {
          return false;
        }
      }

      if (sourceFilter && sourceFilter !== connection.source.sourceDefinitionId) {
        return false;
      }

      if (destinationFilter && destinationFilter !== connection.destination.destinationDefinitionId) {
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
  }, [connections, debouncedSearchFilter, filterValues]);

  return (
    <Card noPadding className={styles.connections}>
      <ConnectionsFilters
        connections={connections}
        searchFilter={filterValues.search}
        setSearchFilter={(search) => setFilterValue("search", search)}
        filterValues={filterValues}
        setFilterValue={setFilterValue}
        resetFilters={resetFilters}
      />
      <ConnectionsTable connections={filteredConnections} variant="white" />
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
