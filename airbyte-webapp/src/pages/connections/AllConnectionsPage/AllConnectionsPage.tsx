import React, { Suspense, useDeferredValue, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { LoadingPage, MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectionOnboarding } from "components/connection/ConnectionOnboarding";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";
import { Text } from "components/ui/Text";

import { useConnectionList, useCurrentWorkspace, useFilters } from "core/api";
import { JobStatus, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";

import styles from "./AllConnectionsPage.module.scss";
import { ConnectionsFilters, FilterValues } from "./ConnectionsFilters";
import { ConnectionsSummary, SummaryKey } from "./ConnectionsSummary";
import ConnectionsTable from "./ConnectionsTable";
import { ConnectionRoutePaths } from "../../routePaths";

const isConnectionPaused = (
  connection: WebBackendConnectionListItem
): connection is WebBackendConnectionListItem & { status: "inactive" } => connection.status === "inactive";

const isConnectionRunning = (
  connection: WebBackendConnectionListItem
): connection is WebBackendConnectionListItem & { isSyncing: true } => connection.isSyncing;

const isConnectionFailed = (
  connection: WebBackendConnectionListItem
): connection is WebBackendConnectionListItem & { latestSyncJobStatus: "failed" } =>
  connection.latestSyncJobStatus === JobStatus.failed ||
  connection.latestSyncJobStatus === JobStatus.cancelled ||
  connection.latestSyncJobStatus === JobStatus.incomplete;

export const AllConnectionsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_LIST);
  const navigate = useNavigate();

  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useIntent("CreateConnection", { workspaceId });

  const connectionList = useConnectionList();
  const connections = useMemo(() => connectionList?.connections ?? [], [connectionList?.connections]);

  const [filterValues, setFilterValue, setFilters] = useFilters<FilterValues>({
    search: "",
    status: null,
    source: null,
    destination: null,
  });
  const debouncedSearchFilter = useDeferredValue(filterValues.search);

  const filteredConnections = useMemo(() => {
    const statusFilter = filterValues.status;
    const sourceFilter = filterValues.source;
    const destinationFilter = filterValues.destination;

    return connections.filter((connection) => {
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

  const connectionsSummary = connections.reduce<Record<SummaryKey, number>>(
    (acc, connection) => {
      let status: SummaryKey;

      if (isConnectionPaused(connection)) {
        status = "paused";
      } else if (isConnectionRunning(connection)) {
        status = "running";
      } else if (isConnectionFailed(connection)) {
        status = "failed";
      } else {
        status = "healthy";
      }

      acc[status] += 1;
      return acc;
    },
    {
      // order here governs render order
      running: 0,
      healthy: 0,
      failed: 0,
      paused: 0,
    }
  );

  const onCreateClick = (sourceDefinitionId?: string) =>
    navigate(`${ConnectionRoutePaths.ConnectionNew}`, { state: { sourceDefinitionId } });

  return (
    <Suspense fallback={<LoadingPage />}>
      <>
        <HeadTitle titles={[{ id: "sidebar.connections" }]} />
        {connections.length ? (
          <MainPageWithScroll
            softScrollEdge={false}
            pageTitle={
              <PageHeader
                leftComponent={
                  <FlexContainer direction="column">
                    <FlexItem>
                      <Heading as="h1" size="lg">
                        <FormattedMessage id="sidebar.connections" />
                      </Heading>
                    </FlexItem>
                    <FlexItem>
                      <ConnectionsSummary {...connectionsSummary} />
                    </FlexItem>
                  </FlexContainer>
                }
                endComponent={
                  <FlexItem className={styles.alignSelfStart}>
                    <Button
                      disabled={!canCreateConnection}
                      icon="plus"
                      variant="primary"
                      size="sm"
                      onClick={() => onCreateClick()}
                      data-testid="new-connection-button"
                    >
                      <FormattedMessage id="connection.newConnection" />
                    </Button>
                  </FlexItem>
                }
              />
            }
          >
            <Card noPadding className={styles.connections}>
              <ConnectionsFilters
                connections={connections}
                searchFilter={filterValues.search}
                setSearchFilter={(search) => setFilterValue("search", search)}
                filterValues={filterValues}
                setFilterValue={setFilterValue}
                setFilters={setFilters}
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
          </MainPageWithScroll>
        ) : (
          <ConnectionOnboarding onCreate={onCreateClick} />
        )}
      </>
    </Suspense>
  );
};
