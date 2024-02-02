import React, { Suspense, useDeferredValue, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { LoadingPage, MainPageWithScroll } from "components";
import { ConnectorIcon } from "components/common/ConnectorIcon";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectionOnboarding } from "components/connection/ConnectionOnboarding";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ListBox } from "components/ui/ListBox";
import { PageHeader } from "components/ui/PageHeader";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useConnectionList, useCurrentWorkspace, useFilters } from "core/api";
import { JobStatus, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { naturalComparatorBy } from "core/utils/objects";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./AllConnectionsPage.module.scss";
import ConnectionsTable from "./ConnectionsTable";
import { ConnectionRoutePaths } from "../../routePaths";

type SummaryKey = "healthy" | "failed" | "paused" | "running";
const connectionStatColors: Record<SummaryKey, React.ComponentPropsWithoutRef<typeof Text>["color"]> = {
  healthy: "green600",
  failed: "red",
  paused: "grey",
  running: "blue",
};
const ConnectionsSummary: React.FC<Record<SummaryKey, number>> = (props) => {
  const keys = Object.keys(props) as SummaryKey[];
  const parts: React.ReactNode[] = [];
  const connectionsCount = keys.reduce((total, value) => total + props[value], 0);
  let consumedConnections = 0;

  for (const key of keys) {
    const value = props[key];
    if (value) {
      consumedConnections += value;
      parts.push(
        <Text key={key} as="span" size="lg" color={connectionStatColors[key]} className={styles.lowercase}>
          {value} <FormattedMessage id={`tables.connections.filters.status.${key}`} />
        </Text>,
        consumedConnections < connectionsCount && (
          <Text key={`${key}-middot`} as="span" size="lg" bold color="grey">
            &nbsp;&middot;&nbsp;
          </Text>
        )
      );
    }
  }

  return <>{parts}</>;
};

interface FilterOption {
  label: React.ReactNode;
  value: string | null;
}

type SortableFilterOption = FilterOption & { sortValue: string };

const statusFilterOptions: FilterOption[] = [
  {
    label: (
      <Text color="grey" bold>
        <FormattedMessage id="tables.connections.filters.status.all" />
      </Text>
    ),
    value: null,
  },
  {
    label: (
      <FlexContainer gap="sm" alignItems="center">
        <FlexItem>
          <Text color={connectionStatColors.healthy} as="span">
            <Icon type="successFilled" size="md" />
          </Text>
        </FlexItem>
        <FlexItem>
          <Text color="grey" bold as="span">
            &nbsp; <FormattedMessage id="tables.connections.filters.status.healthy" />
          </Text>
        </FlexItem>
      </FlexContainer>
    ),
    value: "healthy",
  },
  {
    label: (
      <FlexContainer gap="sm" alignItems="center">
        <FlexItem>
          <Text color={connectionStatColors.failed} as="span">
            <Icon type="errorFilled" size="md" />
          </Text>
        </FlexItem>
        <FlexItem>
          <Text color="grey" bold as="span">
            &nbsp; <FormattedMessage id="tables.connections.filters.status.failed" />
          </Text>
        </FlexItem>
      </FlexContainer>
    ),
    value: "failed",
  },
  {
    label: (
      <FlexContainer gap="sm" alignItems="center">
        <FlexItem>
          <Text color={connectionStatColors.running} as="span">
            <Icon type="sync" size="md" />
          </Text>
        </FlexItem>
        <FlexItem>
          <Text color="grey" bold as="span">
            &nbsp; <FormattedMessage id="tables.connections.filters.status.running" />
          </Text>
        </FlexItem>
      </FlexContainer>
    ),
    value: "running",
  },
  {
    label: (
      <FlexContainer gap="sm" alignItems="center">
        <FlexItem>
          <Text color={connectionStatColors.paused} as="span">
            <Icon type="pauseFilled" size="md" />
          </Text>
        </FlexItem>
        <FlexItem>
          <Text color="grey" bold as="span">
            &nbsp; <FormattedMessage id="tables.connections.filters.status.paused" />
          </Text>
        </FlexItem>
      </FlexContainer>
    ),
    value: "paused",
  },
];

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
  const navigate = useNavigate();

  useTrackPage(PageTrackingCodes.CONNECTIONS_LIST);
  const isConnectionsSummaryEnabled = useExperiment("connections.summaryView", false);

  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useIntent("CreateConnection", { workspaceId });

  const connectionList = useConnectionList();
  const connections = useMemo(() => connectionList?.connections ?? [], [connectionList?.connections]);

  const availableSourceOptions = useMemo(() => getAvailableSourceOptions(connections), [connections]);
  const availableDestinationOptions = useMemo(() => getAvailableDestinationOptions(connections), [connections]);

  const [filterValues, setFilterValue, setFilters] = useFilters({
    status: statusFilterOptions[0].value,
    source: availableSourceOptions[0].value,
    destination: availableDestinationOptions[0].value,
  });

  const [searchFilter, setSearchFilter] = useState<string>("");
  const debouncedSearchFilter = useDeferredValue(searchFilter);
  const hasAnyFilterSelected =
    !!filterValues.status || !!filterValues.source || !!filterValues.destination || debouncedSearchFilter;

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
                    {isConnectionsSummaryEnabled && (
                      <FlexItem>
                        <ConnectionsSummary {...connectionsSummary} />
                      </FlexItem>
                    )}
                  </FlexContainer>
                }
                endComponent={
                  <FlexItem className={styles.alignSelfStart}>
                    <Button
                      disabled={!canCreateConnection}
                      icon={<Icon type="plus" />}
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
            <Card noPadding>
              {isConnectionsSummaryEnabled && (
                <Box pt="lg" pb="lg" pl="lg">
                  <FlexContainer justifyContent="flex-start">
                    <FlexItem>
                      <ListBox
                        buttonClassName={styles.filterButton}
                        optionClassName={styles.filterOption}
                        optionTextAs="span"
                        options={statusFilterOptions}
                        selectedValue={filterValues.status}
                        onSelect={(value) => setFilterValue("status", value)}
                      />
                    </FlexItem>
                    <FlexItem>
                      <ListBox
                        buttonClassName={styles.filterButton}
                        optionsMenuClassName={styles.filterOptionsMenu}
                        optionClassName={styles.filterOption}
                        optionTextAs="span"
                        options={availableSourceOptions}
                        selectedValue={filterValues.source}
                        onSelect={(value) => setFilterValue("source", value)}
                      />
                    </FlexItem>
                    <FlexItem>
                      <ListBox
                        buttonClassName={styles.filterButton}
                        optionClassName={styles.filterOption}
                        optionTextAs="span"
                        options={availableDestinationOptions}
                        selectedValue={filterValues.destination}
                        onSelect={(value) => setFilterValue("destination", value)}
                      />
                    </FlexItem>
                    <FlexItem>
                      <SearchInput value={searchFilter} onChange={(event) => setSearchFilter(event.target.value)} />
                    </FlexItem>
                    {hasAnyFilterSelected && (
                      <FlexItem>
                        <ClearFiltersButton
                          onClick={() => {
                            setFilters({
                              status: null,
                              source: null,
                              destination: null,
                            });
                            setSearchFilter("");
                          }}
                        />
                      </FlexItem>
                    )}
                  </FlexContainer>
                </Box>
              )}
              <ConnectionsTable
                connections={filteredConnections}
                variant={isConnectionsSummaryEnabled ? "white" : "default"}
              />
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

function getAvailableSourceOptions(connections: WebBackendConnectionListItem[]) {
  return connections
    .reduce<{
      foundSourceIds: Set<string>;
      options: SortableFilterOption[];
    }>(
      (acc, connection) => {
        const { sourceName, sourceDefinitionId, icon } = connection.source;
        if (acc.foundSourceIds.has(sourceDefinitionId) === false) {
          acc.foundSourceIds.add(sourceDefinitionId);
          acc.options.push({
            label: (
              <FlexContainer gap="sm" alignItems="center" as="span">
                <FlexItem>
                  <ConnectorIcon icon={icon} />
                </FlexItem>
                <FlexItem>
                  <Text size="sm">{sourceName}</Text>
                </FlexItem>
              </FlexContainer>
            ),
            value: sourceDefinitionId,
            sortValue: sourceName,
          });
        }
        return acc;
      },
      {
        foundSourceIds: new Set(),
        options: [
          {
            label: (
              <Text bold color="grey">
                <FormattedMessage id="tables.connections.filters.source.all" />
              </Text>
            ),
            value: null,
            sortValue: "",
          },
        ],
      }
    )
    .options.sort(naturalComparatorBy((option) => option.sortValue));
}

function getAvailableDestinationOptions(connections: WebBackendConnectionListItem[]) {
  return connections
    .reduce<{
      foundDestinationIds: Set<string>;
      options: SortableFilterOption[];
    }>(
      (acc, connection) => {
        const { destinationName, destinationDefinitionId, icon } = connection.destination;
        if (acc.foundDestinationIds.has(destinationDefinitionId) === false) {
          acc.foundDestinationIds.add(connection.destination.destinationDefinitionId);
          acc.options.push({
            label: (
              <FlexContainer gap="sm" alignItems="center" as="span">
                <FlexItem>
                  <ConnectorIcon icon={icon} />
                </FlexItem>
                <FlexItem>
                  <Text size="sm">{destinationName}</Text>
                </FlexItem>
              </FlexContainer>
            ),
            value: destinationDefinitionId,
            sortValue: destinationName,
          });
        }
        return acc;
      },
      {
        foundDestinationIds: new Set(),
        options: [
          {
            label: (
              <Text bold color="grey">
                <FormattedMessage id="tables.connections.filters.destination.all" />
              </Text>
            ),
            value: null,
            sortValue: "",
          },
        ],
      }
    )
    .options.sort(naturalComparatorBy((option) => option.sortValue));
}
