import React, { Suspense, useMemo, useState } from "react";
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
import { Text } from "components/ui/Text";

import { useConnectionList } from "core/api";
import { JobStatus, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { naturalComparatorBy } from "core/utils/objects";
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

  const connectionList = useConnectionList();
  const connections = useMemo(() => connectionList?.connections ?? [], [connectionList?.connections]);

  const availableSourceOptions = getAvailableSourceOptions(connections);
  const availableDestinationOptions = getAvailableDestinationOptions(connections);

  const [statusFilterSelection, setStatusFilterSelection] = useState<FilterOption>(statusFilterOptions[0]);
  const [sourceFilterSelection, setSourceFilterSelection] = useState<SortableFilterOption>(availableSourceOptions[0]);
  const [destinationFilterSelection, setDestinationFilterSelection] = useState<SortableFilterOption>(
    availableDestinationOptions[0]
  );
  const hasAnyFilterSelected = [statusFilterSelection, sourceFilterSelection, destinationFilterSelection].some(
    (selection) => !!selection.value
  );

  const filteredConnections = useMemo(() => {
    const statusFilter = statusFilterSelection?.value;
    const sourceFilter = sourceFilterSelection?.value;
    const destinationFilter = destinationFilterSelection?.value;

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

      return true;
    });
  }, [connections, statusFilterSelection, sourceFilterSelection, destinationFilterSelection]);

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
            <Card>
              {isConnectionsSummaryEnabled && (
                <Box pt="lg" pb="lg" pl="lg">
                  <FlexContainer justifyContent="flex-start">
                    <FlexItem>
                      <ListBox
                        buttonClassName={styles.filterButton}
                        optionClassName={styles.filterOption}
                        optionTextAs="span"
                        options={statusFilterOptions}
                        selectedValue={statusFilterSelection.value}
                        onSelect={(value) =>
                          setStatusFilterSelection(statusFilterOptions.find((option) => option.value === value)!)
                        }
                      />
                    </FlexItem>
                    <FlexItem>
                      <ListBox
                        buttonClassName={styles.filterButton}
                        optionsMenuClassName={styles.filterOptionsMenu}
                        optionClassName={styles.filterOption}
                        optionTextAs="span"
                        options={availableSourceOptions}
                        selectedValue={sourceFilterSelection.value}
                        onSelect={(value) =>
                          setSourceFilterSelection(availableSourceOptions.find((option) => option.value === value)!)
                        }
                      />
                    </FlexItem>
                    <FlexItem>
                      <ListBox
                        buttonClassName={styles.filterButton}
                        optionClassName={styles.filterOption}
                        optionTextAs="span"
                        options={availableDestinationOptions}
                        selectedValue={destinationFilterSelection.value}
                        onSelect={(value) =>
                          setDestinationFilterSelection(
                            availableDestinationOptions.find((option) => option.value === value)!
                          )
                        }
                      />
                    </FlexItem>
                    {hasAnyFilterSelected && (
                      <FlexItem>
                        <ClearFiltersButton
                          onClick={() => {
                            setStatusFilterSelection(statusFilterOptions[0]);
                            setSourceFilterSelection(availableSourceOptions[0]);
                            setDestinationFilterSelection(availableDestinationOptions[0]);
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
