import React, { Suspense, useLayoutEffect, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { LoadingPage } from "components";
import { HeadTitle } from "components/HeadTitle";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageGridContainer } from "components/ui/PageGridContainer";
import { PageHeader } from "components/ui/PageHeader";
import { ScrollParent } from "components/ui/ScrollParent";

import { ActiveConnectionLimitReachedModal } from "area/workspace/components/ActiveConnectionLimitReachedModal";
import { useCurrentWorkspaceLimits } from "area/workspace/utils/useCurrentWorkspaceLimits";
import { useConnectionList, useCurrentWorkspace, useListConnectionsStatusesAsync, useFilters } from "core/api";
import { WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useDrawerActions } from "core/services/ui/DrawerService";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";
import { RoutePaths, ConnectionRoutePaths } from "pages/routePaths";

import styles from "./AllConnectionsPage.module.scss";
import { FilterValues } from "./ConnectionsFilters/ConnectionsFilters";
import { ConnectionsListCard } from "./ConnectionsListCard";
import { ConnectionsSummary } from "./ConnectionsSummary";

const ConnectionOnboarding = React.lazy(() =>
  import("components/connection/ConnectionOnboarding").then((module) => ({ default: module.ConnectionOnboarding }))
);

export const AllConnectionsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_LIST);
  const navigate = useNavigate();
  const { activeConnectionLimitReached, limits } = useCurrentWorkspaceLimits();
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();
  const { closeDrawer } = useDrawerActions();

  // Filter state management
  const [filterValues, setFilterValue, resetFilters] = useFilters<FilterValues>({
    search: "",
    state: null,
    status: null,
    source: null,
    destination: null,
  });

  // Tag filter state management
  const [tagFilters, setTagFilters] = React.useState<string[]>([]);

  // Sort state management
  const [sortKey, setSortKey] = React.useState<WebBackendConnectionListSortKey>("connectionName_asc");

  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useGeneratedIntent(Intent.CreateOrEditConnection);

  // Pass filters and sort state to useConnectionList for server-side filtering and sorting
  const connectionListQuery = useConnectionList({
    filters: {
      search: filterValues.search,
      status: filterValues.status,
      state: filterValues.state,
      sourceDefinitionIds: filterValues.source ? [filterValues.source] : [],
      destinationDefinitionIds: filterValues.destination ? [filterValues.destination] : [],
      tagIds: tagFilters.length > 0 ? tagFilters : [],
    },
    sortKey,
  });

  const connections = useMemo(
    () => connectionListQuery.data?.pages.flatMap((page) => page.connections) ?? [],
    [connectionListQuery.data?.pages]
  );
  const hasConnections = connections.length > 0;
  const isAllConnectionsStatusEnabled = useExperiment("connections.connectionsStatusesEnabled");

  const onCreateConnection = () => {
    navigate(
      `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`
    );
  };

  useListConnectionsStatusesAsync(
    connections.map((connection) => connection.connectionId),
    isAllConnectionsStatusEnabled
  );

  const onCreateClick = (sourceDefinitionId?: string) => {
    if (activeConnectionLimitReached && limits) {
      openModal({
        title: formatMessage({ id: "workspaces.activeConnectionLimitReached.title" }),
        content: () => <ActiveConnectionLimitReachedModal connectionCount={limits.activeConnections.current} />,
      });
    } else {
      navigate(`${ConnectionRoutePaths.ConnectionNew}`, { state: { sourceDefinitionId } });
    }
  };

  useLayoutEffect(() => {
    return () => closeDrawer();
  }, [closeDrawer]);

  const anyActiveFilters: boolean =
    tagFilters.length > 0 ||
    !!filterValues.search ||
    !!filterValues.status ||
    !!filterValues.state ||
    !!filterValues.source ||
    !!filterValues.destination;

  return (
    <Suspense fallback={<LoadingPage />}>
      <>
        <HeadTitle titles={[{ id: "sidebar.connections" }]} />
        {anyActiveFilters || hasConnections || connectionListQuery.isLoading ? (
          <PageGridContainer>
            <PageHeader
              className={styles.pageHeader}
              leftComponent={
                <FlexContainer direction="column">
                  <FlexItem>
                    <Heading as="h1" size="lg">
                      <FormattedMessage id="sidebar.connections" />
                    </Heading>
                  </FlexItem>
                  <FlexItem>
                    <ConnectionsSummary />
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
            <ScrollParent props={{ className: styles.pageBody }}>
              <ConnectionsListCard
                isLoading={connectionListQuery.isLoading}
                connections={connections}
                fetchNextPage={() => !connectionListQuery.isFetchingNextPage && connectionListQuery.fetchNextPage()}
                hasNextPage={connectionListQuery.hasNextPage ?? false}
                filterValues={filterValues}
                setFilterValue={setFilterValue}
                resetFilters={() => {
                  resetFilters();
                  setTagFilters([]);
                }}
                tagFilters={tagFilters}
                setTagFilters={setTagFilters}
                sortKey={sortKey}
                setSortKey={setSortKey}
              />
            </ScrollParent>
          </PageGridContainer>
        ) : (
          <ConnectionOnboarding onCreate={onCreateConnection} />
        )}
      </>
    </Suspense>
  );
};
