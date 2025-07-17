import { useEffect, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { createSearchParams } from "react-router-dom";

import { LoadingPage } from "components";
import { ConnectorEmptyStateContent } from "components/connector/ConnectorEmptyStateContent";
import { TableItemTitle } from "components/ConnectorBlocks";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex/FlexContainer";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { ScrollParent } from "components/ui/ScrollParent";
import { Text } from "components/ui/Text";

import { useGetSourceFromParams } from "area/connector/utils";
import { useCurrentWorkspace, useConnectionList } from "core/api";
import { WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./SourceConnectionsPage.module.scss";
import SourceConnectionTable from "./SourceConnectionTable";

export const SourceConnectionsPage = () => {
  const [connectionCount, setConnectionCount] = useState<null | number>(null);
  const [sortKey, setSortKey] = useState<WebBackendConnectionListSortKey>("connectionName_asc");
  const { workspaceId } = useCurrentWorkspace();

  const source = useGetSourceFromParams();

  const connectionQuery = useConnectionList({
    sourceId: [source.sourceId],
    sortKey,
  });

  // Treating this as a side effect so we can keep the number of connections even as the sorting key changes and
  // invalidates the query data. If we ever introduced searching/filtering on this page, we would need to change this
  // logic, because the number of connections would change based on the search/filter.
  useEffect(() => {
    const countFromFinalPage = connectionQuery.data?.pages.at(-1)?.num_connections;
    if (countFromFinalPage !== undefined) {
      setConnectionCount(countFromFinalPage);
    }
  }, [connectionQuery.data]);

  const infiniteConnections = useMemo(() => {
    return connectionQuery.data?.pages.flatMap((page) => page.connections) ?? [];
  }, [connectionQuery.data?.pages]);

  const createConnectionLink = useMemo(() => {
    const searchParams = createSearchParams({ sourceId: source.sourceId });
    return `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}?${searchParams}`;
  }, [source.sourceId, workspaceId]);

  if (connectionCount === null) {
    return <LoadingPage />;
  }

  if (connectionCount === null) {
    return <LoadingPage />;
  }

  return (
    <>
      {connectionCount > 0 ? (
        <FlexContainer direction="column" gap="xl" className={styles.fullHeight}>
          <TableItemTitle createConnectionLink={createConnectionLink} connectionsCount={connectionCount} />
          <ScrollParent props={{ className: styles.scrollContainer }}>
            <SourceConnectionTable
              connections={infiniteConnections}
              hasNextPage={!!connectionQuery.hasNextPage}
              fetchNextPage={() => !connectionQuery.isFetchingNextPage && connectionQuery.fetchNextPage()}
              setSortKey={setSortKey}
              sortKey={sortKey}
            />
            {(connectionQuery.isLoading || connectionQuery.isFetchingNextPage) && (
              <Box p="xl">
                <FlexContainer justifyContent="center" alignItems="center">
                  <LoadingSpinner />
                  <Text>
                    <FormattedMessage id="tables.connections.loading" />
                  </Text>
                </FlexContainer>
              </Box>
            )}
          </ScrollParent>
        </FlexContainer>
      ) : (
        <ConnectorEmptyStateContent
          connectorId={source.sourceId}
          icon={source.icon}
          connectorType="source"
          connectorName={source.name}
        />
      )}
    </>
  );
};
