import React, { Suspense, useLayoutEffect, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { LoadingPage } from "components";
import { ConnectionOnboarding } from "components/connection/ConnectionOnboarding";
import { HeadTitle } from "components/HeadTitle";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageGridContainer } from "components/ui/PageGridContainer";
import { PageHeader } from "components/ui/PageHeader";
import { ScrollParent } from "components/ui/ScrollParent";

import { ActiveConnectionLimitReachedModal } from "area/workspace/components/ActiveConnectionLimitReachedModal";
import { useCurrentWorkspaceLimits } from "area/workspace/utils/useCurrentWorkspaceLimits";
import { useConnectionList, useCurrentWorkspace, useListConnectionsStatusesAsync } from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useDrawerActions } from "core/services/ui/DrawerService";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";

import styles from "./AllConnectionsPage.module.scss";
import { ConnectionsListCard } from "./ConnectionsListCard";
import { ConnectionsSummary } from "./ConnectionsSummary";
import { ConnectionRoutePaths } from "../../routePaths";

const emptyArray: never[] = [];
export const AllConnectionsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_LIST);
  const navigate = useNavigate();
  const { activeConnectionLimitReached, limits } = useCurrentWorkspaceLimits();
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();
  const { closeDrawer } = useDrawerActions();

  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useIntent("CreateConnection", { workspaceId });

  const connections = useConnectionList()?.connections ?? (emptyArray as WebBackendConnectionListItem[]);
  const hasConnections = connections.length > 0;
  const isAllConnectionsStatusEnabled = useExperiment("connections.connectionsStatusesEnabled");

  const connectionIds = useMemo(() => connections.map((connection) => connection.connectionId), [connections]);
  const { data: statusesByConnectionId } = useListConnectionsStatusesAsync(
    connectionIds,
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

  return (
    <Suspense fallback={<LoadingPage />}>
      <>
        <HeadTitle titles={[{ id: "sidebar.connections" }]} />
        {hasConnections ? (
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
                    <Suspense fallback={null}>
                      <ConnectionsSummary connections={connections} statuses={statusesByConnectionId} />
                    </Suspense>
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
              <ConnectionsListCard connections={connections} />
            </ScrollParent>
          </PageGridContainer>
        ) : (
          <ConnectionOnboarding onCreate={onCreateClick} />
        )}
      </>
    </Suspense>
  );
};
