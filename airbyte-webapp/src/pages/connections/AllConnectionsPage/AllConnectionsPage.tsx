import React, { Suspense } from "react";
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
import { useCurrentWorkspace, useCurrentWorkspaceState } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";

import styles from "./AllConnectionsPage.module.scss";
import { ConnectionsListCard } from "./ConnectionsListCard";
import { ConnectionsSummary } from "./ConnectionsSummary";
import { ConnectionRoutePaths } from "../../routePaths";

export const AllConnectionsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_LIST);
  const navigate = useNavigate();
  const { activeConnectionLimitReached, limits } = useCurrentWorkspaceLimits();
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();

  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useIntent("CreateConnection", { workspaceId });

  const { hasConnections } = useCurrentWorkspaceState();

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
                      <ConnectionsSummary />
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
              <ConnectionsListCard />
            </ScrollParent>
          </PageGridContainer>
        ) : (
          <ConnectionOnboarding onCreate={onCreateClick} />
        )}
      </>
    </Suspense>
  );
};
