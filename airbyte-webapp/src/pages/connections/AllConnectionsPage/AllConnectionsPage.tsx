import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React, { Suspense } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { LoadingPage, MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectionOnboarding } from "components/connection/ConnectionOnboarding";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";

import { useConnectionList } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import ConnectionsTable from "./ConnectionsTable";
import { ConnectionRoutePaths } from "../../routePaths";

export const AllConnectionsPage: React.FC = () => {
  const navigate = useNavigate();

  useTrackPage(PageTrackingCodes.CONNECTIONS_LIST);
  const { connections } = useConnectionList();

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
                  <Heading as="h1" size="lg">
                    <FormattedMessage id="sidebar.connections" />
                  </Heading>
                }
                endComponent={
                  <Button
                    icon={<FontAwesomeIcon icon={faPlus} />}
                    variant="primary"
                    size="sm"
                    onClick={() => onCreateClick()}
                    data-testid="new-connection-button"
                  >
                    <FormattedMessage id="connection.newConnection" />
                  </Button>
                }
              />
            }
          >
            <ConnectionsTable connections={connections} />
          </MainPageWithScroll>
        ) : (
          <ConnectionOnboarding onCreate={onCreateClick} />
        )}
      </>
    </Suspense>
  );
};
