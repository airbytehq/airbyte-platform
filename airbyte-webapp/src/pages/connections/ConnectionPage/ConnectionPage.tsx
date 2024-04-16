import React, { Suspense, useMemo } from "react";
import { Outlet, useLocation, useParams } from "react-router-dom";

import { LoadingPage, MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";

import { DefaultErrorBoundary } from "core/errors";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import {
  ConnectionEditServiceProvider,
  useConnectionEditService,
} from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperimentContext } from "hooks/services/Experiment";
import { ConnectionRoutePaths } from "pages/routePaths";

import { ConnectionPageHeader } from "./ConnectionPageHeader";

const ConnectionHeadTitle: React.FC = () => {
  const { connection } = useConnectionEditService();
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);
  useExperimentContext("connection", connection.connectionId);

  return (
    <HeadTitle
      titles={[
        { id: "sidebar.connections" },
        {
          id: "connection.fromTo",
          values: {
            source: connection.source.name,
            destination: connection.destination.name,
          },
        },
      ]}
    />
  );
};

export const ConnectionPage: React.FC = () => {
  const { connectionId = "" } = useParams<{
    connectionId: string;
  }>();
  const location = useLocation();
  const isReplicationPage = useMemo(
    () => location.pathname.includes(`/${ConnectionRoutePaths.Replication}`),
    [location.pathname]
  );

  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM);

  return (
    <ConnectionEditServiceProvider connectionId={connectionId}>
      <DefaultErrorBoundary>
        <MainPageWithScroll
          headTitle={<ConnectionHeadTitle />}
          pageTitle={<ConnectionPageHeader />}
          noBottomPadding={isReplicationPage}
        >
          <Suspense fallback={<LoadingPage />}>
            <Outlet />
          </Suspense>
        </MainPageWithScroll>
      </DefaultErrorBoundary>
    </ConnectionEditServiceProvider>
  );
};
