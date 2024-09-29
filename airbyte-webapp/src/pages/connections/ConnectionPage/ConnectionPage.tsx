import React, { Suspense } from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { HeadTitle } from "components/HeadTitle";

import { useCurrentConnectionId } from "area/connection/utils/useCurrentConnectionId";
import { useCurrentConnection } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { ConnectionEditServiceProvider } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperimentContext } from "hooks/services/Experiment";

import styles from "./ConnectionPage.module.scss";
import { ConnectionPageHeader } from "./ConnectionPageHeader";

const ConnectionHeadTitle: React.FC = () => {
  const connection = useCurrentConnection();
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
  const connectionId = useCurrentConnectionId();

  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM);

  return (
    <ConnectionEditServiceProvider connectionId={connectionId}>
      <DefaultErrorBoundary>
        <div className={styles.container}>
          <ConnectionHeadTitle />
          <ConnectionPageHeader />
          <Suspense fallback={<LoadingPage />}>
            <Outlet />
          </Suspense>
        </div>
      </DefaultErrorBoundary>
    </ConnectionEditServiceProvider>
  );
};
