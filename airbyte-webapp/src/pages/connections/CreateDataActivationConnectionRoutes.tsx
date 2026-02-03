import { Suspense, useState, createContext, useContext, useMemo } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { HeadTitle } from "components/ui/HeadTitle";
import LoadingPage from "components/ui/LoadingPage";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { CreateConnectionFlowLayout } from "area/connection/components/CreateConnectionFlowLayout";
import { ConnectorDocumentationWrapper } from "area/connector/components/ConnectorDocumentationLayout";
import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { CreateConnectionTitleBlock } from "pages/connections/CreateConnectionPage/CreateConnectionTitleBlock";
import { RoutePaths } from "pages/routePaths";

interface StreamMappingsContextValue {
  streamMappings?: DataActivationConnectionFormValues;
  setStreamMappings: React.Dispatch<React.SetStateAction<DataActivationConnectionFormValues | undefined>>;
}

export const StreamMappingsContext = createContext<StreamMappingsContextValue | undefined>(undefined);

export const useStreamMappings = () => {
  const context = useContext(StreamMappingsContext);
  if (!context) {
    throw new Error("useStreamMappings must be used within a StreamMappingsProvider");
  }
  return context;
};

export const CreateDataActivationConnectionRoutes = () => {
  const [streamMappings, setStreamMappings] = useState<DataActivationConnectionFormValues>();
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW_DATA_ACTIVATION);

  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();

  const contextValue = useMemo(() => ({ streamMappings, setStreamMappings }), [streamMappings, setStreamMappings]);

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.connections" }),
      to: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/`,
    },
    { label: formatMessage({ id: "connection.newConnection" }) },
  ];

  return (
    <ConnectorDocumentationWrapper>
      <CreateConnectionFlowLayout.Grid>
        <CreateConnectionFlowLayout.Header>
          <HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />
          <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
            <CreateConnectionTitleBlock />
          </PageHeaderWithNavigation>
        </CreateConnectionFlowLayout.Header>
        <Suspense fallback={<LoadingPage />}>
          <StreamMappingsContext.Provider value={contextValue}>
            <Outlet />
          </StreamMappingsContext.Provider>
        </Suspense>
      </CreateConnectionFlowLayout.Grid>
    </ConnectorDocumentationWrapper>
  );
};
