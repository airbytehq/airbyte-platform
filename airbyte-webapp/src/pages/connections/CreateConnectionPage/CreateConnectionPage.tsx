import { MutableRefObject, Suspense, useEffect, useRef } from "react";
import { useIntl } from "react-intl";
import { Navigate, useSearchParams } from "react-router-dom";

import { LoadingPage } from "components";
import { DefineDestination } from "components/connection/CreateConnection/DefineDestination";
import { DefineSource } from "components/connection/CreateConnection/DefineSource";
import { HeadTitle } from "components/HeadTitle";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useDestinationDefinitionList, useGetDestination, useGetSource } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { AppActionCodes, trackAction } from "core/utils/datadog";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import { CreateConnectionTitleBlock } from "./CreateConnectionTitleBlock";

export const CreateConnectionPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW);
  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.connections" }),
      to: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/`,
    },
    { label: formatMessage({ id: "connection.newConnection" }) },
  ];

  return (
    <ConnectorDocumentationWrapper>
      <HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />
      <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
        <CreateConnectionTitleBlock />
      </PageHeaderWithNavigation>
      <Suspense fallback={<LoadingPage />}>
        <CurrentStep />
      </Suspense>
    </ConnectorDocumentationWrapper>
  );
};

const CurrentStep: React.FC = () => {
  const workspaceId = useCurrentWorkspaceId();
  const [searchParams, setSearchParams] = useSearchParams();
  const sourceId = searchParams.get("sourceId");
  const destinationId = searchParams.get("destinationId");
  const source = useGetSource(sourceId);
  const destination = useGetDestination(destinationId);
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const destinationDefinition = destinationDefinitionMap.get(destination?.destinationDefinitionId || "");
  const dataActivationEnabled = useExperiment("connection.dataActivationUI");

  const sourceRef: MutableRefObject<string | null> = useRef(sourceId);
  useEffect(() => {
    if (destinationId) {
      // don't do anything if destination is already set
      return;
    }
    if (sourceRef.current !== sourceId && sourceId) {
      // when the sourceId changes remove all params except sourceId
      const paramKeys = Array.from(searchParams.keys());
      paramKeys.forEach((key) => {
        if (key !== "sourceId") {
          searchParams.delete(key);
        }
      });
      setSearchParams(searchParams);
    }
    sourceRef.current = sourceId;
  }, [sourceId, destinationId, searchParams, setSearchParams]);

  if (!source) {
    return <DefineSource />;
  }
  // source is configured, but destination is not
  if (!destination) {
    return <DefineDestination />;
  }
  // both source and destination are configured, configure the connection now
  if (source && destination) {
    // Data Activation connections are handled in a different route
    if (dataActivationEnabled && destinationDefinition?.supportsDataActivation) {
      return (
        <Navigate
          to={{
            pathname: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.ConfigureDataActivation}`,
            search: `?${searchParams.toString()}`,
          }}
        />
      );
    }
    return (
      <Navigate
        to={{
          pathname: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}`,
          search: `?${searchParams.toString()}`,
        }}
      />
    );
  }

  trackAction(AppActionCodes.UNEXPECTED_CONNECTION_FLOW_STATE, {
    sourceId: source?.sourceId,
    destinationId: destination?.destinationId,
    workspaceId,
  });

  return (
    <Navigate
      to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`}
    />
  );
};
