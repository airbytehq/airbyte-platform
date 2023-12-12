import { Suspense } from "react";
import { useIntl } from "react-intl";
import { Navigate, useSearchParams } from "react-router-dom";

import { LoadingPage } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { SelectDestination } from "components/connection/CreateConnection/SelectDestination";
import { SelectSource } from "components/connection/CreateConnection/SelectSource";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useGetDestination, useGetSource } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { trackAction } from "core/utils/datadog";
import { AppActionCodes } from "hooks/services/AppMonitoringService";
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
  const [searchParams] = useSearchParams();
  const sourceId = searchParams.get("sourceId");
  const destinationId = searchParams.get("destinationId");
  const source = useGetSource(sourceId);
  const destination = useGetDestination(destinationId);

  if (!source) {
    return <SelectSource />;
  }
  // source is configured, but destination is not
  if (!destination) {
    return <SelectDestination />;
  }
  // both source and destination are configured, configure the connection now
  if (source && destination) {
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
