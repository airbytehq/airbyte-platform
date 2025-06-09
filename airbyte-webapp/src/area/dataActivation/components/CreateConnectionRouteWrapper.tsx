import { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { CreateConnectionFlowLayout } from "components/connection/CreateConnectionFlowLayout";
import { HeadTitle } from "components/HeadTitle";
import LoadingPage from "components/LoadingPage";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { CreateConnectionTitleBlock } from "pages/connections/CreateConnectionPage/CreateConnectionTitleBlock";
import { RoutePaths } from "pages/routePaths";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

export const CreateConnectionRouteWrapper = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW_DATA_ACTIVATION);
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
      <CreateConnectionFlowLayout.Grid>
        <CreateConnectionFlowLayout.Header>
          <HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />
          <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
            <CreateConnectionTitleBlock />
          </PageHeaderWithNavigation>
        </CreateConnectionFlowLayout.Header>
        <Suspense fallback={<LoadingPage />}>
          <Outlet />
        </Suspense>
      </CreateConnectionFlowLayout.Grid>
    </ConnectorDocumentationWrapper>
  );
};
