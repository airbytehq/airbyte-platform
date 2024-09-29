import { Suspense } from "react";
import { useIntl } from "react-intl";
import { Navigate, useParams, useSearchParams } from "react-router-dom";

import { LoadingPage } from "components";
import { CreateConnectionForm } from "components/connection/CreateConnectionForm/CreateConnectionForm";
import { HeadTitle } from "components/HeadTitle";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConfigureConnectionPage.module.scss";
import { CreateConnectionTitleBlock } from "../CreateConnectionPage/CreateConnectionTitleBlock";

export const ConfigureConnectionPage = () => {
  const { formatMessage } = useIntl();
  const { workspaceId } = useParams<{ workspaceId: string }>();
  const [searchParams] = useSearchParams();

  const sourceId = searchParams.get("sourceId");
  const destinationId = searchParams.get("destinationId");

  if (!sourceId || !destinationId) {
    return (
      <Navigate
        to={{
          pathname: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`,
          search: `?${searchParams.toString()}`,
        }}
      />
    );
  }

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.connections" }),
      to: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/`,
    },
    { label: formatMessage({ id: "connection.newConnection" }) },
  ];

  return (
    <div className={styles.container}>
      <HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />
      <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
        <CreateConnectionTitleBlock />
      </PageHeaderWithNavigation>
      <Suspense fallback={<LoadingPage />}>
        <CreateConnectionForm />
      </Suspense>
    </div>
  );
};
