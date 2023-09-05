import { useIntl } from "react-intl";
import { Navigate, useParams, useSearchParams } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll/MainPageWithScroll";
import { CreateConnectionForm } from "components/connection/CreateConnectionForm";
import { CreateConnectionHookForm } from "components/connection/CreateConnectionHookForm/CreateConnectionHookForm";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { useExperiment } from "hooks/services/Experiment";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import { CreateConnectionTitleBlock } from "../CreateConnectionPage/CreateConnectionTitleBlock";

export const ConfigureConnectionPage = () => {
  const { formatMessage } = useIntl();
  const { workspaceId } = useParams<{ workspaceId: string }>();
  const [searchParams] = useSearchParams();
  const doUseCreateConnectionHookForm = useExperiment("form.createConnectionHookForm", false);

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
    <MainPageWithScroll
      headTitle={<HeadTitle titles={[{ id: "connection.newConnectionTitle" }]} />}
      pageTitle={
        <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
          <CreateConnectionTitleBlock />
        </PageHeaderWithNavigation>
      }
    >
      {doUseCreateConnectionHookForm ? <CreateConnectionHookForm /> : <CreateConnectionForm />}
    </MainPageWithScroll>
  );
};
