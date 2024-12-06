import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useParams } from "react-router-dom";

import { ConnectorNavigationTabs } from "components/connector/ConnectorNavigationTabs";
import { ConnectorTitleBlock } from "components/connector/ConnectorTitleBlock";
import { StepsTypes } from "components/ConnectorBlocks";
import { HeadTitle } from "components/HeadTitle";
import LoadingPage from "components/LoadingPage";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { useGetSourceFromParams } from "area/connector/utils";
import { useSourceDefinitionVersion, useSourceDefinition } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { RoutePaths } from "pages/routePaths";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import styles from "./SourceItemPage.module.scss";

export const SourceItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SOURCE_ITEM);
  const params = useParams<{ workspaceId: string; "*": StepsTypes | "" | undefined }>();
  const source = useGetSourceFromParams();
  const sourceDefinition = useSourceDefinition(source.sourceDefinitionId);
  const actorDefinitionVersion = useSourceDefinitionVersion(source.sourceId);
  const { formatMessage } = useIntl();

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Source}`;

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.sources" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: source.name },
  ];

  return (
    <DefaultErrorBoundary>
      <ConnectorDocumentationWrapper>
        <div className={styles.container}>
          <HeadTitle titles={[{ id: "admin.sources" }, { title: source.name }]} />
          <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} className={styles.pageHeader}>
            <ConnectorTitleBlock
              connector={source}
              connectorDefinition={sourceDefinition}
              actorDefinitionVersion={actorDefinitionVersion}
            />
            <ConnectorNavigationTabs connectorType="source" connector={source} id={source.sourceId} />
          </PageHeaderWithNavigation>
          <Suspense fallback={<LoadingPage />}>
            <DefaultErrorBoundary>
              <div className={styles.pageBody}>
                <Outlet />
              </div>
            </DefaultErrorBoundary>
          </Suspense>
        </div>
      </ConnectorDocumentationWrapper>
    </DefaultErrorBoundary>
  );
};
