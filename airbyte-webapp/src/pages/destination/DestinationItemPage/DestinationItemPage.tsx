import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet, useParams } from "react-router-dom";

import { LoadingPage } from "components";
import { HeadTitle } from "components/ui/HeadTitle";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { StepsTypes } from "area/connector/components/ConnectorBlocks";
import { ConnectorDocumentationWrapper } from "area/connector/components/ConnectorDocumentationLayout";
import { ConnectorNavigationTabs } from "area/connector/components/ConnectorNavigationTabs";
import { ConnectorTitleBlock } from "area/connector/components/ConnectorTitleBlock";
import { useGetDestinationFromParams } from "area/connector/utils";
import { useDestinationDefinitionVersion, useDestinationDefinition } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { RoutePaths } from "pages/routePaths";

import styles from "./DestinationItemPage.module.scss";

export const DestinationItemPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.DESTINATION_ITEM);
  const params = useParams<{ workspaceId: string; "*": StepsTypes | "" | undefined }>();
  const destination = useGetDestinationFromParams();
  const destinationDefinition = useDestinationDefinition(destination.destinationDefinitionId);
  const actorDefinitionVersion = useDestinationDefinitionVersion(destination.destinationId);
  const { formatMessage } = useIntl();

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Destination}`;

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.destinations" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: destination.name },
  ];

  return (
    <DefaultErrorBoundary>
      <ConnectorDocumentationWrapper>
        <div className={styles.container}>
          <HeadTitle titles={[{ id: "admin.destinations" }, { title: destination.name }]} />
          <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} className={styles.pageHeader}>
            <ConnectorTitleBlock
              connector={destination}
              connectorDefinition={destinationDefinition}
              actorDefinitionVersion={actorDefinitionVersion}
            />
            <ConnectorNavigationTabs
              connectorType="destination"
              connector={destination}
              id={destination.destinationId}
            />
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
