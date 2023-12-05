import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useParams } from "react-router-dom";

import { ChangesStatusIcon } from "components/EntityTable/components/ChangesStatusIcon";
import { FlexContainer } from "components/ui/Flex";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";
import { Tabs, LinkTab } from "components/ui/Tabs";

import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { RoutePaths, ConnectionRoutePaths } from "pages/routePaths";

import { ConnectionTitleBlock } from "./ConnectionTitleBlock";

export const ConnectionPageHeader = () => {
  const params = useParams<{ workspaceId: string; connectionId: string; "*": ConnectionRoutePaths }>();
  const basePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Connections}/${params.connectionId}`;
  const { formatMessage } = useIntl();
  const currentTab = params["*"] || ConnectionRoutePaths.Status;

  const { connection, schemaRefreshing } = useConnectionEditService();
  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.connections" }),
      to: `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Connections}`,
    },
    { label: connection.name },
  ];

  const tabsData = useMemo(() => {
    const tabs = [
      {
        id: ConnectionRoutePaths.Status,
        name: <FormattedMessage id="sources.status" />,
        to: basePath,
        disabled: schemaRefreshing,
      },
      {
        id: ConnectionRoutePaths.JobHistory,
        name: <FormattedMessage id="connectionForm.jobHistory" />,
        to: `${basePath}/${ConnectionRoutePaths.JobHistory}`,
        disabled: schemaRefreshing,
      },
      {
        id: ConnectionRoutePaths.Replication,
        name: (
          <FlexContainer gap="sm" as="span">
            <FormattedMessage id="connection.replication" />
            <ChangesStatusIcon schemaChange={connection.schemaChange} />
          </FlexContainer>
        ),
        to: `${basePath}/${ConnectionRoutePaths.Replication}`,
        disabled: schemaRefreshing,
      },
      {
        id: ConnectionRoutePaths.Transformation,
        name: <FormattedMessage id="connectionForm.transformation.title" />,
        to: `${basePath}/${ConnectionRoutePaths.Transformation}`,
        disabled: schemaRefreshing,
      },
      {
        id: ConnectionRoutePaths.Settings,
        name: <FormattedMessage id="sources.settings" />,
        to: `${basePath}/${ConnectionRoutePaths.Settings}`,
        disabled: schemaRefreshing,
      },
    ];

    return tabs;
  }, [basePath, connection.schemaChange, schemaRefreshing]);

  return (
    <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
      <ConnectionTitleBlock />
      <Tabs>
        {tabsData.map((tabItem) => (
          <LinkTab
            key={tabItem.id}
            id={tabItem.id}
            name={tabItem.name}
            to={tabItem.to}
            isActive={currentTab === tabItem.id}
            disabled={tabItem.disabled}
          />
        ))}
      </Tabs>
    </PageHeaderWithNavigation>
  );
};
