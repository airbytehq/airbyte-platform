import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useParams } from "react-router-dom";

import { NextPageHeaderWithNavigation } from "components/ui/PageHeader/NextPageHeaderWithNavigation";
import { Tabs } from "components/ui/Tabs";
import { LinkTab } from "components/ui/Tabs/LinkTab";

import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";
import { RoutePaths } from "pages/routePaths";

import { ConnectionTitleBlock } from "./ConnectionTitleBlock";
import { ConnectionRoutePaths } from "../types";

export const ConnectionPageHeader = () => {
  const streamCentricUIEnabled = useExperiment("connection.streamCentricUI.v1", false);

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
        id: ConnectionRoutePaths.Replication,
        name: <FormattedMessage id="connection.replication" />,
        to: `${basePath}/${ConnectionRoutePaths.Replication}`,
        disabled: schemaRefreshing,
      },
      {
        id: ConnectionRoutePaths.Transformation,
        name: <FormattedMessage id="connectionForm.transformation.title" />,
        to: `${basePath}/${ConnectionRoutePaths.Transformation}`,
        disabled: schemaRefreshing,
      },
    ];

    if (streamCentricUIEnabled) {
      tabs.splice(1, 0, {
        id: ConnectionRoutePaths.JobHistory,
        name: <FormattedMessage id="connectionForm.jobHistory" />,
        to: `${basePath}/${ConnectionRoutePaths.JobHistory}`,
        disabled: schemaRefreshing,
      });
    }

    tabs.push({
      id: ConnectionRoutePaths.Settings,
      name: <FormattedMessage id="sources.settings" />,
      to: `${basePath}/${ConnectionRoutePaths.Settings}`,
      disabled: schemaRefreshing,
    });

    return tabs;
  }, [basePath, schemaRefreshing, streamCentricUIEnabled]);

  return (
    <NextPageHeaderWithNavigation breadCrumbsData={breadcrumbsData}>
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
    </NextPageHeaderWithNavigation>
  );
};
