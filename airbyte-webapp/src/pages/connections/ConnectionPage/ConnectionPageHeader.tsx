import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useParams } from "react-router-dom";

import { ChangesStatusIcon } from "components/EntityTable/components/ChangesStatusIcon";
import { FlexContainer } from "components/ui/Flex";
import { NextPageHeaderWithNavigation } from "components/ui/PageHeader/NextPageHeaderWithNavigation";
import { Tabs } from "components/ui/Tabs";
import { LinkTab } from "components/ui/Tabs/LinkTab";

import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";
import { RoutePaths } from "pages/routePaths";
import { ConnectionRoutePaths } from "pages/routePaths";

import { ConnectionTitleBlock } from "./ConnectionTitleBlock";

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
  }, [basePath, connection.schemaChange, schemaRefreshing, streamCentricUIEnabled]);

  return (
    <NextPageHeaderWithNavigation breadcrumbsData={breadcrumbsData}>
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
