import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useParams } from "react-router-dom";

import { Tabs, LinkTab } from "components/ui/Tabs";

import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

enum TabTypes {
  SETTINGS = "settings",
  CONNECTIONS = "connections",
}

export const ConnectorNavigationTabs: React.FC<{
  connectorType: "source" | "destination";
  connector: SourceRead | DestinationRead;
  id: string;
}> = ({ connectorType, id }) => {
  const params = useParams<{ "*": TabTypes | "" | undefined; workspaceId: string }>();

  const connectorTypePath = connectorType === "source" ? RoutePaths.Source : RoutePaths.Destination;

  const basePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${connectorTypePath}/${id}`;
  const tabs = [
    {
      id: TabTypes.SETTINGS,
      name: <FormattedMessage id="connector.settings" />,
      to: basePath,
    },
    {
      id: TabTypes.CONNECTIONS,
      name: <FormattedMessage id="connector.connections" />,
      to: `${basePath}/connections`,
    },
  ];

  const currentTab = useMemo<TabTypes | "" | undefined>(
    () => (params["*"] === "" ? TabTypes.SETTINGS : params["*"]),
    [params]
  );

  return (
    <Tabs>
      {tabs.map((tabItem) => {
        return (
          <LinkTab
            id={tabItem.id}
            key={tabItem.id}
            name={tabItem.name}
            to={tabItem.to}
            isActive={tabItem.id === currentTab}
          />
        );
      })}
    </Tabs>
  );
};
