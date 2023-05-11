import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useParams } from "react-router-dom";

import { Tabs } from "components/ui/Tabs";
import { LinkTab } from "components/ui/Tabs/LinkTab";

import { DestinationRead, SourceRead } from "core/request/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

enum TabTypes {
  OVERVIEW = "overview",
  SETTINGS = "settings",
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
      id: TabTypes.OVERVIEW,
      name: <FormattedMessage id="tables.overview" />,
      to: basePath,
    },
    {
      id: TabTypes.SETTINGS,
      name: <FormattedMessage id="tables.settings" />,
      to: `${basePath}/settings`,
    },
  ];

  const currentTab = useMemo<TabTypes | "" | undefined>(
    () => (params["*"] === "" ? TabTypes.OVERVIEW : params["*"]),
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
