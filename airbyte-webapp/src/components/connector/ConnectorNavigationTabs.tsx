import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Tabs } from "components/ui/Tabs";
import { LinkTab } from "components/ui/Tabs/LinkTab";

import { RoutePaths } from "pages/routePaths";

enum TabTypes {
  OVERVIEW = "overview",
  SETTINGS = "settings",
}

export const ConnectorNavigationTabs: React.FC<{ connectorType: "source" | "destination" }> = ({ connectorType }) => {
  const params = useParams<{ "*": TabTypes | "" | undefined; workspaceId: string; id: string }>();

  const connectorTypePath = connectorType === "source" ? RoutePaths.Source : RoutePaths.Destination;

  const tabs = [
    {
      id: TabTypes.OVERVIEW,
      name: <FormattedMessage id="tables.overview" />,
      to: `/${RoutePaths.Workspaces}/${params.workspaceId}/${connectorTypePath}/${params.id}/`,
    },
    {
      id: TabTypes.SETTINGS,
      name: <FormattedMessage id="tables.settings" />,
      to: `/${RoutePaths.Workspaces}/${params.workspaceId}/${connectorTypePath}/${params.id}/settings`,
    },
  ];

  const currentTab = useMemo<TabTypes | "" | undefined>(
    () => (params["*"] === "" ? TabTypes.OVERVIEW : params["*"]),
    [params]
  );

  return (
    <Box pl="xl">
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
    </Box>
  );
};
