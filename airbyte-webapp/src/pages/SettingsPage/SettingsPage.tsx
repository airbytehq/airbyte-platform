import React, { Suspense } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll";
import LoadingPage from "components/LoadingPage";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";
import { SideMenu, CategoryItem, SideMenuItem } from "components/ui/SideMenu";

import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";

import { AccountPage } from "./pages/AccountPage";
import { ConfigurationsPage } from "./pages/ConfigurationsPage";
import { DestinationsPage, SourcesPage } from "./pages/ConnectorsPage";
import { MetricsPage } from "./pages/MetricsPage";
import { NotificationPage } from "./pages/NotificationPage";

export interface PageConfig {
  menuConfig: CategoryItem[];
}

interface SettingsPageProps {
  pageConfig?: PageConfig;
}

export const SettingsRoute = {
  Account: "account",
  Destination: "destination",
  Source: "source",
  Configuration: "configuration",
  Notifications: "notifications",
  Metrics: "metrics",
  DataResidency: "data-residency",
} as const;

const SettingsPage: React.FC<SettingsPageProps> = ({ pageConfig }) => {
  const push = useNavigate();
  const { pathname } = useLocation();
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();

  const menuItems: CategoryItem[] = pageConfig?.menuConfig || [
    {
      routes: [
        {
          path: `${SettingsRoute.Account}`,
          name: <FormattedMessage id="settings.account" />,
          component: AccountPage,
        },
        {
          path: `${SettingsRoute.Source}`,
          name: <FormattedMessage id="tables.sources" />,
          indicatorCount: countNewSourceVersion,
          component: SourcesPage,
        },
        {
          path: `${SettingsRoute.Destination}`,
          name: <FormattedMessage id="tables.destinations" />,
          indicatorCount: countNewDestinationVersion,
          component: DestinationsPage,
        },
        {
          path: `${SettingsRoute.Configuration}`,
          name: <FormattedMessage id="admin.configuration" />,
          component: ConfigurationsPage,
        },
        {
          path: `${SettingsRoute.Notifications}`,
          name: <FormattedMessage id="settings.notifications" />,
          component: NotificationPage,
        },
        {
          path: `${SettingsRoute.Metrics}`,
          name: <FormattedMessage id="settings.metrics" />,
          component: MetricsPage,
        },
      ],
    },
  ];

  const onSelectMenuItem = (newPath: string) => push(newPath);
  const firstRoute = menuItems[0].routes?.[0]?.path;

  return (
    <MainPageWithScroll
      headTitle={<HeadTitle titles={[{ id: "sidebar.settings" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="sidebar.settings" />
            </Heading>
          }
        />
      }
    >
      <FlexContainer direction="row" gap="2xl">
        <SideMenu data={menuItems} onSelect={onSelectMenuItem} activeItem={pathname} />
        <FlexItem grow>
          <Suspense fallback={<LoadingPage />}>
            <Routes>
              {menuItems
                .flatMap((menuItem) => menuItem.routes)
                .filter(
                  (menuItem): menuItem is SideMenuItem & { component: NonNullable<SideMenuItem["component"]> } =>
                    !!menuItem.component
                )
                .map(({ path, component: Component }) => (
                  <Route key={path} path={path} element={<Component />} />
                ))}

              <Route path="*" element={<Navigate to={firstRoute} replace />} />
            </Routes>
          </Suspense>
        </FlexItem>
      </FlexContainer>
    </MainPageWithScroll>
  );
};

export default SettingsPage;
