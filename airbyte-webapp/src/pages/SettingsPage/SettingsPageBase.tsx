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

export interface PageConfig {
  menuConfig: CategoryItem[];
}

interface SettingsPageBaseProps {
  pageConfig: PageConfig;
}

export const SettingsPageBase: React.FC<SettingsPageBaseProps> = ({ pageConfig }) => {
  const navigate = useNavigate();
  const { pathname } = useLocation();

  const menuItems: CategoryItem[] = pageConfig?.menuConfig;

  const onSelectMenuItem = (newPath: string) => navigate(newPath);
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
