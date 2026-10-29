import classNames from "classnames";
import React from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";

import { SideBar } from "area/layout/SideBar";
import { AdpOrganizationAccessGuard } from "cloud/components/AdpOrganizationAccessGuard";
import { CloudBannerGroup } from "cloud/components/CloudBannerGroup";
import { usePrefetchOrganizationSummaries } from "core/api/";
import { DefaultErrorBoundary, ForbiddenErrorBoundary } from "core/errors";
import { useIsCloudApp } from "core/utils/app";

import styles from "./MainLayout.module.scss";

const MainLayout: React.FC<React.PropsWithChildren> = () => {
  usePrefetchOrganizationSummaries()();
  const isCloudApp = useIsCloudApp();

  return (
    <ForbiddenErrorBoundary>
      <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
        {isCloudApp && <CloudBannerGroup />}
        <FlexContainer className={classNames(styles.mainViewContainer)} gap="none">
          <SideBar />
          <div className={styles.content}>
            <DefaultErrorBoundary>
              <React.Suspense fallback={<LoadingPage />}>
                {isCloudApp ? (
                  <AdpOrganizationAccessGuard>
                    <Outlet />
                  </AdpOrganizationAccessGuard>
                ) : (
                  <Outlet />
                )}
              </React.Suspense>
            </DefaultErrorBoundary>
          </div>
        </FlexContainer>
      </FlexContainer>
    </ForbiddenErrorBoundary>
  );
};

export default MainLayout;
