import classNames from "classnames";
import React from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { LicenseBanner } from "components/LicenseBanner/LicenseBanner";
import { FlexContainer } from "components/ui/Flex";

import { SideBar } from "area/layout/SideBar";
import { usePrefetchOrganizationSummaries } from "core/api/";
import { DefaultErrorBoundary, ForbiddenErrorBoundary } from "core/errors";
import { useIsCloudApp } from "core/utils/app";
import { StatusBanner } from "packages/cloud/area/billing/components/StatusBanner";
import { useTrialEndedModal } from "packages/cloud/area/billing/utils/useTrialEndedModal";

import styles from "./MainLayout.module.scss";

const MainLayout: React.FC<React.PropsWithChildren> = () => {
  const isCloudApp = useIsCloudApp();
  usePrefetchOrganizationSummaries()();
  useTrialEndedModal();

  return (
    <ForbiddenErrorBoundary>
      <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
        {isCloudApp ? <StatusBanner /> : <LicenseBanner />}
        <FlexContainer className={classNames(styles.mainViewContainer)} gap="none">
          <SideBar />
          <div className={styles.content}>
            <DefaultErrorBoundary>
              <React.Suspense fallback={<LoadingPage />}>
                <Outlet />
              </React.Suspense>
            </DefaultErrorBoundary>
          </div>
        </FlexContainer>
      </FlexContainer>
    </ForbiddenErrorBoundary>
  );
};

export default MainLayout;
