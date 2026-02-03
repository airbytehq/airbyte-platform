import classNames from "classnames";
import React from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";
import { LicenseBanner } from "components/ui/LicenseBanner/LicenseBanner";

import { SideBar } from "area/layout/SideBar";
import { StatusBanner } from "cloud/area/billing/components/StatusBanner";
import { useTrialEndedModal } from "cloud/area/billing/utils/useTrialEndedModal";
import { usePrefetchOrganizationSummaries } from "core/api/";
import { DefaultErrorBoundary, ForbiddenErrorBoundary } from "core/errors";
import { FeatureItem, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";

import styles from "./MainLayout.module.scss";

const MainLayout: React.FC<React.PropsWithChildren> = () => {
  usePrefetchOrganizationSummaries()();
  useTrialEndedModal();
  const isCloudApp = useIsCloudApp();

  const checkEnterpriseLicense = useFeature(FeatureItem.EnterpriseLicenseChecking);

  return (
    <ForbiddenErrorBoundary>
      <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
        {checkEnterpriseLicense && <LicenseBanner />}
        {isCloudApp && <StatusBanner />}
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
