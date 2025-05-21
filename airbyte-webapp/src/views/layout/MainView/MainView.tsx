import classNames from "classnames";
import React from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { LicenseBanner } from "components/LicenseBanner/LicenseBanner";
import { FlexContainer } from "components/ui/Flex";

import { DefaultErrorBoundary, ForbiddenErrorBoundary } from "core/errors";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";

import styles from "./MainView.module.scss";
import { HelpDropdown } from "../SideBar/components/HelpDropdown";
import { SideBar } from "../SideBar/SideBar";

const MainView: React.FC<React.PropsWithChildren> = () => {
  const { hasNewVersions } = useGetConnectorsOutOfDate();

  return (
    <ForbiddenErrorBoundary>
      <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
        <LicenseBanner />
        <FlexContainer className={classNames(styles.mainViewContainer)} gap="none">
          <SideBar bottomSlot={<HelpDropdown />} settingHighlight={hasNewVersions} />
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

export default MainView;
