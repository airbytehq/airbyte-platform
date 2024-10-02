import classNames from "classnames";
import React from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";

import { useListCloudWorkspacesInfinite } from "core/api/cloud";
import { DefaultErrorBoundary } from "core/errors";
import { StatusBanner } from "packages/cloud/area/billing/components/StatusBanner/StatusBanner";
import { SideBar } from "views/layout/SideBar/SideBar";

import { CloudHelpDropdown } from "./CloudHelpDropdown";
import styles from "./CloudMainView.module.scss";

const CloudMainView: React.FC<React.PropsWithChildren> = (props) => {
  return (
    <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
      <StatusBanner />
      <FlexContainer className={styles.mainViewContainer} gap="none">
        <SideBar workspaceFetcher={useListCloudWorkspacesInfinite} bottomSlot={<CloudHelpDropdown />} />
        <div className={styles.content}>
          <DefaultErrorBoundary>
            <React.Suspense fallback={<LoadingPage />}>{props.children ?? <Outlet />}</React.Suspense>
          </DefaultErrorBoundary>
        </div>
      </FlexContainer>
    </FlexContainer>
  );
};

export default CloudMainView;
