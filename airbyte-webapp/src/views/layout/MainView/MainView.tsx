import classNames from "classnames";
import React from "react";

import { LoadingPage } from "components";
import { LicenseBanner } from "components/LicenseBanner/LicenseBanner";
import { FlexContainer } from "components/ui/Flex";

import { useListWorkspacesInfinite } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";

import styles from "./MainView.module.scss";
import { HelpDropdown } from "../SideBar/components/HelpDropdown";
import { SideBar } from "../SideBar/SideBar";

const MainView: React.FC<React.PropsWithChildren> = (props) => {
  const { hasNewVersions } = useGetConnectorsOutOfDate();

  return (
    <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
      <LicenseBanner />
      <FlexContainer className={classNames(styles.mainViewContainer)} gap="none">
        <SideBar
          workspaceFetcher={useListWorkspacesInfinite}
          bottomSlot={<HelpDropdown />}
          settingHighlight={hasNewVersions}
        />
        <div className={styles.content}>
          <DefaultErrorBoundary>
            <React.Suspense fallback={<LoadingPage />}>{props.children}</React.Suspense>
          </DefaultErrorBoundary>
        </div>
      </FlexContainer>
    </FlexContainer>
  );
};

export default MainView;
