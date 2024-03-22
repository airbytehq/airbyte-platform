import classNames from "classnames";
import React from "react";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";

import { useListWorkspacesInfinite } from "core/api";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";

import styles from "./MainView.module.scss";
import { SideBar } from "../SideBar/SideBar";

const MainView: React.FC<React.PropsWithChildren> = (props) => {
  const { trackError } = useAppMonitoringService();
  const { hasNewVersions } = useGetConnectorsOutOfDate();

  return (
    <FlexContainer className={classNames(styles.mainViewContainer)} gap="none">
      <SideBar workspaceFetcher={useListWorkspacesInfinite} settingHighlight={hasNewVersions} />
      <div className={styles.content}>
        <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
          <React.Suspense fallback={<LoadingPage />}>{props.children}</React.Suspense>
        </ResourceNotFoundErrorBoundary>
      </div>
    </FlexContainer>
  );
};

export default MainView;
