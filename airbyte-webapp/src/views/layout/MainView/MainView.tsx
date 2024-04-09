import classNames from "classnames";
import React from "react";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";

import { useListWorkspacesInfinite } from "core/api";
import { ApiErrorBoundary } from "core/errors";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";

import styles from "./MainView.module.scss";
import { HelpDropdown } from "../SideBar/components/HelpDropdown";
import { SideBar } from "../SideBar/SideBar";

const MainView: React.FC<React.PropsWithChildren> = (props) => {
  const { hasNewVersions } = useGetConnectorsOutOfDate();

  return (
    <FlexContainer className={classNames(styles.mainViewContainer)} gap="none">
      <SideBar
        workspaceFetcher={useListWorkspacesInfinite}
        bottomSlot={<HelpDropdown />}
        settingHighlight={hasNewVersions}
      />
      <div className={styles.content}>
        <ApiErrorBoundary>
          <React.Suspense fallback={<LoadingPage />}>{props.children}</React.Suspense>
        </ApiErrorBoundary>
      </div>
    </FlexContainer>
  );
};

export default MainView;
