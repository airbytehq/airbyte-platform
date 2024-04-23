import classNames from "classnames";
import React from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentWorkspace } from "core/api";
import { useGetCloudWorkspaceAsync, useListCloudWorkspacesInfinite } from "core/api/cloud";
import { DefaultErrorBoundary } from "core/errors";
import { SideBar } from "views/layout/SideBar/SideBar";

import { CloudHelpDropdown } from "./CloudHelpDropdown";
import styles from "./CloudMainView.module.scss";
import { WorkspaceStatusBanner } from "./WorkspaceStatusBanner";

const CloudMainView: React.FC<React.PropsWithChildren> = (props) => {
  const workspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspaceAsync(workspace.workspaceId);

  return (
    <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
      {cloudWorkspace && <WorkspaceStatusBanner cloudWorkspace={cloudWorkspace} />}
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
