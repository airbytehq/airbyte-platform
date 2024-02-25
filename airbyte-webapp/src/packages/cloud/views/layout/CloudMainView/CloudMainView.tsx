import classNames from "classnames";
import React from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentWorkspace } from "core/api";
import { useGetCloudWorkspaceAsync, useListCloudWorkspacesInfinite } from "core/api/cloud";
import { CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useAuthService } from "core/services/auth";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperimentSpeedyConnection } from "packages/cloud/components/experiments/SpeedyConnection/hooks/useExperimentSpeedyConnection";
import { SpeedyConnectionBanner } from "packages/cloud/components/experiments/SpeedyConnection/SpeedyConnectionBanner";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { SideBar } from "views/layout/SideBar/SideBar";

import { CloudHelpDropdown } from "./CloudHelpDropdown";
import styles from "./CloudMainView.module.scss";
import { InsufficientPermissionsErrorBoundary } from "./InsufficientPermissionsErrorBoundary";
import { WorkspaceStatusBanner } from "./WorkspaceStatusBanner";

const CloudMainView: React.FC<React.PropsWithChildren> = (props) => {
  const workspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspaceAsync(workspace.workspaceId);

  const { trackError } = useAppMonitoringService();

  // exp-speedy-connection
  const { isExperimentVariant } = useExperimentSpeedyConnection();

  const { user } = useAuthService();

  const isTrial =
    cloudWorkspace?.workspaceTrialStatus === WorkspaceTrialStatus.in_trial ||
    cloudWorkspace?.workspaceTrialStatus === WorkspaceTrialStatus.pre_trial;

  const showExperimentBanner = isExperimentVariant && isTrial && user && isCorporateEmail(user.email);

  return (
    <FlexContainer className={classNames(styles.wrapper)} direction="column" gap="none">
      <InsufficientPermissionsErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
        <div>
          {cloudWorkspace &&
            (showExperimentBanner ? (
              <SpeedyConnectionBanner />
            ) : (
              <WorkspaceStatusBanner cloudWorkspace={cloudWorkspace} />
            ))}
        </div>
        <FlexContainer className={styles.mainViewContainer} gap="none">
          <SideBar workspaceFetcher={useListCloudWorkspacesInfinite} bottomSlot={<CloudHelpDropdown />} />
          <div className={styles.content}>
            <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
              <React.Suspense fallback={<LoadingPage />}>{props.children ?? <Outlet />}</React.Suspense>
            </ResourceNotFoundErrorBoundary>
          </div>
        </FlexContainer>
      </InsufficientPermissionsErrorBoundary>
    </FlexContainer>
  );
};

export default CloudMainView;
