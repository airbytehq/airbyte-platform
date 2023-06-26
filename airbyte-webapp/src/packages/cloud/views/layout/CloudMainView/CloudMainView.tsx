import React from "react";
import { FormattedMessage } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { CreditsIcon } from "components/icons/CreditsIcon";
import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";

import { useGetCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus } from "core/api/types/CloudApi";
import { FeatureItem, useFeature } from "core/services/features";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { useExperimentSpeedyConnection } from "packages/cloud/components/experiments/SpeedyConnection/hooks/useExperimentSpeedyConnection";
import { SpeedyConnectionBanner } from "packages/cloud/components/experiments/SpeedyConnection/SpeedyConnectionBanner";
import { useAuthService } from "packages/cloud/services/auth/AuthService";
import { RoutePaths } from "pages/routePaths";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";
import { AirbyteHomeLink } from "views/layout/SideBar/AirbyteHomeLink";
import { MenuContent } from "views/layout/SideBar/components/MenuContent";
import { NavItem } from "views/layout/SideBar/components/NavItem";
import SettingsIcon from "views/layout/SideBar/components/SettingsIcon";
import { MainNavItems } from "views/layout/SideBar/MainNavItems";
import { SideBar } from "views/layout/SideBar/SideBar";

import styles from "./CloudMainView.module.scss";
import { CloudResourcesDropdown } from "./CloudResourcesDropdown";
import { CloudSupportDropdown } from "./CloudSupportDropdown";
import { InsufficientPermissionsErrorBoundary } from "./InsufficientPermissionsErrorBoundary";
import { WorkspaceStatusBanner } from "./WorkspaceStatusBanner";
import { LOW_BALANCE_CREDIT_THRESHOLD } from "../../billing/BillingPage/components/LowCreditBalanceHint/LowCreditBalanceHint";
import { WorkspacePopout } from "../../workspaces/WorkspacePopout";

const CloudMainView: React.FC<React.PropsWithChildren<unknown>> = (props) => {
  const workspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(workspace.workspaceId);
  const isShowAdminWarningEnabled = useFeature(FeatureItem.ShowAdminWarningInWorkspace);
  const isNewTrialPolicy = useExperiment("billing.newTrialPolicy", false);
  const { trackError } = useAppMonitoringService();

  // exp-speedy-connection
  const { isExperimentVariant } = useExperimentSpeedyConnection();

  const { hasCorporateEmail } = useAuthService();
  const isTrial = isNewTrialPolicy
    ? cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.in_trial ||
      cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.pre_trial
    : Boolean(cloudWorkspace.trialExpiryTimestamp);
  const showExperimentBanner = isExperimentVariant && isTrial && hasCorporateEmail();

  return (
    <div className={styles.mainContainer}>
      <InsufficientPermissionsErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
        <SideBar>
          <AirbyteHomeLink />
          {isShowAdminWarningEnabled && <AdminWorkspaceWarning />}
          <WorkspacePopout>
            {({ onOpen, value }) => (
              <button className={styles.workspaceButton} onClick={onOpen} data-testid="workspaceButton">
                {value}
              </button>
            )}
          </WorkspacePopout>
          <MenuContent>
            <MainNavItems />
            <MenuContent>
              <NavItem
                to={CloudRoutes.Billing}
                icon={<CreditsIcon />}
                label={<FormattedMessage id="sidebar.billing" />}
                testId="creditsButton"
                withNotification={
                  !cloudWorkspace.remainingCredits || cloudWorkspace.remainingCredits <= LOW_BALANCE_CREDIT_THRESHOLD
                }
              />
              <CloudResourcesDropdown /> <CloudSupportDropdown />
              <NavItem
                label={<FormattedMessage id="sidebar.settings" />}
                icon={<SettingsIcon />}
                to={RoutePaths.Settings}
              />
            </MenuContent>
          </MenuContent>
        </SideBar>
        <div className={styles.content}>
          {showExperimentBanner ? <SpeedyConnectionBanner /> : <WorkspaceStatusBanner />}
          <div className={styles.dataBlock}>
            <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
              <React.Suspense fallback={<LoadingPage />}>{props.children ?? <Outlet />}</React.Suspense>
            </ResourceNotFoundErrorBoundary>
          </div>
        </div>
      </InsufficientPermissionsErrorBoundary>
    </div>
  );
};

export default CloudMainView;
