import classNames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { CreditsIcon } from "components/icons/CreditsIcon";
import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";

import { useExperiment } from "hooks/services/Experiment";
import { FeatureItem, useFeature } from "hooks/services/Feature";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { useExperimentSpeedyConnection } from "packages/cloud/components/experiments/SpeedyConnection/hooks/useExperimentSpeedyConnection";
import { SpeedyConnectionBanner } from "packages/cloud/components/experiments/SpeedyConnection/SpeedyConnectionBanner";
import { WorkspaceTrialStatus } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useAuthService } from "packages/cloud/services/auth/AuthService";
import { useIntercom } from "packages/cloud/services/thirdParty/intercom";
import { useGetCloudWorkspace } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { RoutePaths } from "pages/routePaths";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import { ResourceNotFoundErrorBoundary } from "views/common/ResorceNotFoundErrorBoundary";
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
  useIntercom();
  const workspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(workspace.workspaceId);
  const isAllowUpdateConnectorsEnabled = useFeature(FeatureItem.AllowUpdateConnectors);
  const isShowAdminWarningEnabled = useFeature(FeatureItem.ShowAdminWarningInWorkspace);
  const isNewTrialPolicy = useExperiment("billing.newTrialPolicy", false);

  // exp-speedy-connection
  const { isExperimentVariant } = useExperimentSpeedyConnection();

  const { hasCorporateEmail } = useAuthService();
  const isTrial = isNewTrialPolicy
    ? cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.IN_TRIAL ||
      cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.PRE_TRIAL
    : Boolean(cloudWorkspace.trialExpiryTimestamp);
  const showExperimentBanner = isExperimentVariant && isTrial && hasCorporateEmail();

  return (
    <div className={styles.mainContainer}>
      <InsufficientPermissionsErrorBoundary errorComponent={<StartOverErrorView />}>
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
                withNotification={cloudWorkspace.remainingCredits <= LOW_BALANCE_CREDIT_THRESHOLD}
              />
              <CloudResourcesDropdown /> <CloudSupportDropdown />
              <NavItem
                label={<FormattedMessage id="sidebar.settings" />}
                icon={<SettingsIcon />}
                to={RoutePaths.Settings}
                withNotification={isAllowUpdateConnectorsEnabled}
              />
            </MenuContent>
          </MenuContent>
        </SideBar>
        <div
          className={classNames(styles.content, {
            [styles.speedyConnectionBanner]: showExperimentBanner,
          })}
        >
          {showExperimentBanner ? <SpeedyConnectionBanner /> : <WorkspaceStatusBanner />}
          <div className={styles.dataBlock}>
            <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />}>
              <React.Suspense fallback={<LoadingPage />}>{props.children ?? <Outlet />}</React.Suspense>
            </ResourceNotFoundErrorBoundary>
          </div>
        </div>
      </InsufficientPermissionsErrorBoundary>
    </div>
  );
};

export default CloudMainView;
