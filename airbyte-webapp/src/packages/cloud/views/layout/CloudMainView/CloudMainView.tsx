import classNames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { CreditsIcon } from "components/icons/CreditsIcon";

import { FeatureItem, useFeature } from "hooks/services/Feature";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { useExperimentSpeedyConnection } from "packages/cloud/components/experiments/SpeedyConnection/hooks/useExperimentSpeedyConnection";
import { SpeedyConnectionBanner } from "packages/cloud/components/experiments/SpeedyConnection/SpeedyConnectionBanner";
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
import { LOW_BALANCE_CREDIT_THRESHOLD } from "../../credits/CreditsPage/components/LowCreditBalanceHint/LowCreditBalanceHint";
import { WorkspacePopout } from "../../workspaces/WorkspacePopout";

const CloudMainView: React.FC<React.PropsWithChildren<unknown>> = (props) => {
  useIntercom();
  const workspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(workspace.workspaceId);
  const isAllowUpdateConnectorsEnabled = useFeature(FeatureItem.AllowUpdateConnectors);

  // exp-speedy-connection
  const { isExperimentVariant } = useExperimentSpeedyConnection();

  const { hasCorporateEmail } = useAuthService();
  const isTrial = Boolean(cloudWorkspace.trialExpiryTimestamp);
  const showExperimentBanner = isExperimentVariant && isTrial && hasCorporateEmail();

  const [hasWorkspaceCreditsBanner, setHasCreditsBanner] = React.useState(false);

  return (
    <div className={styles.mainContainer}>
      <InsufficientPermissionsErrorBoundary errorComponent={<StartOverErrorView />}>
        <SideBar>
          <AirbyteHomeLink />
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
                to={CloudRoutes.Credits}
                icon={<CreditsIcon />}
                label={<FormattedMessage id="sidebar.credits" />}
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
            [styles.alertBanner]: !!hasWorkspaceCreditsBanner && !showExperimentBanner,
            [styles.speedyConnectionBanner]: showExperimentBanner,
          })}
        >
          {showExperimentBanner && <SpeedyConnectionBanner />}
          <WorkspaceStatusBanner setHasWorkspaceCreditsBanner={setHasCreditsBanner} />
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
