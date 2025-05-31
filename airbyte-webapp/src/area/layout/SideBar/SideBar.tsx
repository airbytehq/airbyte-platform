import classNames from "classnames";
import { PropsWithChildren } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { matchPath, useLocation } from "react-router-dom";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ThemeToggle } from "components/ui/ThemeToggle";

import { AirbyteOrgPicker } from "area/layout/SideBar/components/AirbyteOrgPicker";
import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { CloudHelpDropdown } from "packages/cloud/views/layout/CloudMainView/CloudHelpDropdown";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { ConnectorBuilderRoutePaths } from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { RoutePaths, SettingsRoutePaths } from "pages/routePaths";

import { HelpDropdown } from "./components/HelpDropdown";
import { MenuContent } from "./components/MenuContent";
import { NavDropdown } from "./components/NavDropdown";
import { NavItem } from "./components/NavItem";
import styles from "./SideBar.module.scss";

const HIDDEN_SIDEBAR_PATHS = [
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.ConnectorBuilder}/${ConnectorBuilderRoutePaths.Edit}`,
];

const WorkspaceNavItems = () => {
  const { hasNewVersions } = useGetConnectorsOutOfDate();
  const workspaceId = useCurrentWorkspaceId();
  const basePath = `${RoutePaths.Workspaces}/${workspaceId}/`;
  return (
    <>
      <NavItem
        label={<FormattedMessage id="sidebar.connections" />}
        icon="connection"
        to={basePath + RoutePaths.Connections}
        testId="connectionsLink"
      />
      <NavItem
        label={<FormattedMessage id="sidebar.sources" />}
        icon="source"
        to={basePath + RoutePaths.Source}
        testId="sourcesLink"
      />
      <NavItem
        label={<FormattedMessage id="sidebar.destinations" />}
        icon="destination"
        testId="destinationsLink"
        to={basePath + RoutePaths.Destination}
      />
      <NavItem
        label={<FormattedMessage id="sidebar.builder" />}
        icon="wrench"
        testId="builderLink"
        to={basePath + RoutePaths.ConnectorBuilder}
      />
      <NavItem
        label={<FormattedMessage id="sidebar.settings" />}
        icon="gear"
        to={basePath + RoutePaths.Settings}
        withNotification={hasNewVersions}
      />
    </>
  );
};

const OrganizationNavItems = () => {
  const organizationId = useCurrentOrganizationId();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const displayOrganizationUsers = useFeature(FeatureItem.DisplayOrganizationUsers);
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling);
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage);
  const basePath = `${RoutePaths.Organization}/${organizationId}/`;
  return (
    <>
      {multiWorkspaceUI && canViewOrgSettings && (
        <>
          <NavItem
            label={<FormattedMessage id="workspaces.title" />}
            icon="grid"
            to={basePath + RoutePaths.Workspaces}
            testId="workspacesLink"
          />
          {displayOrganizationUsers && (
            <NavItem
              label={<FormattedMessage id="settings.members" />}
              icon="community"
              to={basePath + SettingsRoutePaths.OrganizationMembers}
              testId="organizationMembersLink"
            />
          )}
          {canManageOrganizationBilling && (
            <NavItem
              label={<FormattedMessage id="sidebar.billing" />}
              icon="credits"
              to={basePath + CloudSettingsRoutePaths.Billing}
              testId="billingLink"
            />
          )}
          {canViewOrganizationUsage && (
            <NavItem
              label={<FormattedMessage id="settings.usage" />}
              icon="chart"
              to={basePath + CloudSettingsRoutePaths.OrganizationUsage}
              testId="organizationUsageLink"
            />
          )}
        </>
      )}
      <NavItem
        label={<FormattedMessage id="settings.organizationSettings" />}
        icon="gear"
        to={basePath + RoutePaths.Settings}
        testId="orgSettingsLink"
      />
    </>
  );
};

export const SideBar: React.FC<PropsWithChildren> = () => {
  const { logout, user, authType } = useAuthService();
  const { formatMessage } = useIntl();
  const { pathname } = useLocation();
  const isHidden = HIDDEN_SIDEBAR_PATHS.some((path) => !!matchPath(path, pathname));
  const isCloudApp = useIsCloudApp();

  const organizationId = useCurrentOrganizationId();
  const showOrgNav = Boolean(organizationId);

  const username =
    authType === "simple" || authType === "none"
      ? formatMessage({ id: "sidebar.defaultUsername" })
      : user?.name?.trim() || user?.email?.trim();

  return (
    <nav className={classNames(styles.sidebar, { [styles.hidden]: isHidden })}>
      <AirbyteOrgPicker />
      {/* NOTE: AdminWorkspaceWarning wants a workspace */}
      {/* <IfFeatureEnabled feature={FeatureItem.ShowAdminWarningInWorkspace}>
        <AdminWorkspaceWarning />
      </IfFeatureEnabled> */}
      <FlexContainer className={styles.sidebar__menuItems} direction="column" justifyContent="flex-start">
        <MenuContent data-testid="navMainItems">
          {showOrgNav ? <OrganizationNavItems /> : <WorkspaceNavItems />}
        </MenuContent>
        <Box className={styles.sidebar__menuContentSeparator} />
        <MenuContent>
          {isCloudApp ? <CloudHelpDropdown /> : <HelpDropdown />}
          <ThemeToggle />
          {logout && user && (
            <NavDropdown
              buttonTestId="sidebar.userDropdown"
              onChange={({ value }) => {
                value === "logout" && logout();
              }}
              options={[
                {
                  as: "a",
                  href: RoutePaths.Settings, // NOTE: This needs to be fixed once user paths are set up
                  displayName: formatMessage({ id: "sidebar.userSettings" }),
                  internal: true,
                  icon: <Icon type="gear" />,
                },
                {
                  as: "button",
                  displayName: formatMessage({ id: "sidebar.logout" }),
                  icon: <Icon type="signout" />,
                  value: "logout",
                  className: styles.sidebar__logoutButton,
                  "data-testid": "sidebar.signout",
                },
              ]}
              icon="user"
              label={username}
            />
          )}
        </MenuContent>
      </FlexContainer>
    </nav>
  );
};
