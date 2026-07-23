import classNames from "classnames";
import { PropsWithChildren, useCallback, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { matchPath, useLocation } from "react-router-dom";

import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";
import { Box } from "components/ui/Box";
import { Icon } from "components/ui/Icon";
import { Separator } from "components/ui/Separator";
import { Text } from "components/ui/Text";
import { ThemeToggle } from "components/ui/ThemeToggle";

import { useGetConnectorsOutOfDate } from "area/connector/utils/useConnector";
import { AirbyteHomeLink } from "area/layout/SideBar/AirbyteHomeLink";
import { OrganizationPicker } from "area/organization/OrganizationPicker/OrganizationPicker";
import { useCurrentOrganizationId, useTrackLastOrganization } from "area/organization/utils";
import { WorkspacesPickerNext } from "area/workspace/components/WorkspacesPickerNext";
import { CloudHelpDropdown } from "cloud/components/CloudHelpDropdown";
import {
  useCurrentWorkspaceOrUndefined,
  useDefaultWorkspaceInOrganization,
  useGetWorkspaceIgnoreErrors,
} from "core/api";
import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { useAuthService } from "core/services/auth";
import { FeatureItem, IfFeatureEnabled, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";
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

interface WorkspaceNavItemsProps {
  workspace: WorkspaceRead;
}

const WorkspaceNavItems: React.FC<WorkspaceNavItemsProps> = ({ workspace }) => {
  const { hasNewVersions } = useGetConnectorsOutOfDate();

  const createWorkspaceLink = useCallback(
    (link: string) => `/${RoutePaths.Workspaces}/${workspace.workspaceId}${link}`,
    [workspace]
  );

  return (
    <MenuContent data-testid="navMainItems">
      <NavItem
        label={<FormattedMessage id="sidebar.connections" />}
        icon="connection"
        to={createWorkspaceLink(`/${RoutePaths.Connections}`)}
        testId="connectionsLink"
      />
      <NavItem
        label={<FormattedMessage id="sidebar.sources" />}
        icon="source"
        to={createWorkspaceLink(`/${RoutePaths.Source}`)}
        testId="sourcesLink"
      />
      <NavItem
        label={<FormattedMessage id="sidebar.destinations" />}
        icon="destination"
        testId="destinationsLink"
        to={createWorkspaceLink(`/${RoutePaths.Destination}`)}
      />
      <NavItem
        label={<FormattedMessage id="sidebar.builder" />}
        icon="wrench"
        testId="builderLink"
        to={createWorkspaceLink(`/${RoutePaths.ConnectorBuilder}`)}
      />
      <NavItem
        label={<FormattedMessage id="sidebar.workspaceSettings" />}
        icon="gear"
        to={createWorkspaceLink(`/${RoutePaths.Settings}`)}
        withNotification={hasNewVersions}
      />
    </MenuContent>
  );
};

const OrganizationNavItems = () => {
  const organizationId = useCurrentOrganizationId();
  const canViewOrganizationSettings = useGeneratedIntent(Intent.ViewOrganizationSettings);
  const basePath = `${RoutePaths.Organization}/${organizationId}/`;
  return (
    <MenuContent data-testid="navMainItems">
      <NavItem
        label={<FormattedMessage id="sidebar.home" />}
        icon="house"
        to={basePath + RoutePaths.Workspaces}
        testId="workspacesLink"
      />
      {canViewOrganizationSettings && (
        <NavItem
          label={<FormattedMessage id="settings.organizationSettings" />}
          icon="gear"
          to={basePath + RoutePaths.Settings}
          testId="orgSettingsLink"
        />
      )}
    </MenuContent>
  );
};

// Depending on the route, there are various possibilities for the "current workspace", ordered by priority:
// 1. In /workspaces/:workspaceId routes, we can get the current workspace directly from the URL parameter
// 2. In /organization/:organizationId routes, there are three cases:
//    a. We find a value in localStorage indicating which workspace the user most recently visited in this org
//    b. The organization has at least one workspace, so we show the first one as the "default"
//    c. The organization has no workspaces, so we do not render the links in the sidebar at all
const useCurrentWorkspaceForOrganization = () => {
  const [organizationWorkspaceMap, setOrganizationWorkspaceMap] = useLocalStorage(
    "airbyte_organization-workspace-map",
    {}
  );
  const currentOrganizationId = useCurrentOrganizationId();
  const currentWorkspace = useCurrentWorkspaceOrUndefined();
  const storedWorkspaceId = organizationWorkspaceMap[currentOrganizationId];
  const savedWorkspace = useGetWorkspaceIgnoreErrors(storedWorkspaceId);
  useEffect(() => {
    // If the user is currently in a /workspaces/:workspaceId route, we should store that in localStorage as the most
    // recently visited workspace in the organization
    if (currentWorkspace?.workspaceId) {
      setOrganizationWorkspaceMap((map) => ({ ...map, [currentOrganizationId]: currentWorkspace.workspaceId }));
    }
  }, [currentWorkspace?.workspaceId, currentOrganizationId, setOrganizationWorkspaceMap]);
  const defaultOrganizationWorkspace = useDefaultWorkspaceInOrganization(
    currentOrganizationId,
    !currentWorkspace && !savedWorkspace
  );
  return currentWorkspace ?? savedWorkspace ?? defaultOrganizationWorkspace ?? null;
};

export const SideBar: React.FC<PropsWithChildren> = () => {
  const { logout, user, authType } = useAuthService();
  const { formatMessage } = useIntl();
  const { pathname } = useLocation();
  const isHidden = HIDDEN_SIDEBAR_PATHS.some((path) => !!matchPath(path, pathname));
  const isCloudApp = useIsCloudApp();
  const showOrganizationUI = useFeature(FeatureItem.OrganizationUI);

  const workspace = useCurrentWorkspaceForOrganization();

  useTrackLastOrganization();

  const username =
    authType === "simple" || authType === "none"
      ? formatMessage({ id: "sidebar.defaultUsername" })
      : user?.name?.trim() || user?.email?.trim();

  const basePath = pathname.split("/").slice(0, 3).join("/");

  const location = useLocation();

  // The NavDropdown for user settings does not work with react-router's NavLink "active" detection, because it is a
  // dropdown and not a standard link. This is a simple proxy for whether the user settings routes are open.
  const areUserSettingsActive = location.pathname.split("/").includes(SettingsRoutePaths.User);

  return (
    <nav className={classNames(styles.sidebar, { [styles.hidden]: isHidden })}>
      {showOrganizationUI ? <OrganizationPicker /> : <AirbyteHomeLink />}

      <Separator />

      <IfFeatureEnabled feature={FeatureItem.ShowAdminWarningInWorkspace}>
        <AdminWorkspaceWarning />
      </IfFeatureEnabled>

      <IfFeatureEnabled feature={FeatureItem.OrganizationUI}>
        <Box mt="xl" mb="md">
          <Text size="sm" bold color="grey" className={styles.sidebar__sectionTitle}>
            <FormattedMessage id="settings.organization" />
          </Text>
        </Box>
        <OrganizationNavItems />
        <Box mt="md">
          <Separator />
        </Box>
      </IfFeatureEnabled>

      {workspace && (
        <>
          <Box mt="xl">
            <Text size="sm" bold color="grey" className={styles.sidebar__sectionTitle}>
              <FormattedMessage id="settings.workspaceSettings" />
            </Text>
          </Box>
          <IfFeatureEnabled feature={FeatureItem.ShowWorkspacePicker}>
            <Box mt="md">
              <WorkspacesPickerNext currentWorkspace={workspace} />
            </Box>
          </IfFeatureEnabled>
          <Box mt="sm">
            <WorkspaceNavItems workspace={workspace} />
          </Box>
        </>
      )}

      <div className={styles.sidebar__userSettings}>
        <MenuContent>
          {isCloudApp ? <CloudHelpDropdown /> : <HelpDropdown />}
          <ThemeToggle />
          {logout && user && (
            <NavDropdown
              isActive={areUserSettingsActive}
              buttonTestId="sidebar.userDropdown"
              onChange={({ value }) => {
                value === "logout" && logout();
              }}
              options={[
                {
                  as: "a",
                  href: `${basePath}/${SettingsRoutePaths.User}/${SettingsRoutePaths.Account}`,
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
      </div>
    </nav>
  );
};
