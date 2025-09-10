import classNames from "classnames";
import { PropsWithChildren, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { matchPath, useLocation } from "react-router-dom";

import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ThemeToggle } from "components/ui/ThemeToggle";
import { WorkspacesPicker } from "components/workspace/WorkspacesPicker";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useListWorkspacesInfinite } from "core/api";
import { useAuthService } from "core/services/auth";
import { FeatureItem, IfFeatureEnabled, useFeature } from "core/services/features";
import { ConnectorBuilderRoutePaths } from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { RoutePaths, SettingsRoutePaths } from "pages/routePaths";
import { WORKSPACE_LIST_LENGTH } from "pages/workspaces/WorkspacesPage";

import { AirbyteHomeLink } from "./AirbyteHomeLink";
import { MenuContent } from "./components/MenuContent";
import { NavDropdown } from "./components/NavDropdown";
import { NavItem } from "./components/NavItem";
import styles from "./SideBar.module.scss";

interface SideBarProps {
  bottomSlot?: React.ReactNode;
  settingHighlight?: boolean;
}

const HIDDEN_SIDEBAR_PATHS = [
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.ConnectorBuilder}/${ConnectorBuilderRoutePaths.Edit}`,
];

export const SideBar: React.FC<PropsWithChildren<SideBarProps>> = ({ bottomSlot, settingHighlight }) => {
  const { logout, user, authType } = useAuthService();
  const { formatMessage } = useIntl();

  const { pathname } = useLocation();
  const isHidden = HIDDEN_SIDEBAR_PATHS.some((path) => !!matchPath(path, pathname));

  const username = useMemo(() => {
    if (authType === "simple" || authType === "none") {
      return formatMessage({ id: "sidebar.defaultUsername" });
    }
    return user?.name?.trim() || user?.email?.trim();
  }, [authType, user?.name, user?.email, formatMessage]);

  const workspaceId = useCurrentWorkspaceId();
  const workspacesPath = `${RoutePaths.Workspaces}/${workspaceId}/`;

  // This is the same query as inside the picker. We want to show the workspace picker if a) the MultiWorkspaceUI
  // feature flag is on or b) there is more than one workspace
  const { data: workspacePages } = useListWorkspacesInfinite(WORKSPACE_LIST_LENGTH, "", true);
  const workspaces = useMemo(
    () => workspacePages?.pages.flatMap((page) => page.data.workspaces) ?? [],
    [workspacePages]
  );
  const showWorkspacesPicker = useFeature(FeatureItem.MultiWorkspaceUI) || workspaces.length > 1;

  return (
    <nav className={classNames(styles.sidebar, { [styles.hidden]: isHidden })}>
      <AirbyteHomeLink />
      <IfFeatureEnabled feature={FeatureItem.ShowAdminWarningInWorkspace}>
        <AdminWorkspaceWarning />
      </IfFeatureEnabled>
      {showWorkspacesPicker && <WorkspacesPicker />}
      <FlexContainer className={styles.sidebar__menuItems} direction="column" justifyContent="space-between">
        <MenuContent data-testid="navMainItems">
          <NavItem
            label={<FormattedMessage id="sidebar.connections" />}
            icon="connection"
            to={workspacesPath + RoutePaths.Connections}
            testId="connectionsLink"
          />
          <NavItem
            label={<FormattedMessage id="sidebar.sources" />}
            icon="source"
            to={workspacesPath + RoutePaths.Source}
            testId="sourcesLink"
          />
          <NavItem
            label={<FormattedMessage id="sidebar.destinations" />}
            icon="destination"
            testId="destinationsLink"
            to={workspacesPath + RoutePaths.Destination}
          />
          <NavItem
            label={<FormattedMessage id="sidebar.builder" />}
            icon="wrench"
            testId="builderLink"
            to={workspacesPath + RoutePaths.ConnectorBuilder}
          />
          <NavItem
            label={<FormattedMessage id="sidebar.settings" />}
            icon="gear"
            to={workspacesPath + RoutePaths.Settings}
            withNotification={settingHighlight}
          />
        </MenuContent>
        <MenuContent>
          {bottomSlot}
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
                  href: `${workspacesPath}${RoutePaths.Settings}/${SettingsRoutePaths.Account}`,
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
