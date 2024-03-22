import classNames from "classnames";
import { PropsWithChildren } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ThemeToggle } from "components/ui/ThemeToggle";
import { WorkspacesPicker } from "components/workspace/WorkspacesPicker";
import type { WorkspaceFetcher } from "components/workspace/WorkspacesPickerList";

import { useAuthService } from "core/services/auth";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { RoutePaths } from "pages/routePaths";

import { AirbyteHomeLink } from "./AirbyteHomeLink";
import { MenuContent } from "./components/MenuContent";
import { NavDropdown } from "./components/NavDropdown";
import { NavItem } from "./components/NavItem";
import styles from "./SideBar.module.scss";

interface SideBarProps {
  workspaceFetcher: WorkspaceFetcher;
  bottomSlot?: React.ReactNode;
  settingHighlight?: boolean;
}

export const SideBar: React.FC<PropsWithChildren<SideBarProps>> = ({
  workspaceFetcher,
  bottomSlot,
  settingHighlight,
}) => {
  const { logout, user } = useAuthService();
  const { formatMessage } = useIntl();
  return (
    <nav className={classNames(styles.sidebar)}>
      <AirbyteHomeLink />
      <IfFeatureEnabled feature={FeatureItem.ShowAdminWarningInWorkspace}>
        <AdminWorkspaceWarning />
      </IfFeatureEnabled>
      <IfFeatureEnabled feature={FeatureItem.MultiWorkspaceUI}>
        <WorkspacesPicker useFetchWorkspaces={workspaceFetcher} />
      </IfFeatureEnabled>
      <FlexContainer className={styles.sidebar__menuItems} direction="column" justifyContent="space-between">
        <MenuContent data-testid="navMainItems">
          <NavItem
            label={<FormattedMessage id="sidebar.connections" />}
            icon="connection"
            to={RoutePaths.Connections}
            testId="connectionsLink"
          />
          <NavItem
            label={<FormattedMessage id="sidebar.sources" />}
            icon="source"
            to={RoutePaths.Source}
            testId="sourcesLink"
          />
          <NavItem
            label={<FormattedMessage id="sidebar.destinations" />}
            icon="destination"
            testId="destinationsLink"
            to={RoutePaths.Destination}
          />
          <NavItem
            label={<FormattedMessage id="sidebar.builder" />}
            icon="wrench"
            testId="builderLink"
            to={RoutePaths.ConnectorBuilder}
            withBadge="beta"
          />
          <IfFeatureEnabled feature={FeatureItem.Billing}>
            <NavItem
              icon="credits"
              label={<FormattedMessage id="sidebar.billing" />}
              to={CloudRoutes.Billing}
              testId="creditsButton"
            />
          </IfFeatureEnabled>
          <NavItem
            label={<FormattedMessage id="sidebar.settings" />}
            icon="gear"
            to={RoutePaths.Settings}
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
                  href: RoutePaths.Settings,
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
              label={user.name}
            />
          )}
        </MenuContent>
      </FlexContainer>
    </nav>
  );
};
