import classNames from "classnames";
import { PropsWithChildren, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { matchPath, useLocation } from "react-router-dom";

import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { ThemeToggle } from "components/ui/ThemeToggle";
import { Tooltip } from "components/ui/Tooltip";
import { WorkspacesPicker } from "components/workspace/WorkspacesPicker";
import type { WorkspaceFetcher } from "components/workspace/WorkspacesPickerList";

import { useAuthService } from "core/services/auth";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";
import { useShowBillingPageV2 } from "packages/cloud/area/billing/utils/useShowBillingPage";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { ConnectorBuilderRoutePaths } from "pages/connectorBuilder/ConnectorBuilderRoutes";
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

const HIDDEN_SIDEBAR_PATHS = [
  `${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.ConnectorBuilder}/${ConnectorBuilderRoutePaths.Edit}`,
];

const BillingPageLink: React.FC = () => {
  const showBillingPageV2 = useShowBillingPageV2();
  const isBillingMigrationMaintenance = useExperiment("billing.migrationMaintenance");

  if (showBillingPageV2) {
    return null;
  }

  return (
    <Tooltip
      containerClassName={styles.sidebar__tooltip}
      placement="right"
      control={
        <NavItem
          icon="credits"
          disabled={isBillingMigrationMaintenance}
          label={<FormattedMessage id="sidebar.billing" />}
          to={CloudRoutes.Billing}
          testId="creditsButton"
        />
      }
      disabled={!isBillingMigrationMaintenance}
    >
      <FormattedMessage
        id="sidebar.billingMigrationMaintenance"
        values={{
          statusPage: (node: React.ReactNode) => (
            <ExternalLink href={links.statusLink} opensInNewTab>
              {node}
            </ExternalLink>
          ),
        }}
      />
    </Tooltip>
  );
};

export const SideBar: React.FC<PropsWithChildren<SideBarProps>> = ({
  workspaceFetcher,
  bottomSlot,
  settingHighlight,
}) => {
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

  return (
    <nav className={classNames(styles.sidebar, { [styles.hidden]: isHidden })}>
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
          />
          <IfFeatureEnabled feature={FeatureItem.Billing}>
            <BillingPageLink />
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
              label={username}
            />
          )}
        </MenuContent>
      </FlexContainer>
    </nav>
  );
};
