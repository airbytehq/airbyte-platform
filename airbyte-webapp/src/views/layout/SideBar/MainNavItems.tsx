import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";

import { RoutePaths } from "pages/routePaths";

import { MenuContent } from "./components/MenuContent";
import { NavItem } from "./components/NavItem";
import styles from "./MainNavItems.module.scss";

export const MainNavItems: React.FC = () => {
  return (
    <MenuContent data-testid="navMainItems">
      <NavItem
        label={<FormattedMessage id="sidebar.connections" />}
        icon={<Icon type="connection" />}
        to={RoutePaths.Connections}
        testId="connectionsLink"
      />

      <NavItem
        label={<FormattedMessage id="sidebar.sources" />}
        icon={<Icon type="source" />}
        to={RoutePaths.Source}
        testId="sourcesLink"
      />

      <NavItem
        label={<FormattedMessage id="sidebar.destinations" />}
        icon={<Icon type="destination" />}
        testId="destinationsLink"
        to={RoutePaths.Destination}
      />
      <NavItem
        label={<FormattedMessage id="sidebar.builder" />}
        icon={<Icon type="wrench" />}
        testId="builderLink"
        to={RoutePaths.ConnectorBuilder}
        className={styles.beta}
        activeClassName={styles["beta--active"]}
      />
    </MenuContent>
  );
};
