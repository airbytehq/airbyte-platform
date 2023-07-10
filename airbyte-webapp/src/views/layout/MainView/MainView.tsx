import { faRocket } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { FormattedMessage } from "react-intl";

import { LoadingPage } from "components";
import { Version } from "components/common/Version";
import { FlexContainer } from "components/ui/Flex";

import { useConfig } from "config";
import { links } from "core/utils/links";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { RoutePaths } from "pages/routePaths";
import { ResourceNotFoundErrorBoundary } from "views/common/ResourceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";

import styles from "./MainView.module.scss";
import { AirbyteHomeLink } from "../SideBar/AirbyteHomeLink";
import { MenuContent } from "../SideBar/components/MenuContent";
import { NavItem } from "../SideBar/components/NavItem";
import { ResourcesDropdown } from "../SideBar/components/ResourcesDropdown";
import SettingsIcon from "../SideBar/components/SettingsIcon";
import { MainNavItems } from "../SideBar/MainNavItems";
import { SideBar } from "../SideBar/SideBar";

const MainView: React.FC<React.PropsWithChildren<unknown>> = (props) => {
  const { version } = useConfig();
  const { trackError } = useAppMonitoringService();
  const { hasNewVersions } = useGetConnectorsOutOfDate();

  return (
    <FlexContainer className={styles.mainViewContainer} gap="none">
      <SideBar>
        <AirbyteHomeLink />
        <MenuContent>
          <MainNavItems />
          <MenuContent>
            <NavItem
              as="a"
              to={links.updateLink}
              icon={<FontAwesomeIcon icon={faRocket} />}
              label={<FormattedMessage id="sidebar.update" />}
              testId="updateLink"
            />
            <ResourcesDropdown />
            <NavItem
              label={<FormattedMessage id="sidebar.settings" />}
              icon={<SettingsIcon />}
              to={RoutePaths.Settings}
              withNotification={hasNewVersions}
            />
            {version && <Version primary />}
          </MenuContent>
        </MenuContent>
      </SideBar>
      <div className={styles.content}>
        <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />} trackError={trackError}>
          <React.Suspense fallback={<LoadingPage />}>{props.children}</React.Suspense>
        </ResourceNotFoundErrorBoundary>
      </div>
    </FlexContainer>
  );
};

export default MainView;
