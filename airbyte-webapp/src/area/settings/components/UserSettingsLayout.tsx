import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { HeadTitle } from "components/HeadTitle";
import LoadingPage from "components/LoadingPage";

import {
  SettingsButton,
  SettingsLink,
  SettingsNavigation,
  SettingsNavigationBlock,
} from "area/settings/components/SettingsNavigation";
import { useAuthService } from "core/services/auth";
import { isOsanoActive, showOsanoDrawer } from "core/utils/dataPrivacy";
import { useExperiment } from "hooks/services/Experiment";
import { SettingsRoutePaths } from "pages/routePaths";

import { SettingsLayoutContent } from "./SettingsLayout";
import styles from "./UserSettingsLayout.module.scss";

export const UserSettingsLayout: React.FC<React.PropsWithChildren> = () => {
  const { formatMessage } = useIntl();
  const { applicationSupport } = useAuthService();
  const showAdvancedSettings = useExperiment("settings.showAdvancedSettings");

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.userSettings" }]} />
      <main className={styles.container}>
        <SettingsNavigation>
          <SettingsNavigationBlock title={formatMessage({ id: "settings.userSettings" })}>
            <SettingsLink
              iconType="user"
              name={formatMessage({ id: "settings.account" })}
              to={SettingsRoutePaths.Account}
            />
            {applicationSupport !== "none" && (
              <SettingsLink
                iconType="grid"
                name={formatMessage({ id: "settings.applications" })}
                to={SettingsRoutePaths.Applications}
              />
            )}
            {isOsanoActive() && (
              <SettingsButton
                iconType="parameters"
                onClick={() => showOsanoDrawer()}
                name={formatMessage({ id: "settings.cookiePreferences" })}
              />
            )}
            {showAdvancedSettings && (
              <SettingsLink
                iconType="gear"
                name={formatMessage({ id: "settings.advanced" })}
                to={SettingsRoutePaths.Advanced}
              />
            )}
          </SettingsNavigationBlock>
        </SettingsNavigation>
        <SettingsLayoutContent>
          <Suspense fallback={<LoadingPage />}>
            <Outlet />
          </Suspense>
        </SettingsLayoutContent>
      </main>
    </>
  );
};
