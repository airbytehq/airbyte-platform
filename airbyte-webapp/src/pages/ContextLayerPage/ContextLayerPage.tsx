import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { useCurrentOrganizationId } from "area/organization/utils";
import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { RoutePaths } from "pages/routePaths";

export const ContextLayerPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();

  return (
    <SettingsLayout titleId="cloud.contextLayer.navigation.title">
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "cloud.contextLayer.navigation.title" })}>
          <SettingsLink
            iconType="gear"
            name={formatMessage({ id: "sidebar.settings" })}
            to={`/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`}
          />
        </SettingsNavigationBlock>
      </SettingsNavigation>
      <SettingsLayoutContent>
        <Suspense fallback={<LoadingPage />}>
          <Outlet />
        </Suspense>
      </SettingsLayoutContent>
    </SettingsLayout>
  );
};
