import React, { Suspense } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage, MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import {
  SettingsButton,
  SettingsLink,
  SettingsNavigation,
  SettingsNavigationBlock,
} from "components/settings/SettingsNavigation";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";

import { useCurrentOrganizationInfo } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { isOsanoActive, showOsanoDrawer } from "core/utils/dataPrivacy";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";

import { CloudSettingsRoutePaths } from "./routePaths";

export const CloudSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration);
  const supportsDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);
  const isTokenManagementEnabled = useExperiment("settings.token-management-ui", false);
  const organization = useCurrentOrganizationInfo();
  const isSsoEnabled = organization?.sso;
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId: organization?.organizationId });

  return (
    <MainPageWithScroll
      headTitle={<HeadTitle titles={[{ id: "sidebar.settings" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="sidebar.settings" />
            </Heading>
          }
        />
      }
    >
      <FlexContainer direction="row" gap="2xl">
        <SettingsNavigation>
          <SettingsNavigationBlock title={formatMessage({ id: "settings.userSettings" })}>
            <SettingsLink name={formatMessage({ id: "settings.account" })} to={CloudSettingsRoutePaths.Account} />
            {isTokenManagementEnabled && (
              <SettingsLink
                name={formatMessage({ id: "settings.applications" })}
                to={CloudSettingsRoutePaths.Applications}
              />
            )}
            {isOsanoActive() && (
              <SettingsButton
                onClick={() => showOsanoDrawer()}
                name={formatMessage({ id: "settings.cookiePreferences" })}
              />
            )}
          </SettingsNavigationBlock>
          <SettingsNavigationBlock title={formatMessage({ id: "settings.workspaceSettings" })}>
            <SettingsLink
              name={formatMessage({ id: "settings.generalSettings" })}
              to={CloudSettingsRoutePaths.Workspace}
            />
            {supportsDataResidency && (
              <SettingsLink
                name={formatMessage({ id: "settings.dataResidency" })}
                to={CloudSettingsRoutePaths.DataResidency}
              />
            )}
            <SettingsLink name={formatMessage({ id: "tables.sources" })} to={CloudSettingsRoutePaths.Source} />
            <SettingsLink
              name={formatMessage({ id: "tables.destinations" })}
              to={CloudSettingsRoutePaths.Destination}
            />
            <SettingsLink
              name={formatMessage({ id: "settings.accessManagement" })}
              to={CloudSettingsRoutePaths.AccessManagement}
            />
            <SettingsLink
              name={formatMessage({ id: "settings.notifications" })}
              to={CloudSettingsRoutePaths.Notifications}
            />
          </SettingsNavigationBlock>
          {organization && canViewOrgSettings && (
            <SettingsNavigationBlock title={formatMessage({ id: "settings.organizationSettings" })}>
              <SettingsLink
                name={formatMessage({ id: "settings.generalSettings" })}
                to={CloudSettingsRoutePaths.Organization}
              />
              {isSsoEnabled && (
                <SettingsLink
                  name={formatMessage({ id: "settings.accessManagement" })}
                  to={`${CloudSettingsRoutePaths.Organization}/${CloudSettingsRoutePaths.AccessManagement}`}
                />
              )}
            </SettingsNavigationBlock>
          )}
          {supportsCloudDbtIntegration && (
            <SettingsNavigationBlock title={formatMessage({ id: "settings.integrationSettings" })}>
              <SettingsLink
                name={formatMessage({ id: "settings.integrationSettings.dbtCloudSettings" })}
                to={CloudSettingsRoutePaths.DbtCloud}
              />
            </SettingsNavigationBlock>
          )}
        </SettingsNavigation>
        <FlexItem grow>
          <Suspense fallback={<LoadingPage />}>
            <Outlet />
          </Suspense>
        </FlexItem>
      </FlexContainer>
    </MainPageWithScroll>
  );
};

export default CloudSettingsPage;
