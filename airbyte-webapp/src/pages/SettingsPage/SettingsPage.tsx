import React, { Suspense } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage, MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "components/settings/SettingsNavigation";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";

import { useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { SettingsRoutePaths } from "pages/routePaths";

export const SettingsPage: React.FC = () => {
  const { organizationId, workspaceId } = useCurrentWorkspace();
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);
  const apiTokenManagement = false;
  // const apiTokenManagement = useFeature(FeatureItem.APITokenManagement);
  const canViewWorkspaceSettings = useIntent("ViewWorkspaceSettings", { workspaceId });
  const canViewOrganizationSettings = useIntent("ViewOrganizationSettings", { organizationId });
  const { formatMessage } = useIntl();

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
            <SettingsLink
              iconType="user"
              name={formatMessage({ id: "settings.account" })}
              to={SettingsRoutePaths.Account}
            />
          </SettingsNavigationBlock>
          {apiTokenManagement && (
            <SettingsLink
              iconType="grid"
              name={formatMessage({ id: "settings.applications" })}
              to={SettingsRoutePaths.Applications}
            />
          )}
          {canViewWorkspaceSettings && (
            <SettingsNavigationBlock title={formatMessage({ id: "settings.workspaceSettings" })}>
              {multiWorkspaceUI && (
                <SettingsLink
                  iconType="gear"
                  name={formatMessage({ id: "settings.generalSettings" })}
                  to={SettingsRoutePaths.Workspace}
                />
              )}
              {!multiWorkspaceUI && (
                <>
                  <SettingsLink
                    iconType="source"
                    count={countNewSourceVersion}
                    name={formatMessage({ id: "tables.sources" })}
                    to={SettingsRoutePaths.Source}
                  />

                  <SettingsLink
                    iconType="destination"
                    count={countNewDestinationVersion}
                    name={formatMessage({ id: "tables.destinations" })}
                    to={SettingsRoutePaths.Destination}
                  />
                </>
              )}
              <SettingsLink
                iconType="bell"
                name={formatMessage({ id: "settings.notifications" })}
                to={SettingsRoutePaths.Notifications}
              />
              <SettingsLink
                iconType="chart"
                name={formatMessage({ id: "settings.metrics" })}
                to={SettingsRoutePaths.Metrics}
              />
              {multiWorkspaceUI && isAccessManagementEnabled && (
                <SettingsLink
                  iconType="community"
                  name={formatMessage({ id: "settings.accessManagement" })}
                  to={`${SettingsRoutePaths.Workspace}/${SettingsRoutePaths.AccessManagement}`}
                />
              )}
            </SettingsNavigationBlock>
          )}
          {multiWorkspaceUI && organizationId && canViewOrganizationSettings && (
            <SettingsNavigationBlock title={formatMessage({ id: "settings.organizationSettings" })}>
              <SettingsLink
                iconType="gear"
                name={formatMessage({ id: "settings.generalSettings" })}
                to={SettingsRoutePaths.Organization}
              />
              {isAccessManagementEnabled && (
                <SettingsLink
                  iconType="community"
                  name={formatMessage({ id: "settings.accessManagement" })}
                  to={`${SettingsRoutePaths.Organization}/${SettingsRoutePaths.AccessManagement}`}
                />
              )}
            </SettingsNavigationBlock>
          )}
          {multiWorkspaceUI && canViewWorkspaceSettings && (
            <SettingsNavigationBlock title={formatMessage({ id: "settings.instanceSettings" })}>
              <SettingsLink
                iconType="source"
                count={countNewSourceVersion}
                name={formatMessage({ id: "tables.sources" })}
                to={SettingsRoutePaths.Source}
              />
              <SettingsLink
                iconType="destination"
                count={countNewDestinationVersion}
                name={formatMessage({ id: "tables.destinations" })}
                to={SettingsRoutePaths.Destination}
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
