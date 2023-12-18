import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { ApplicationSettingsView } from "packages/cloud/views/users/ApplicationSettingsView/ApplicationSettingsView";
import { SettingsRoutePaths } from "pages/routePaths";
import { NotificationPage } from "pages/SettingsPage/pages/NotificationPage";
import { PageConfig, SettingsPageBase } from "pages/SettingsPage/SettingsPageBase";

import { GeneralOrganizationSettingsPage } from "./GeneralOrganizationSettingsPage";
import { GeneralWorkspaceSettingsPage } from "./GeneralWorkspaceSettingsPage";
import { OrganizationAccessManagementPage } from "./pages/AccessManagementPage/OrganizationAccessManagementPage";
import { WorkspaceAccessManagementPage } from "./pages/AccessManagementPage/WorkspaceAccessManagementPage";
import { AccountPage } from "./pages/AccountPage";
import { DestinationsPage, SourcesPage } from "./pages/ConnectorsPage";
import { MetricsPage } from "./pages/MetricsPage";

export const SettingsPage: React.FC = () => {
  const { organizationId, workspaceId } = useCurrentWorkspace();
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);
  const apiTokenManagement = false;
  // const apiTokenManagement = useFeature(FeatureItem.APITokenManagement);
  const canViewWorkspaceSettings = useIntent("ViewWorkspaceSettings", { workspaceId });
  const canViewOrganizationSettings = useIntent("ViewOrganizationSettings", { organizationId });

  const pageConfig: PageConfig = useMemo<PageConfig>(
    () => ({
      menuConfig: [
        {
          category: <FormattedMessage id="settings.userSettings" />,
          routes: [
            {
              path: `${SettingsRoutePaths.Account}`,
              name: <FormattedMessage id="settings.account" />,
              component: AccountPage,
            },
            ...(apiTokenManagement
              ? [
                  {
                    path: `${SettingsRoutePaths.Applications}`,
                    name: <FormattedMessage id="settings.applications" />,
                    component: ApplicationSettingsView,
                  },
                ]
              : []),
          ],
        },
        ...(canViewWorkspaceSettings
          ? [
              {
                category: <FormattedMessage id="settings.workspaceSettings" />,
                routes: [
                  ...(multiWorkspaceUI
                    ? [
                        {
                          path: `${SettingsRoutePaths.Workspace}`,
                          name: <FormattedMessage id="settings.generalSettings" />,
                          component: GeneralWorkspaceSettingsPage,
                        },
                      ]
                    : []),
                  ...(!multiWorkspaceUI
                    ? [
                        {
                          path: `${SettingsRoutePaths.Source}`,
                          name: <FormattedMessage id="tables.sources" />,
                          indicatorCount: countNewSourceVersion,
                          component: SourcesPage,
                        },
                        {
                          path: `${SettingsRoutePaths.Destination}`,
                          name: <FormattedMessage id="tables.destinations" />,
                          indicatorCount: countNewDestinationVersion,
                          component: DestinationsPage,
                        },
                      ]
                    : []),
                  {
                    path: `${SettingsRoutePaths.Notifications}`,
                    name: <FormattedMessage id="settings.notifications" />,
                    component: NotificationPage,
                  },
                  {
                    path: `${SettingsRoutePaths.Metrics}`,
                    name: <FormattedMessage id="settings.metrics" />,
                    component: MetricsPage,
                  },
                  ...(multiWorkspaceUI && isAccessManagementEnabled
                    ? [
                        {
                          path: `${SettingsRoutePaths.Workspace}/${SettingsRoutePaths.AccessManagement}`,
                          name: <FormattedMessage id="settings.accessManagement" />,
                          component: WorkspaceAccessManagementPage,
                        },
                      ]
                    : []),
                ],
              },
            ]
          : []),
        ...(multiWorkspaceUI && organizationId && canViewOrganizationSettings
          ? [
              {
                category: <FormattedMessage id="settings.organizationSettings" />,
                routes: [
                  {
                    path: `${SettingsRoutePaths.Organization}`,
                    name: <FormattedMessage id="settings.generalSettings" />,
                    component: GeneralOrganizationSettingsPage,
                  },
                  ...(isAccessManagementEnabled
                    ? [
                        {
                          path: `${SettingsRoutePaths.Organization}/${SettingsRoutePaths.AccessManagement}`,
                          name: <FormattedMessage id="settings.accessManagement" />,
                          component: OrganizationAccessManagementPage,
                        },
                      ]
                    : []),
                ],
              },
            ]
          : []),
        ...(multiWorkspaceUI && canViewWorkspaceSettings
          ? [
              {
                category: <FormattedMessage id="settings.instanceSettings" />,
                routes: [
                  {
                    path: `${SettingsRoutePaths.Source}`,
                    name: <FormattedMessage id="tables.sources" />,
                    indicatorCount: countNewSourceVersion,
                    component: SourcesPage,
                  },
                  {
                    path: `${SettingsRoutePaths.Destination}`,
                    name: <FormattedMessage id="tables.destinations" />,
                    indicatorCount: countNewDestinationVersion,
                    component: DestinationsPage,
                  },
                ],
              },
            ]
          : []),
      ],
    }),
    [
      apiTokenManagement,
      canViewOrganizationSettings,
      canViewWorkspaceSettings,
      countNewDestinationVersion,
      countNewSourceVersion,
      isAccessManagementEnabled,
      multiWorkspaceUI,
      organizationId,
    ]
  );

  return <SettingsPageBase pageConfig={pageConfig} />;
};
