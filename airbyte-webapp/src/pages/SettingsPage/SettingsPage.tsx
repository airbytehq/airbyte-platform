import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { useCurrentWorkspace } from "core/api";
import { useIntent } from "core/utils/rbac/intent";
import { useExperiment } from "hooks/services/Experiment";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { SettingsRoutePaths } from "pages/routePaths";
import { NotificationPage } from "pages/SettingsPage/pages/NotificationPage";
import { PageConfig, SettingsPageBase } from "pages/SettingsPage/SettingsPageBase";

import { GeneralOrganizationSettingsPage } from "./GeneralOrganizationSettingsPage";
import { GeneralWorkspaceSettingsPage } from "./GeneralWorkspaceSettingsPage";
import { OrganizationAccessManagementPage } from "./pages/AccessManagementPage/OrganizationAccessManagementPage";
import { WorkspaceAccessManagementPage } from "./pages/AccessManagementPage/WorkspaceAccessManagementPage";
import { AccountPage } from "./pages/AccountPage";
import { ConfigurationsPage } from "./pages/ConfigurationsPage";
import { DestinationsPage, SourcesPage } from "./pages/ConnectorsPage";
import { MetricsPage } from "./pages/MetricsPage";

export const SettingsPage: React.FC = () => {
  const { organizationId, workspaceId } = useCurrentWorkspace();
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();
  const newWorkspacesUI = useExperiment("workspaces.newWorkspacesUI", false);
  const isAccessManagementEnabled = useExperiment("settings.accessManagement", false);
  const canListWorkspaceUsers = useIntent("ListWorkspaceMembers", { workspaceId });
  const canListOrganizationUsers = useIntent("ListOrganizationMembers", { organizationId });

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
          ],
        },
        {
          category: <FormattedMessage id="settings.workspaceSettings" />,
          routes: [
            ...(newWorkspacesUI
              ? [
                  {
                    path: `${SettingsRoutePaths.Workspace}`,
                    name: <FormattedMessage id="settings.generalSettings" />,
                    component: GeneralWorkspaceSettingsPage,
                  },
                ]
              : []),
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
            {
              path: `${SettingsRoutePaths.Configuration}`,
              name: <FormattedMessage id="admin.configuration" />,
              component: ConfigurationsPage,
            },
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
            ...(newWorkspacesUI && isAccessManagementEnabled && canListWorkspaceUsers
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
        ...(newWorkspacesUI && organizationId && canListOrganizationUsers
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
      ],
    }),
    [
      canListOrganizationUsers,
      canListWorkspaceUsers,
      countNewDestinationVersion,
      countNewSourceVersion,
      isAccessManagementEnabled,
      newWorkspacesUI,
      organizationId,
    ]
  );

  return <SettingsPageBase pageConfig={pageConfig} />;
};
