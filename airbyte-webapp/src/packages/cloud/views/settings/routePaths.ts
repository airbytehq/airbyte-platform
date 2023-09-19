import { SettingsRoutePaths } from "pages/routePaths";

export const CloudSettingsRoutePaths = {
  Configuration: SettingsRoutePaths.Configuration,
  Notifications: SettingsRoutePaths.Notifications,
  Account: SettingsRoutePaths.Account,
  Source: SettingsRoutePaths.Source,
  Destination: SettingsRoutePaths.Destination,
  DataResidency: SettingsRoutePaths.DataResidency,

  Workspace: "workspaces",
  AccessManagement: "access-management",
  DbtCloud: "dbt-cloud",
} as const;
