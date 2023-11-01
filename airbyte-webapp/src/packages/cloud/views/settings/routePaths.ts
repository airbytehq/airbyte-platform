import { SettingsRoutePaths } from "pages/routePaths";

export const CloudSettingsRoutePaths = {
  Notifications: SettingsRoutePaths.Notifications,
  Account: SettingsRoutePaths.Account,
  Source: SettingsRoutePaths.Source,
  Destination: SettingsRoutePaths.Destination,
  DataResidency: SettingsRoutePaths.DataResidency,
  Workspace: SettingsRoutePaths.Workspace,
  Organization: SettingsRoutePaths.Organization,
  AccessManagement: "access-management",
  DbtCloud: "dbt-cloud",
} as const;
