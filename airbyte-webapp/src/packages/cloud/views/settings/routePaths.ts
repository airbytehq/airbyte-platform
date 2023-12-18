import { SettingsRoutePaths } from "pages/routePaths";

export const CloudSettingsRoutePaths = {
  Notifications: SettingsRoutePaths.Notifications,
  Account: SettingsRoutePaths.Account,
  Source: SettingsRoutePaths.Source,
  Destination: SettingsRoutePaths.Destination,
  DataResidency: SettingsRoutePaths.DataResidency,
  Workspace: SettingsRoutePaths.Workspace,
  Organization: SettingsRoutePaths.Organization,
  AccessManagement: SettingsRoutePaths.AccessManagement,
  DbtCloud: "dbt-cloud",
  Applications: SettingsRoutePaths.Applications,
} as const;
