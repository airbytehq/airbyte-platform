import { SettingsRoutePaths } from "pages/routePaths";

export const CloudSettingsRoutePaths = {
  Notifications: SettingsRoutePaths.Notifications,
  Account: SettingsRoutePaths.Account,
  Advanced: SettingsRoutePaths.Advanced,
  Source: SettingsRoutePaths.Source,
  Destination: SettingsRoutePaths.Destination,
  DataResidency: SettingsRoutePaths.DataResidency,
  Workspace: SettingsRoutePaths.Workspace,
  Organization: SettingsRoutePaths.Organization,
  AccessManagement: SettingsRoutePaths.AccessManagement,
  DbtCloud: "dbt-cloud",
  Applications: SettingsRoutePaths.Applications,
} as const;
