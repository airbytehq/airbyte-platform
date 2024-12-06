import { SettingsRoutePaths } from "pages/routePaths";

export const CloudSettingsRoutePaths = {
  Usage: "usage",
  Billing: "billing",
  OrganizationUsage: "organization-usage",
  Notifications: SettingsRoutePaths.Notifications,
  Account: SettingsRoutePaths.Account,
  Advanced: SettingsRoutePaths.Advanced,
  Source: SettingsRoutePaths.Source,
  Destination: SettingsRoutePaths.Destination,
  DataResidency: SettingsRoutePaths.DataResidency,
  Workspace: SettingsRoutePaths.Workspace,
  Organization: SettingsRoutePaths.Organization,
  OrganizationMembers: SettingsRoutePaths.OrganizationMembers,
  AccessManagement: SettingsRoutePaths.AccessManagement,
  DbtCloud: "dbt-cloud",
  Applications: SettingsRoutePaths.Applications,
} as const;
