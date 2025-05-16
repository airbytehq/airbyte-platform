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
  Workspace: SettingsRoutePaths.Workspace,
  WorkspaceMembers: SettingsRoutePaths.WorkspaceMembers,
  Organization: SettingsRoutePaths.Organization,
  OrganizationMembers: SettingsRoutePaths.OrganizationMembers,
  DbtCloud: "dbt-cloud",
  Applications: SettingsRoutePaths.Applications,
  Embedded: "embedded",
} as const;
