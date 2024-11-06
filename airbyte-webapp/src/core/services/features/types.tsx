/**
 * FeatureItems are for permanent flags to differentiate features between environments (e.g. Cloud vs. OSS),
 * workspaces, specific user groups, etc.
 */

export enum FeatureItem {
  AllowAllRBACRoles = "ALLOW_ALL_RBAC_ROLES",
  AllowAutoDetectSchema = "ALLOW_AUTO_DETECT_SCHEMA",
  AllowUploadCustomImage = "ALLOW_UPLOAD_CUSTOM_IMAGE",
  AllowDBTCloudIntegration = "ALLOW_DBT_CLOUD_INTEGRATION",
  AllowUpdateConnectors = "ALLOW_UPDATE_CONNECTORS",
  AllowOAuthConnector = "ALLOW_OAUTH_CONNECTOR",
  AllowChangeDataGeographies = "ALLOW_CHANGE_DATA_GEOGRAPHIES",
  AllowSyncSubOneHourCronExpressions = "ALLOW_SYNC_SUB_ONE_HOUR_CRON_EXPRESSIONS",
  Billing = "BILLING",
  CloudForTeamsBranding = "CLOUD_FOR_TEAMS_BRANDING",
  ConnectionHistoryGraphs = "CONNECTION_HISTORY_GRAPHS",
  ConnectorBreakingChangeDeadlines = "CONNECTOR_BREAKING_CHANGE_DEADLINES",
  DiagnosticsExport = "DIAGNOSTICS_EXPORT",
  DisplayOrganizationUsers = "DISPLAY_ORGANIZATION_USERS",
  EmailNotifications = "EMAIL_NOTIFICATIONS",
  EnterpriseBranding = "ENTERPRISE_BRANDING",
  EnterpriseLicenseChecking = "ENTERPRISE_LICENSE_CHECKING",
  ExternalInvitations = "EXTERNAL_INVITATIONS",
  FieldHashing = "FIELD_HASHING",
  IndicateGuestUsers = "INDICATE_GUEST_USERS",
  MultiWorkspaceUI = "MULTI_WORKSPACE_UI",
  RBAC = "RBAC",
  RestrictAdminInForeignWorkspace = "RESTRICT_ADMIN_IN_FOREIGN_WORKSPACE",
  ShowAdminWarningInWorkspace = "SHOW_ADMIN_WARNING_IN_WORKSPACE",
  ShowInviteUsersHint = "SHOW_INVITE_USERS_HINT",
}

export type FeatureSet = Partial<Record<FeatureItem, boolean>>;
