/**
 * FeatureItems are for permanent flags to differentiate features between environments (e.g. Cloud vs. OSS),
 * workspaces, specific user groups, etc.
 */

export enum FeatureItem {
  AllowAutoDetectSchema = "ALLOW_AUTO_DETECT_SCHEMA",
  AllowUploadCustomImage = "ALLOW_UPLOAD_CUSTOM_IMAGE",
  AllowCustomDBT = "ALLOW_CUSTOM_DBT",
  AllowDBTCloudIntegration = "ALLOW_DBT_CLOUD_INTEGRATION",
  AllowUpdateConnectors = "ALLOW_UPDATE_CONNECTORS",
  AllowOAuthConnector = "ALLOW_OAUTH_CONNECTOR",
  AllowChangeDataGeographies = "ALLOW_CHANGE_DATA_GEOGRAPHIES",
  AllowSyncSubOneHourCronExpressions = "ALLOW_SYNC_SUB_ONE_HOUR_CRON_EXPRESSIONS",
  APITokenManagement = "API_TOKEN_MANAGEMENT",
  ShowAdminWarningInWorkspace = "SHOW_ADMIN_WARNING_IN_WORKSPACE",
  KeycloakAuthentication = "KEYCLOAK_AUTHENTICATION",
  EmailNotifications = "EMAIL_NOTIFICATIONS",
  EnterpriseBranding = "ENTERPRISE_BRANDING",
  ShowInviteUsersHint = "SHOW_INVITE_USERS_HINT",
  RBAC = "RBAC",
  RestrictAdminInForeignWorkspace = "RESTRICT_ADMIN_IN_FOREIGN_WORKSPACE",
  ConnectorBreakingChangeDeadlines = "CONNECTOR_BREAKING_CHANGE_DEADLINES",
  MultiWorkspaceUI = "MULTI_WORKSPACE_UI",
}

export type FeatureSet = Partial<Record<FeatureItem, boolean>>;
