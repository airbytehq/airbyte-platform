/**
 * FeatureItems are for permanent flags to differentiate features between environments (e.g. Cloud vs. OSS),
 * workspaces, specific user groups, etc.
 */

export enum FeatureItem {
  AICopilot = "AI_COPILOT",
  AllowAllRBACRoles = "ALLOW_ALL_RBAC_ROLES", // corresponds to the feature-rbac-roles entitlement
  AllowAutoDetectSchema = "ALLOW_AUTO_DETECT_SCHEMA",
  AllowUploadCustomImage = "ALLOW_UPLOAD_CUSTOM_IMAGE",
  AllowUpdateSSOConfig = "ALLOW_UPDATE_SSO_CONFIG", // corresponds to the feature-sso entitlement
  AllowUpdateConnectors = "ALLOW_UPDATE_CONNECTORS",
  AllowChangeDataplanes = "ALLOW_CHANGE_DATAPLANES",
  AllowDBTCloudIntegration = "ALLOW_DBT_CLOUD_INTEGRATION",
  CloudForTeamsBranding = "CLOUD_FOR_TEAMS_BRANDING",
  CloudForTeamsUpsell = "CLOUD_FOR_TEAMS_UPSELLING",
  ConnectionHistoryGraphs = "CONNECTION_HISTORY_GRAPHS",
  ConnectorBreakingChangeDeadlines = "CONNECTOR_BREAKING_CHANGE_DEADLINES",
  ConnectorResourceAllocation = "CONNECTOR_RESOURCE_ALLOCATION",
  DiagnosticsExport = "DIAGNOSTICS_EXPORT",
  DisplayOrganizationUsers = "DISPLAY_ORGANIZATION_USERS", // corresponds to the feature-fe-display-organization-users entitlement
  EmailNotifications = "EMAIL_NOTIFICATIONS",
  EnterpriseBranding = "ENTERPRISE_BRANDING",
  EnterpriseUpsell = "ENTERPRISE_UPSELL",
  EnterpriseLicenseChecking = "ENTERPRISE_LICENSE_CHECKING",
  ExternalInvitations = "EXTERNAL_INVITATIONS",
  IndicateGuestUsers = "INDICATE_GUEST_USERS", // corresponds to the feature-fe-indicate-guest-users entitlement
  MappingsUI = "MAPPINGS_UI", // corresponds to the feature-mappers entitlement, which also covers the connection.mappingsUI FF referenced here https://github.com/airbytehq/airbyte-platform-internal/blob/32a4284748a6881945a5a519c6cbc626e851399a/oss/airbyte-webapp/src/hooks/services/Experiment/experiments.ts#L16
  CreateMultipleWorkspaces = "CREATE_MULTIPLE_WORKSPACES_V2", // corresponds to the feature-multiple-workspaces entitlement
  OrganizationUI = "ORGANIZATION_UI",
  OrganizationConnectorSettings = "ORGANIZATION_CONNECTOR_SETTINGS", // show sources and destinations in organization settings
  RBAC = "RBAC",
  RestrictAdminInForeignWorkspace = "RESTRICT_ADMIN_IN_FOREIGN_WORKSPACE",
  ShowAdminWarningInWorkspace = "SHOW_ADMIN_WARNING_IN_WORKSPACE",
  ShowInviteUsersHint = "SHOW_INVITE_USERS_HINT",
  ShowWorkspacePicker = "SHOW_WORKSPACE_PICKER",
}

export type FeatureSet = Partial<Record<FeatureItem, boolean>>;
