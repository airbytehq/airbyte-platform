import { FeatureItem } from "./types";

export const defaultOssFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowSyncFrequencyUnderOneHour,
  FeatureItem.AllowUpdateConnectors,
  FeatureItem.AllowUploadCustomImage,
  FeatureItem.EnterpriseUpsell,
];

export const defaultEnterpriseFeatures = [
  ...defaultOssFeatures,
  FeatureItem.AllowAllRBACRoles,
  FeatureItem.AllowChangeDataplanes,
  FeatureItem.ConnectionHistoryGraphs,
  FeatureItem.ConnectorResourceAllocation,
  FeatureItem.DiagnosticsExport,
  FeatureItem.DisplayOrganizationUsers,
  FeatureItem.EnterpriseBranding,
  FeatureItem.EnterpriseLicenseChecking,
  FeatureItem.IndicateGuestUsers,
  FeatureItem.MappingsUI,
  FeatureItem.CreateMultipleWorkspaces,
  FeatureItem.OrganizationUI,
  FeatureItem.OrganizationConnectorSettings,
  FeatureItem.RBAC,
  FeatureItem.ShowWorkspacePicker,
];

export const defaultCloudFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowDBTCloudIntegration,
  FeatureItem.CloudForTeamsUpsell,
  FeatureItem.ConnectionHistoryGraphs,
  FeatureItem.ConnectorBreakingChangeDeadlines,
  FeatureItem.EmailNotifications,
  FeatureItem.ExternalInvitations,
  FeatureItem.OrganizationUI,
  FeatureItem.RBAC,
  FeatureItem.RestrictAdminInForeignWorkspace,
  FeatureItem.ShowInviteUsersHint,
  FeatureItem.DisplayOrganizationUsers,
  FeatureItem.ShowWorkspacePicker,
];
