import { FeatureItem } from "./types";

export const defaultOssFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowUpdateConnectors,
  FeatureItem.AllowUploadCustomImage,
  FeatureItem.AllowSyncSubOneHourCronExpressions,
];

export const defaultEnterpriseFeatures = [
  ...defaultOssFeatures,
  FeatureItem.AllowAllRBACRoles,
  FeatureItem.ConnectionHistoryGraphs,
  FeatureItem.DiagnosticsExport,
  FeatureItem.DisplayOrganizationUsers,
  FeatureItem.EnterpriseBranding,
  FeatureItem.EnterpriseLicenseChecking,
  FeatureItem.FieldHashing,
  FeatureItem.IndicateGuestUsers,
  FeatureItem.MultiWorkspaceUI,
  FeatureItem.RBAC,
];

export const defaultCloudFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowOAuthConnector,
  FeatureItem.AllowChangeDataGeographies,
  FeatureItem.AllowDBTCloudIntegration,
  FeatureItem.Billing,
  FeatureItem.ConnectionHistoryGraphs,
  FeatureItem.ConnectorBreakingChangeDeadlines,
  FeatureItem.EmailNotifications,
  FeatureItem.ExternalInvitations,
  FeatureItem.MultiWorkspaceUI,
  FeatureItem.RBAC,
  FeatureItem.RestrictAdminInForeignWorkspace,
  FeatureItem.ShowInviteUsersHint,
  FeatureItem.FieldHashing, // also goverened by connection.hashingUI experiment
];
