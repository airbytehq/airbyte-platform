import { FeatureItem } from "./types";

export const defaultOssFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowCustomDBT,
  FeatureItem.AllowUpdateConnectors,
  FeatureItem.AllowUploadCustomImage,
  FeatureItem.AllowSyncSubOneHourCronExpressions,
];

export const defaultEnterpriseFeatures = [
  ...defaultOssFeatures,
  FeatureItem.MultiWorkspaceUI,
  FeatureItem.RBAC,
  FeatureItem.EnterpriseBranding,
  FeatureItem.APITokenManagement,
  FeatureItem.ConnectionHistoryGraphs,
];

export const defaultCloudFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowOAuthConnector,
  FeatureItem.AllowChangeDataGeographies,
  FeatureItem.AllowDBTCloudIntegration,
  FeatureItem.Billing,
  FeatureItem.EmailNotifications,
  FeatureItem.ShowInviteUsersHint,
  FeatureItem.MultiWorkspaceUI,
  FeatureItem.RestrictAdminInForeignWorkspace,
  FeatureItem.ConnectorBreakingChangeDeadlines,
  FeatureItem.RBAC,
];
