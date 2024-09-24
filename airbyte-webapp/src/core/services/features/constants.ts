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
  FeatureItem.DisplayOrganizationUsers,
  FeatureItem.EnterpriseBranding,
  FeatureItem.FieldHashing,
  FeatureItem.IndicateGuestUsers,
  FeatureItem.MultiWorkspaceUI,
  FeatureItem.RBAC,
  FeatureItem.SyncCatalogV2,
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
  FeatureItem.SyncCatalogV2,
  FeatureItem.FieldHashing, // also goverened by connection.hashingUI experiment
];
