import { FeatureItem } from "./types";

export const defaultOssFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowOAuthConnector,
  FeatureItem.AllowUpdateConnectors,
  FeatureItem.AllowUploadCustomImage,
  FeatureItem.EnterpriseUpsell,
];

export const defaultEnterpriseFeatures = [
  ...defaultOssFeatures,
  FeatureItem.AllowAllRBACRoles,
  FeatureItem.AllowOAuthConnector,
  FeatureItem.ConnectionHistoryGraphs,
  FeatureItem.DiagnosticsExport,
  FeatureItem.DisplayOrganizationUsers,
  FeatureItem.EnterpriseBranding,
  FeatureItem.EnterpriseLicenseChecking,
  FeatureItem.FieldHashing,
  FeatureItem.IndicateGuestUsers,
  FeatureItem.MappingsUI, // Also governed by connection.mappingsUI experiment.  This flag indicates whether the user has the right level of product.  The experiment indicates whether the UI is "on" overall.
  FeatureItem.MultiWorkspaceUI,
  FeatureItem.RBAC,
];

export const defaultCloudFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowOAuthConnector,
  FeatureItem.AllowChangeDataGeographies,
  FeatureItem.AllowDBTCloudIntegration,
  FeatureItem.CloudForTeamsUpsell,
  FeatureItem.ConnectionHistoryGraphs,
  FeatureItem.ConnectorBreakingChangeDeadlines,
  FeatureItem.EmailNotifications,
  FeatureItem.ExternalInvitations,
  FeatureItem.MultiWorkspaceUI,
  FeatureItem.RBAC,
  FeatureItem.RestrictAdminInForeignWorkspace,
  FeatureItem.ShowInviteUsersHint,
  FeatureItem.FieldHashing, // also governed by connection.hashingUI experiment
];
