import { FeatureItem } from "./types";

export const defaultOssFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowSyncFrequencyUnderOneHour,
  FeatureItem.AllowUpdateConnectors,
  FeatureItem.AllowUploadCustomImage,
];

export const defaultCloudFeatures = [
  FeatureItem.AllowAutoDetectSchema,
  FeatureItem.AllowChangeDataplanes,
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
