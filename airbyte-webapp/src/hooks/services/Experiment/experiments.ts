/* eslint sort-keys: "error" */
/**
 * Experiments are short-term flags for A/B testing or staged rollouts of features.
 *
 * When adding a new feature flag in LaunchDarkly to consume in code you'll need to make
 * sure to update the typing here.
 */

export interface Experiments {
  "authPage.rightSideUrl": string | undefined;
  "billing.early-sync-enabled": boolean;
  "billing.autoRecharge": boolean;
  "connection.columnSelection": boolean;
  "connection.onboarding.destinations": string;
  "connection.onboarding.sources": string;
  "connection.rateLimitedUI": boolean;
  "connection.timeline": boolean;
  "connection.timeline.schemaUpdates": boolean;
  "connector.airbyteCloudIpAddresses": string;
  "connector.suggestedSourceConnectors": string;
  "connector.suggestedDestinationConnectors": string;
  "settings.breakingChangeNotifications": boolean;
  "settings.downloadDiagnostics": boolean;
  "settings.showAdvancedSettings": boolean;
  "upcomingFeaturesPage.url": string;
  "connection.syncCatalogV2": boolean;
  "connection.hashingUI": boolean;
  "connectorBuilder.contributeEditsToMarketplace": boolean;
  "connectorBuilder.aiAssist.enabled": boolean;
}

export const defaultExperimentValues: Experiments = {
  "authPage.rightSideUrl": undefined,
  "billing.autoRecharge": false,
  "billing.early-sync-enabled": false,
  "connection.columnSelection": true,
  "connection.hashingUI": true, // also requires FeatureItem.FieldHashing and FeatureItem.SyncCatalogV2
  "connection.onboarding.destinations": "",
  "connection.onboarding.sources": "",
  "connection.rateLimitedUI": false,
  "connection.syncCatalogV2": true, // also requires FeatureItem.SyncCatalogV2
  "connection.timeline": false,
  "connection.timeline.schemaUpdates": false,
  "connector.airbyteCloudIpAddresses":
    "34.106.109.131, 34.106.196.165, 34.106.60.246, 34.106.229.69, 34.106.127.139, 34.106.218.58, 34.106.115.240, 34.106.225.141, 13.37.4.46, 13.37.142.60, 35.181.124.238",
  "connector.suggestedDestinationConnectors": "",
  "connector.suggestedSourceConnectors": "",
  "connectorBuilder.aiAssist.enabled": false,
  "connectorBuilder.contributeEditsToMarketplace": false,
  "settings.breakingChangeNotifications": false,
  "settings.downloadDiagnostics": false,
  "settings.showAdvancedSettings": false,
  "upcomingFeaturesPage.url": "",
};
