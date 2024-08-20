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
  "connector.airbyteCloudIpAddresses": string;
  "connector.suggestedSourceConnectors": string;
  "connector.suggestedDestinationConnectors": string;
  "platform.auto-backfill-on-new-columns": boolean;
  "settings.breakingChangeNotifications": boolean;
  "settings.token-management-ui": boolean;
  "settings.showAdvancedSettings": boolean;
  "upcomingFeaturesPage.url": string;
  "connection.syncCatalogV2": boolean;
  "connectorBuilder.contributeToMarketplace": boolean;
  "billing.organizationBillingPage": boolean;
}
