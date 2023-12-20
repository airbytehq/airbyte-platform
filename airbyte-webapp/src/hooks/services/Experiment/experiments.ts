/* eslint sort-keys: "error" */
/**
 * Experiments are short-term flags for A/B testing or staged rollouts of features.
 *
 * When adding a new feature flag in LaunchDarkly to consume in code you'll need to make
 * sure to update the typing here.
 */

export interface Experiments {
  "authPage.rightSideUrl": string | undefined;
  "authPage.signup.hideCompanyName": boolean;
  "authPage.signup.hideName": boolean;
  "autopropagation.enabled": boolean;
  "billing.early-sync-enabled": boolean;
  "billing.newTrialPolicy": boolean;
  "billing.autoRecharge": boolean;
  "connections.summaryView": boolean;
  "connection.autoDetectSchemaChanges": boolean;
  "connection.columnSelection": boolean;
  "connection.onboarding.destinations": string;
  "connection.onboarding.sources": string;
  "connection.streamCentricUI.errorMultiplier": number;
  "connection.streamCentricUI.lateMultiplier": number;
  "connection.streamCentricUI.numberOfLogsToLoad": number;
  "connection.streamCentricUI.v2": boolean;
  "connection.streamCentricUI.historicalOverview": boolean;
  "connection.syncCatalog.simplifiedCatalogRow": boolean;
  "connector.airbyteCloudIpAddresses": string;
  "connector.allowlistIpBanner": boolean;
  "connector.orderOverwrite": Record<string, number>;
  "connector.shortSetupGuides": boolean;
  "connector.showRequestSchemabutton": boolean;
  "connector.suggestedSourceConnectors": string;
  "connector.suggestedDestinationConnectors": string;
  "onboarding.speedyConnection": boolean;
  "settings.emailNotifications": boolean;
  "settings.breakingChangeNotifications": boolean;
  "upcomingFeaturesPage.url": string;
  "settings.accessManagement": boolean;
  "settings.token-management-ui": boolean;
  "catalog.skipInitialCalculation": boolean;
}
