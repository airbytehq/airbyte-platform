/* eslint sort-keys: "error" */
/**
 * Experiments are short-term flags for A/B testing or staged rollouts of features.
 *
 * When adding a new feature flag in LaunchDarkly to consume in code you'll need to make
 * sure to update the typing here.
 */

export interface Experiments {
  "autopropagation.enabled": boolean;
  "connector.orderOverwrite": Record<string, number>;
  "connector.shortSetupGuides": boolean;
  "authPage.rightSideUrl": string | undefined;
  "authPage.signup.hideName": boolean;
  "authPage.signup.hideCompanyName": boolean;
  "onboarding.speedyConnection": boolean;
  "connection.onboarding.sources": string;
  "connection.onboarding.destinations": string;
  "connection.autoDetectSchemaChanges": boolean;
  "connection.columnSelection": boolean;
  "connection.streamCentricUI.v2": boolean;
  "connection.streamCentricUI.lateMultiplier": number;
  "connection.streamCentricUI.errorMultiplier": number;
  "connection.streamCentricUI.numberOfLogsToLoad": number;
  "connection.searchableJobLogs": boolean;
  "connector.showRequestSchemabutton": boolean;
  "connection.syncCatalog.simplifiedCatalogRow": boolean;
  "upcomingFeaturesPage.url": string;
  "billing.newTrialPolicy": boolean;
  "connector.allowlistIpBanner": boolean;
  "settings.emailNotifications": boolean;
  "connector.airbyteCloudIpAddresses": string;
}
