/* eslint sort-keys: "error" */
/**
 * Experiments are short-term flags for A/B testing or staged rollouts of features.
 *
 * When adding a new feature flag in LaunchDarkly to consume in code you'll need to make
 * sure to update the typing here.
 */

export interface Experiments {
  "authPage.keycloak": boolean;
  "authPage.rightSideUrl": string | undefined;
  "authPage.signup.hideCompanyName": boolean;
  "authPage.signup.hideName": boolean;
  "billing.early-sync-enabled": boolean;
  "billing.autoRecharge": boolean;
  "connection.clearNotReset": boolean;
  "connection.columnSelection": boolean;
  "connection.simplifiedCreation": boolean;
  "connection.onboarding.destinations": string;
  "connection.onboarding.sources": string;
  "connection.streamCentricUI.errorMultiplier": number;
  "connection.streamCentricUI.lateMultiplier": number;
  "connection.streamCentricUI.v2": boolean;
  "connection.streamCentricUI.historicalOverview": boolean;
  "connector.airbyteCloudIpAddresses": string;
  "connector.suggestedSourceConnectors": string;
  "connector.suggestedDestinationConnectors": string;
  "platform.auto-backfill-on-new-columns": boolean;
  "platform.activate-refreshes": boolean;
  "settings.breakingChangeNotifications": boolean;
  "settings.token-management-ui": boolean;
  "settings.showAdvancedSettings": boolean;
  "upcomingFeaturesPage.url": string;
}
