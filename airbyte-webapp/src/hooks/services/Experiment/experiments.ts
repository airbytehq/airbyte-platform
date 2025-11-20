/* eslint sort-keys: "error" */
/**
 * Experiments are short-term flags for A/B testing or staged rollouts of features.
 *
 * When adding a new feature flag in LaunchDarkly to consume in code you'll need to make
 * sure to update the typing here.
 */

export interface Experiments {
  asyncSchemaDiscovery: boolean;
  "authPage.embedded.rightSideUrl": string | undefined;
  "authPage.rightSideUrl": string | undefined;
  "billing.early-sync-enabled": boolean;
  "connection.allowToSupportAllSyncModes": boolean;
  "connection.columnSelection": boolean;
  "connection.onboarding.destinations": string;
  "connection.onboarding.sources": string;
  "connection.rateLimitedUI": boolean;
  "connections.connectionsStatusesEnabled": boolean;
  "connector.agentAssistedSetup": boolean;
  "connector.airbyteCloudIpAddressesByDataplane": Record<string, string[]>;
  "connector.allowSavingWithoutTesting": boolean;
  "connector.suggestedDestinationConnectors": string;
  "connector.suggestedSourceConnectors": string;
  "connector.updatedSetupUx": boolean;
  "connectorBuilder.aiAssist.enabled": boolean;
  "connectorBuilder.customComponents": boolean;
  "connectorBuilder.contributeEditsToMarketplace": boolean;
  "connectorBuilder.declarativeOauth": boolean;
  "connectorBuilder.dynamicStreams": boolean;
  "connectorBuilder.generateConnectorFromParams": boolean;
  "embedded.operatorOnboarding.destinations": string;
  "embedded.operatorOnboarding": boolean;
  "embedded.templateCreateButton": boolean;
  "embedded.useSonarServer": boolean;
  "onboarding.surveyEnabled": boolean;
  "organization.workerUsagePage": boolean;
  "platform.allow-config-template-endpoints": boolean;
  "platform.llm-sync-job-failure-explanation": boolean;
  "platform.use-runtime-secret-persistence": boolean;
  "platform.use-verified-domains-for-sso-activate": boolean;
  productLimitsUI: boolean;
  "settings.breakingChangeNotifications": boolean;
  "settings.domainVerification": boolean;
  "settings.downloadDiagnostics": boolean;
  "settings.showAdvancedSettings": boolean;
  "settings.ssoConfigValidation": boolean;
}

export const defaultExperimentValues: Experiments = {
  asyncSchemaDiscovery: true,
  "authPage.embedded.rightSideUrl": undefined,
  "authPage.rightSideUrl": undefined,
  "billing.early-sync-enabled": false,
  "connection.allowToSupportAllSyncModes": false,
  "connection.columnSelection": true,
  "connection.onboarding.destinations": "",
  "connection.onboarding.sources": "",
  "connection.rateLimitedUI": false,
  "connections.connectionsStatusesEnabled": false,
  "connector.agentAssistedSetup": false,
  "connector.airbyteCloudIpAddressesByDataplane": {
    auto: [
      "34.106.109.131",
      "34.106.196.165",
      "34.106.60.246",
      "34.106.229.69",
      "34.106.127.139",
      "34.106.218.58",
      "34.106.115.240",
      "34.106.225.141",
      "13.37.4.46",
      "13.37.142.60",
      "35.181.124.238",
      "34.33.7.0/29",
    ],
  },
  "connector.allowSavingWithoutTesting": false,
  "connector.suggestedDestinationConnectors": "",
  "connector.suggestedSourceConnectors": "",
  "connector.updatedSetupUx": false,
  "connectorBuilder.aiAssist.enabled": false,
  "connectorBuilder.contributeEditsToMarketplace": true,
  "connectorBuilder.customComponents": false,
  "connectorBuilder.declarativeOauth": true,
  "connectorBuilder.dynamicStreams": false,
  "connectorBuilder.generateConnectorFromParams": false,
  "embedded.operatorOnboarding": false,
  "embedded.operatorOnboarding.destinations": "ConnectorIds.Destinations.S3",
  "embedded.templateCreateButton": false,
  "embedded.useSonarServer": false,
  "onboarding.surveyEnabled": false,
  "organization.workerUsagePage": false,
  "platform.allow-config-template-endpoints": false,
  "platform.llm-sync-job-failure-explanation": false,
  "platform.use-runtime-secret-persistence": false,
  "platform.use-verified-domains-for-sso-activate": false,
  productLimitsUI: false,
  "settings.breakingChangeNotifications": false,
  "settings.domainVerification": false,
  "settings.downloadDiagnostics": false,
  "settings.showAdvancedSettings": false,
  "settings.ssoConfigValidation": false,
};
