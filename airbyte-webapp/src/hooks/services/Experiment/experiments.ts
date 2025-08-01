/* eslint sort-keys: "error" */
/**
 * Experiments are short-term flags for A/B testing or staged rollouts of features.
 *
 * When adding a new feature flag in LaunchDarkly to consume in code you'll need to make
 * sure to update the typing here.
 */

export interface Experiments {
  "authPage.embedded.rightSideUrl": string | undefined;
  "authPage.rightSideUrl": string | undefined;
  "billing.early-sync-enabled": boolean;
  "connection.allowToSupportAllSyncModes": boolean;
  "connection.columnSelection": boolean;
  "connection.hashingUI": boolean;
  "connection.mappingsUI": boolean;
  "connection.onboarding.destinations": string;
  "connection.onboarding.sources": string;
  "connection.rateLimitedUI": boolean;
  "connections.connectionsStatusesEnabled": boolean;
  "connector.airbyteCloudIpAddressesByDataplane": Record<string, string[]>;
  "connector.suggestedSourceConnectors": string;
  "connector.suggestedDestinationConnectors": string;
  "connectorBuilder.aiAssist.enabled": boolean;
  "connectorBuilder.customComponents": boolean;
  "connectorBuilder.contributeEditsToMarketplace": boolean;
  "connectorBuilder.declarativeOauth": boolean;
  "connectorBuilder.dynamicStreams": boolean;
  "connectorBuilder.generateConnectorFromParams": boolean;
  "connectorBuilder.schemaForm": boolean;
  "embedded.operatorOnboarding.destinations": string;
  "embedded.operatorOnboarding": boolean;
  "embedded.templateCreateButton": boolean;
  "embedded.useSonarServer": boolean;
  "onboarding.surveyEnabled": boolean;
  "entitlements.showTeamsFeaturesWarnModal": boolean;
  "platform.allow-config-template-endpoints": boolean;
  "platform.llm-sync-job-failure-explanation": boolean;
  "platform.use-runtime-secret-persistence": boolean;
  productLimitsUI: boolean;
  "settings.breakingChangeNotifications": boolean;
  "settings.downloadDiagnostics": boolean;
  "settings.organizationRbacImprovements": boolean;
  "settings.showAdvancedSettings": boolean;
  "sidebar.showOrgPicker": boolean;
}

export const defaultExperimentValues: Experiments = {
  "authPage.embedded.rightSideUrl": undefined,
  "authPage.rightSideUrl": undefined,
  "billing.early-sync-enabled": false,
  "connection.allowToSupportAllSyncModes": false,
  "connection.columnSelection": true,
  "connection.hashingUI": true, // also requires FeatureItem.FieldHashing
  "connection.mappingsUI": true, // requires FeatureItem.MappingsUI to enable configuration
  "connection.onboarding.destinations": "",
  "connection.onboarding.sources": "",
  "connection.rateLimitedUI": false,
  "connections.connectionsStatusesEnabled": false,
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
  "connector.suggestedDestinationConnectors": "",
  "connector.suggestedSourceConnectors": "",
  "connectorBuilder.aiAssist.enabled": false,
  "connectorBuilder.contributeEditsToMarketplace": true,
  "connectorBuilder.customComponents": false,
  "connectorBuilder.declarativeOauth": true,
  "connectorBuilder.dynamicStreams": false,
  "connectorBuilder.generateConnectorFromParams": false,
  "connectorBuilder.schemaForm": false,
  "embedded.operatorOnboarding": false,
  "embedded.operatorOnboarding.destinations": "ConnectorIds.Destinations.S3",
  "embedded.templateCreateButton": false,
  "embedded.useSonarServer": false,
  "entitlements.showTeamsFeaturesWarnModal": false,
  "onboarding.surveyEnabled": false,
  "platform.allow-config-template-endpoints": false,
  "platform.llm-sync-job-failure-explanation": false,
  "platform.use-runtime-secret-persistence": false,
  productLimitsUI: false,
  "settings.breakingChangeNotifications": false,
  "settings.downloadDiagnostics": false,
  "settings.organizationRbacImprovements": false,
  "settings.showAdvancedSettings": false,
  "sidebar.showOrgPicker": false,
};
