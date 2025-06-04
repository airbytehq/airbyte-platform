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
  "connection.allowToSupportAllSyncModes": boolean;
  "connection.columnSelection": boolean;
  "connection.dataActivationUI": boolean;
  "connection.hashingUI": boolean;
  "connection.mappingsUI": boolean;
  "connection.onboarding.destinations": string;
  "connection.onboarding.sources": string;
  "connection.rateLimitedUI": boolean;
  "connections.connectionsStatusesEnabled": boolean;
  "connector.airbyteCloudIpAddresses": string;
  "connector.suggestedSourceConnectors": string;
  "connector.suggestedDestinationConnectors": string;
  "connectorBuilder.aiAssist.enabled": boolean;
  "connectorBuilder.customComponents": boolean;
  "connectorBuilder.contributeEditsToMarketplace": boolean;
  "connectorBuilder.declarativeOauth": boolean;
  "connectorBuilder.dynamicStreams": boolean;
  "connectorBuilder.generateConnectorFromParams": boolean;
  "connectorBuilder.schemaForm": boolean;
  "embedded.templateCreateButton": boolean;
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
  "authPage.rightSideUrl": undefined,
  "billing.early-sync-enabled": false,
  "connection.allowToSupportAllSyncModes": false,
  "connection.columnSelection": true,
  "connection.dataActivationUI": false,
  "connection.hashingUI": true, // also requires FeatureItem.FieldHashing
  "connection.mappingsUI": true, // requires FeatureItem.MappingsUI to enable configuration
  "connection.onboarding.destinations": "",
  "connection.onboarding.sources": "",
  "connection.rateLimitedUI": false,
  "connections.connectionsStatusesEnabled": false,
  "connector.airbyteCloudIpAddresses":
    "Please visit: https://docs.airbyte.com/platform/next/operating-airbyte/ip-allowlist for the latest IP ranges",
  "connector.suggestedDestinationConnectors": "",
  "connector.suggestedSourceConnectors": "",
  "connectorBuilder.aiAssist.enabled": false,
  "connectorBuilder.contributeEditsToMarketplace": true,
  "connectorBuilder.customComponents": false,
  "connectorBuilder.declarativeOauth": true,
  "connectorBuilder.dynamicStreams": false,
  "connectorBuilder.generateConnectorFromParams": false,
  "connectorBuilder.schemaForm": false,
  "embedded.templateCreateButton": false,
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
