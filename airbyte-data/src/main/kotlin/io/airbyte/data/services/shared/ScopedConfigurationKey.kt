package io.airbyte.data.services.shared

import io.airbyte.config.ConfigScopeType

/**
 * A key that identifies something that can be configured via scoped configuration.
 *
 * @param [key] is a unique identifier for the configuration.
 * @param [supportedScopes] is the list of scopes that this configuration supports. Scopes will be evaluated in the order of this list. The first scope that returns a value will be used.
 */
open class ScopedConfigurationKey(val key: String, val supportedScopes: List<ConfigScopeType>)

/**
 * Used for configuring actor definition versions to run, allowing us to run different versions of a connector within different scopes.
 * See ConfigurationDefinitionVersionOverrideProvider for usage.
 */
data object ConnectorVersionKey : ScopedConfigurationKey(
  key = "connector_version",
  supportedScopes = listOf(ConfigScopeType.ACTOR, ConfigScopeType.WORKSPACE, ConfigScopeType.ORGANIZATION),
)

/**
 * A token used to allow the workload launcher to add labels to a pod for network policy application.
 */
data object NetworkSecurityTokenKey : ScopedConfigurationKey(
  key = "network_security_token",
  supportedScopes = listOf(ConfigScopeType.WORKSPACE, ConfigScopeType.ORGANIZATION),
)

data object ProductLimitsKey : ScopedConfigurationKey(
  key = "product_limits",
  supportedScopes = listOf(ConfigScopeType.WORKSPACE, ConfigScopeType.ORGANIZATION),
)

val ScopedConfigurationKeys: Map<String, ScopedConfigurationKey> =
  mapOf(
    ConnectorVersionKey.key to ConnectorVersionKey,
    NetworkSecurityTokenKey.key to NetworkSecurityTokenKey,
    ProductLimitsKey.key to ProductLimitsKey,
  )
