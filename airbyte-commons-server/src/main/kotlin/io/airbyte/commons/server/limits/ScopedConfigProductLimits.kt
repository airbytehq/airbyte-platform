package io.airbyte.commons.server.limits

import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ProductLimitsKey
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ScopedConfigProductLimits(
  private val scopedConfigService: ScopedConfigurationService,
  private val defaultWorkspaceLimits: ProductLimitsProvider.WorkspaceLimits,
  private val defaultOrganizationLimits: ProductLimitsProvider.OrganizationLimits,
) :
  ProductLimitsProvider {
  override fun getLimitForWorkspace(workspaceId: UUID): ProductLimitsProvider.WorkspaceLimits {
    val scopeConfigValue =
      scopedConfigService.getScopedConfiguration(
        ProductLimitsKey.key,
        ConfigScopeType.WORKSPACE,
        workspaceId,
      )
    val maxConnections = scopeConfigValue.firstOrNull { it.resourceType == ConfigResourceType.CONNECTION }?.value?.toLong()
    val maxSources = scopeConfigValue.firstOrNull { it.resourceType == ConfigResourceType.SOURCE }?.value?.toLong()
    val maxDestinations = scopeConfigValue.firstOrNull { it.resourceType == ConfigResourceType.DESTINATION }?.value?.toLong()

    return ProductLimitsProvider.WorkspaceLimits(
      if (maxConnections != null) maxConnections else defaultWorkspaceLimits.maxConnections,
      if (maxSources != null) maxConnections else defaultWorkspaceLimits.maxSourcesOfSameType,
      if (maxDestinations != null) maxConnections else defaultWorkspaceLimits.maxDestinations,
    )
  }

  override fun getLimitForOrganization(organizationId: UUID): ProductLimitsProvider.OrganizationLimits {
    val scopeConfigValue =
      scopedConfigService.getScopedConfiguration(
        ProductLimitsKey.key,
        ConfigScopeType.ORGANIZATION,
        organizationId,
      )

    val maxWorkspaces = scopeConfigValue.firstOrNull { it.resourceType == ConfigResourceType.WORKSPACE }?.value?.toLong()
    val maxUsers = scopeConfigValue.firstOrNull { it.resourceType == ConfigResourceType.USER }?.value?.toLong()

    return ProductLimitsProvider.OrganizationLimits(
      if (maxWorkspaces != null) maxWorkspaces else defaultOrganizationLimits.maxWorkspaces,
      if (maxUsers != null) maxUsers else defaultOrganizationLimits.maxUsers,
    )
  }
}
