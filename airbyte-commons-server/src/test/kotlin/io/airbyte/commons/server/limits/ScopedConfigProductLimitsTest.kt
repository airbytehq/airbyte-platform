package io.airbyte.commons.server.limits

import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ProductLimitsKey
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class ScopedConfigProductLimitsTest {
  private lateinit var limitsProvider: ScopedConfigProductLimits
  private val scopedConfigService = mockk<ScopedConfigurationService>()

  @Test
  fun testQueryScopeConfig() {
    val organizationId = UUID.randomUUID()
    val defaultWorkspaceLimits = ProductLimitsProvider.WorkspaceLimits(1000L, 10L, 8L)
    val defaultOrganizationLimits = ProductLimitsProvider.OrganizationLimits(1000L, null)
    val expectedMaxUser = 100L

    val scopedConfigurations =
      listOf(
        ScopedConfiguration().withId(UUID.randomUUID()).withKey(ProductLimitsKey.key)
          .withScopeType(ConfigScopeType.ORGANIZATION).withScopeId(organizationId)
          .withResourceType(ConfigResourceType.USER).withValue(expectedMaxUser.toString()),
      )

    every {
      scopedConfigService.getScopedConfiguration(
        ProductLimitsKey.key,
        ConfigScopeType.ORGANIZATION,
        organizationId,
      )
    } returns scopedConfigurations

    limitsProvider = ScopedConfigProductLimits(scopedConfigService, defaultWorkspaceLimits, defaultOrganizationLimits)
    val actualMaxUser = limitsProvider.getLimitForOrganization(organizationId).maxUsers
    Assertions.assertEquals(expectedMaxUser, actualMaxUser)
  }

  @Test
  fun testDefaultValue() {
    val organizationId = UUID.randomUUID()
    val expectedMaxUser = 100L
    val defaultWorkspaceLimits = ProductLimitsProvider.WorkspaceLimits(1000L, 10L, 8L)
    val defaultOrganizationLimits = ProductLimitsProvider.OrganizationLimits(1000L, expectedMaxUser)

    val scopedConfigurations =
      listOf(
        ScopedConfiguration().withId(UUID.randomUUID()).withKey(ProductLimitsKey.key)
          .withScopeType(ConfigScopeType.ORGANIZATION).withScopeId(organizationId)
          .withResourceType(ConfigResourceType.CONNECTION).withValue(expectedMaxUser.toString()),
      )

    every {
      scopedConfigService.getScopedConfiguration(
        ProductLimitsKey.key,
        ConfigScopeType.ORGANIZATION,
        organizationId,
      )
    } returns scopedConfigurations

    limitsProvider = ScopedConfigProductLimits(scopedConfigService, defaultWorkspaceLimits, defaultOrganizationLimits)
    val actualMaxUser = limitsProvider.getLimitForOrganization(organizationId).maxUsers
    Assertions.assertEquals(expectedMaxUser, actualMaxUser)
  }
}
