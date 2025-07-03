/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.ActorType
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

class LicenseEntitlementCheckerTest {
  private val entitlementService = mockk<EntitlementService>()
  private val sourceService = mockk<SourceService>()
  private val destinationService = mockk<DestinationService>()
  private val entitlementChecker = LicenseEntitlementChecker(entitlementService, sourceService, destinationService)

  @ParameterizedTest
  @ValueSource(strings = ["SOURCE", "DESTINATION"])
  fun testCheckConnectorEntitlements(actorType: ActorType) {
    val organizationId = UUID.randomUUID()
    val entitledResourceId = UUID.randomUUID()
    val notEntitledResourceId = UUID.randomUUID()
    val notEnterpriseResourceId = UUID.randomUUID()

    val enterpriseResourceIds = listOf(entitledResourceId, notEntitledResourceId)
    val resourceIds = listOf(entitledResourceId, notEntitledResourceId, notEnterpriseResourceId)

    val entitlement =
      when (actorType) {
        ActorType.SOURCE -> Entitlement.SOURCE_CONNECTOR
        ActorType.DESTINATION -> Entitlement.DESTINATION_CONNECTOR
      }

    mockEnterpriseConnectors(actorType, enterpriseResourceIds, true)
    mockEnterpriseConnectors(actorType, listOf(notEnterpriseResourceId), false)

    every {
      entitlementService.hasEnterpriseConnectorEntitlements(eq(organizationId), eq(actorType), any())
    } returns
      mapOf(
        entitledResourceId to true,
        notEntitledResourceId to false,
      )

    val res = entitlementChecker.checkEntitlements(organizationId, entitlement, resourceIds)
    assertEquals(
      mapOf(
        entitledResourceId to true,
        notEntitledResourceId to false,
        notEnterpriseResourceId to true,
      ),
      res,
    )

    assertTrue(entitlementChecker.checkEntitlement(organizationId, entitlement, entitledResourceId))
    assertTrue(entitlementChecker.checkEntitlement(organizationId, entitlement, notEnterpriseResourceId))
    assertFalse(entitlementChecker.checkEntitlement(organizationId, entitlement, notEntitledResourceId))
  }

  private fun mockEnterpriseConnectors(
    actorType: ActorType,
    resourceIds: List<UUID>,
    isEnterprise: Boolean,
  ) {
    when (actorType) {
      ActorType.SOURCE -> {
        every {
          sourceService.getStandardSourceDefinition(match { it in resourceIds })
        } returns StandardSourceDefinition().withEnterprise(isEnterprise)
      }
      ActorType.DESTINATION -> {
        every {
          destinationService.getStandardDestinationDefinition(match { it in resourceIds })
        } returns StandardDestinationDefinition().withEnterprise(isEnterprise)
      }
    }
  }
}
