/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.entitlements

import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.AirbyteLicense
import io.airbyte.config.ActorType
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

class EntitlementProviderTest {
  @Nested
  inner class DefaultEntitlementProviderTest {
    private val entitlementProvider = DefaultEntitlementProvider()

    @ParameterizedTest
    @ValueSource(strings = ["SOURCE", "DESTINATION"])
    fun `test hasEnterpriseConnectorEntitlements`(actorType: ActorType) {
      val organizationId = UUID.randomUUID()
      val actorDefinitionIds = listOf(UUID.randomUUID(), UUID.randomUUID())
      val res = entitlementProvider.hasEnterpriseConnectorEntitlements(organizationId, actorType, actorDefinitionIds)
      assertEquals(
        mapOf(
          actorDefinitionIds[0] to false,
          actorDefinitionIds[1] to false,
        ),
        res,
      )
    }
  }

  @Nested
  inner class EnterpriseEntitlementProviderTest {
    private val activeLicense = mockk<ActiveAirbyteLicense>()
    private val license = mockk<AirbyteLicense>()
    private lateinit var entitlementProvider: EntitlementProvider

    @BeforeEach
    fun setup() {
      entitlementProvider = EnterpriseEntitlementProvider(activeLicense)
      every { activeLicense.license } returns license
    }

    @ParameterizedTest
    @ValueSource(strings = ["SOURCE", "DESTINATION"])
    fun `test hasEnterpriseConnectorEntitlements`(actorType: ActorType) {
      val entitledConnectorId = UUID.randomUUID()
      val notEntitledConnectorId = UUID.randomUUID()
      every { license.enterpriseConnectorIds } returns setOf(entitledConnectorId)

      val organizationId = UUID.randomUUID()
      val actorDefinitionIds = listOf(entitledConnectorId, notEntitledConnectorId)
      val res = entitlementProvider.hasEnterpriseConnectorEntitlements(organizationId, actorType, actorDefinitionIds)
      assertEquals(
        mapOf(
          entitledConnectorId to true,
          notEntitledConnectorId to false,
        ),
        res,
      )
    }
  }
}
