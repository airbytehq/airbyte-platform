/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.AirbyteLicense
import io.airbyte.config.ActorType
import io.airbyte.featureflag.AllowConfigTemplateEndpoints
import io.airbyte.featureflag.AllowConfigWithSecretCoordinatesEndpoints
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.LicenseAllowEnterpriseConnector
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.TestClient
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
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

    @Test
    fun `test hasConfigTemplateEntitlements`() {
      val organizationId = UUID.randomUUID()
      val res = entitlementProvider.hasConfigTemplateEntitlements(organizationId)
      assertFalse(res)
    }

    @Test
    fun `test hasConfigWithSecretCoordinatesEntitlements`() {
      val organizationId = UUID.randomUUID()
      val res = entitlementProvider.hasConfigWithSecretCoordinatesEntitlements(organizationId)
      assertFalse(res)
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

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `test hasConfigTemplateEntitlements returns value from the license`(isEmbedded: Boolean) {
      val organizationId = UUID.randomUUID()

      every { license.isEmbedded } returns isEmbedded
      val res = entitlementProvider.hasConfigTemplateEntitlements(organizationId)
      assertEquals(res, license.isEmbedded)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `test hasConfigWithSecretCoordinatesEntitlements returns value from the license`(isEmbedded: Boolean) {
      val organizationId = UUID.randomUUID()

      every { license.isEmbedded } returns isEmbedded
      val res = entitlementProvider.hasConfigWithSecretCoordinatesEntitlements(organizationId)
      assertEquals(res, license.isEmbedded)
    }
  }

  @Nested
  inner class CloudEntitlementProviderTest {
    private val featureFlagClient = mockk<TestClient>()
    private val entitlementProvider = CloudEntitlementProvider(featureFlagClient)

    @ParameterizedTest
    @ValueSource(strings = ["SOURCE", "DESTINATION"])
    fun `test hasEnterpriseConnectorEntitlements`(actorType: ActorType) {
      val organizationId = UUID.randomUUID()
      val entitledConnectorId = UUID.randomUUID()
      val notEntitledConnectorId = UUID.randomUUID()

      mockEntitledEnterpriseConnector(actorType, organizationId, entitledConnectorId, true)
      mockEntitledEnterpriseConnector(actorType, organizationId, notEntitledConnectorId, false)

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

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `test hasConfigTemplateEntitlements returns value from feature flag`(isEntitled: Boolean) {
      val organizationId = UUID.randomUUID()
      every { featureFlagClient.boolVariation(AllowConfigTemplateEndpoints, Organization(organizationId)) } returns isEntitled

      val res = entitlementProvider.hasConfigTemplateEntitlements(organizationId)
      assertEquals(res, isEntitled)
    }

    // TODO: for cloud, this entitlement is always false for now
    // https://github.com/airbytehq/airbyte-internal-issues/issues/12217
    @ParameterizedTest
    @ValueSource(booleans = [false, false])
    fun `test hasConfigWithSecretCoordinatesEntitlements returns value from feature flag`(isEntitled: Boolean) {
      val organizationId = UUID.randomUUID()
      every { featureFlagClient.boolVariation(AllowConfigWithSecretCoordinatesEndpoints, Organization(organizationId)) } returns isEntitled

      val res = entitlementProvider.hasConfigWithSecretCoordinatesEntitlements(organizationId)
      assertEquals(res, isEntitled)
    }

    private fun mockEntitledEnterpriseConnector(
      actorType: ActorType,
      organizationId: UUID,
      connectorId: UUID,
      isEntitled: Boolean,
    ) {
      when (actorType) {
        ActorType.SOURCE ->
          every {
            featureFlagClient.boolVariation(
              LicenseAllowEnterpriseConnector,
              Multi(
                listOf(
                  Organization(organizationId),
                  SourceDefinition(connectorId),
                ),
              ),
            )
          } returns isEntitled
        ActorType.DESTINATION ->
          every {
            featureFlagClient.boolVariation(
              LicenseAllowEnterpriseConnector,
              Multi(
                listOf(
                  Organization(organizationId),
                  DestinationDefinition(connectorId),
                ),
              ),
            )
          } returns isEntitled
      }
    }
  }
}
