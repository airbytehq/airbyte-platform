/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FeatureEntitlement
import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.AirbyteLicense
import io.airbyte.commons.license.AirbyteLicense.LicenseType
import io.airbyte.config.Configs
import io.airbyte.data.services.OrganizationService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteStiggClientConfig
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class EntitlementClientFactoryTest {
  @Test
  fun `community edition`() {
    val factory =
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.COMMUNITY),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(),
        activeLicense = null,
      )
    assertInstanceOf<NoEntitlementClient>(factory.entitlementClient())
  }

  @Test
  fun `enterprise edition`() {
    val license =
      AirbyteLicense(
        type = LicenseType.ENTERPRISE,
        stiggEntitlements = EXAMPLE_ENTITLEMENTS_JSON,
      )
    val factory =
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.ENTERPRISE),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(),
        activeLicense = ActiveAirbyteLicense("").also { it.license = license },
      )

    val org = OrganizationId(UUID.randomUUID())
    val client = factory.entitlementClient()
    assertInstanceOf<StiggEnterpriseEntitlementClient>(client)

    assertEquals(
      listOf<EntitlementResult>(
        EntitlementResult(featureId = "feature-a", isEntitled = true),
        EntitlementResult(featureId = "feature-b", isEntitled = true),
      ),
      client.getEntitlements(org),
    )

    client.checkEntitlement(org, FeatureEntitlement("feature-a")).assertEntitled()
    client.checkEntitlement(org, FeatureEntitlement("feature-b")).assertEntitled()
    client.checkEntitlement(org, FeatureEntitlement("feature-c")).assertNotEntitled()
  }

  @Test
  fun `enterprise edition with no entitlements in license falls back to NoEntitlementClient`() {
    val license = AirbyteLicense(LicenseType.ENTERPRISE)
    val factory =
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.ENTERPRISE),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(),
        activeLicense = ActiveAirbyteLicense("").also { it.license = license },
      )

    val client = factory.entitlementClient()
    assertInstanceOf<NoEntitlementClient>(client)
  }

  @Test
  fun `enterprise edition with no active license falls back to NoEntitlementClient`() {
    val factory =
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.ENTERPRISE),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(),
        activeLicense = null,
      )

    val client = factory.entitlementClient()
    assertInstanceOf<NoEntitlementClient>(client)
  }

  @Test
  fun `cloud edition`() {
    assertThrows<MissingStiggApiKey> {
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.CLOUD),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(enabled = true),
      ).entitlementClient()
    }
    assertThrows<MissingStiggSidecarHost> {
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.CLOUD),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(enabled = true, apiKey = "foo"),
      ).entitlementClient()
    }
    assertThrows<MissingStiggSidecarPort> {
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.CLOUD),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(enabled = true, apiKey = "foo", sidecarHost = "foo", sidecarPort = 0),
      ).entitlementClient()
    }
    assertThrows<MissingStiggSidecarPort> {
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.CLOUD),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(enabled = true, apiKey = "foo", sidecarHost = "foo", sidecarPort = -1),
      ).entitlementClient()
    }
    assertThrows<MissingOrganizationService> {
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.CLOUD),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(enabled = true, apiKey = "foo", sidecarHost = "foo", sidecarPort = 10000),
      ).entitlementClient()
    }
    val orgService = mockk<OrganizationService>()

    // normal cloud client
    assertInstanceOf<StiggCloudEntitlementClient>(
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.CLOUD),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(enabled = true, apiKey = "foo", sidecarHost = "foo", sidecarPort = 10000),
        organizationService = orgService,
      ).entitlementClient(),
    )

    // stigg disabled in cloud
    assertInstanceOf<NoEntitlementClient>(
      EntitlementClientFactory(
        airbyteConfig = AirbyteConfig(edition = Configs.AirbyteEdition.CLOUD),
        airbyteStiggClientConfig = AirbyteStiggClientConfig(enabled = false),
        organizationService = orgService,
      ).entitlementClient(),
    )
  }
}

private val EXAMPLE_ENTITLEMENTS_JSON =
  """
{
    "entitlements": {
      "feature-a": { "type": "BOOLEAN" },
      "feature-b": { "type": "BOOLEAN" }
    }
}  
  """.trimIndent()

private fun EntitlementResult.assertEntitled() {
  assertEquals(true, this.isEntitled)
}

private fun EntitlementResult.assertNotEntitled() {
  assertEquals(false, this.isEntitled)
}
