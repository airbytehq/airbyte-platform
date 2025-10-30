/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.URI
import java.util.UUID

internal class LocalDefinitionsProviderTest {
  private lateinit var localDefinitionsProvider: LocalDefinitionsProvider

  @BeforeEach
  fun setup() {
    localDefinitionsProvider = LocalDefinitionsProvider()
  }

  @Test
  fun testGetSourceDefinition() {
    // source
    val stripeSourceId = UUID.fromString("e094cb9a-26de-4645-8761-65c0c425d1de")
    val stripeSource = localDefinitionsProvider.getSourceDefinition(stripeSourceId)
    assertEquals(stripeSourceId, stripeSource.sourceDefinitionId)
    assertEquals("Stripe", stripeSource.name)
    assertEquals("airbyte/source-stripe", stripeSource.dockerRepository)
    assertEquals("https://docs.airbyte.com/integrations/sources/stripe", stripeSource.documentationUrl)
    assertEquals("stripe.svg", stripeSource.icon)
    assertFalse(stripeSource.tombstone)
    assertEquals("0.2.0", stripeSource.protocolVersion)
  }

  @Test
  fun testGetDestinationDefinition() {
    val s3DestinationId = UUID.fromString("4816b78f-1489-44c1-9060-4b19d5fa9362")
    val s3Destination =
      localDefinitionsProvider
        .getDestinationDefinition(s3DestinationId)
    assertEquals(s3DestinationId, s3Destination.destinationDefinitionId)
    assertEquals("S3", s3Destination.name)
    assertEquals("airbyte/destination-s3", s3Destination.dockerRepository)
    assertEquals("https://docs.airbyte.com/integrations/destinations/s3", s3Destination.documentationUrl)
    assertEquals(URI.create("https://docs.airbyte.com/integrations/destinations/s3"), s3Destination.spec.documentationUrl)
    assertFalse(s3Destination.tombstone)
    assertEquals("0.2.0", s3Destination.protocolVersion)
  }

  @Test
  fun testGetInvalidDefinitionId() {
    val invalidDefinitionId = UUID.fromString("1a7c360c-1289-4b96-a171-2ac1c86fb7ca")

    assertThrows(
      RegistryDefinitionNotFoundException::class.java,
    ) { localDefinitionsProvider.getSourceDefinition(invalidDefinitionId) }
    assertThrows(
      RegistryDefinitionNotFoundException::class.java,
    ) { localDefinitionsProvider.getDestinationDefinition(invalidDefinitionId) }
  }

  @Test
  fun testGetSourceDefinitions() {
    val sourceDefinitions = localDefinitionsProvider.getSourceDefinitions()
    assertTrue(sourceDefinitions.isNotEmpty())
    assertTrue(
      sourceDefinitions.all { sourceDef: ConnectorRegistrySourceDefinition -> sourceDef.protocolVersion.isNotEmpty() },
    )
  }

  @Test
  fun testGetDestinationDefinitions() {
    val destinationDefinitions = localDefinitionsProvider.getDestinationDefinitions()
    assertTrue(destinationDefinitions.isNotEmpty())
    assertTrue(
      destinationDefinitions.all { sourceDef: ConnectorRegistryDestinationDefinition -> sourceDef.protocolVersion.isNotEmpty() },
    )
  }

  @Test
  fun testGetLocalConnectorRegistry() {
    val connectorRegistry = localDefinitionsProvider.getLocalConnectorRegistry()
    assertTrue(connectorRegistry.sources.isNotEmpty())
    assertTrue(connectorRegistry.destinations.isNotEmpty())
  }
}
