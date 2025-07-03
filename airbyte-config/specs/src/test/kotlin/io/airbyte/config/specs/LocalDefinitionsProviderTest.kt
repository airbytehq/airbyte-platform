/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.specs.RegistryDefinitionNotFoundException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.URI
import java.util.UUID

internal class LocalDefinitionsProviderTest {
  @Test
  @Throws(Exception::class)
  fun testGetSourceDefinition() {
    // source
    val stripeSourceId = UUID.fromString("e094cb9a-26de-4645-8761-65c0c425d1de")
    val stripeSource = localDefinitionsProvider!!.getSourceDefinition(stripeSourceId)
    Assertions.assertEquals(stripeSourceId, stripeSource.sourceDefinitionId)
    Assertions.assertEquals("Stripe", stripeSource.name)
    Assertions.assertEquals("airbyte/source-stripe", stripeSource.dockerRepository)
    Assertions.assertEquals("https://docs.airbyte.com/integrations/sources/stripe", stripeSource.documentationUrl)
    Assertions.assertEquals("stripe.svg", stripeSource.icon)
    Assertions.assertEquals(false, stripeSource.tombstone)
    Assertions.assertEquals("0.2.0", stripeSource.protocolVersion)
  }

  @Test
  @Throws(Exception::class)
  fun testGetDestinationDefinition() {
    val s3DestinationId = UUID.fromString("4816b78f-1489-44c1-9060-4b19d5fa9362")
    val s3Destination =
      localDefinitionsProvider
        .getDestinationDefinition(s3DestinationId)
    Assertions.assertEquals(s3DestinationId, s3Destination.destinationDefinitionId)
    Assertions.assertEquals("S3", s3Destination.name)
    Assertions.assertEquals("airbyte/destination-s3", s3Destination.dockerRepository)
    Assertions.assertEquals("https://docs.airbyte.com/integrations/destinations/s3", s3Destination.documentationUrl)
    Assertions.assertEquals(URI.create("https://docs.airbyte.com/integrations/destinations/s3"), s3Destination.spec.documentationUrl)
    Assertions.assertEquals(false, s3Destination.tombstone)
    Assertions.assertEquals("0.2.0", s3Destination.protocolVersion)
  }

  @Test
  fun testGetInvalidDefinitionId() {
    val invalidDefinitionId = UUID.fromString("1a7c360c-1289-4b96-a171-2ac1c86fb7ca")

    Assertions.assertThrows(
      RegistryDefinitionNotFoundException::class.java,
    ) { localDefinitionsProvider!!.getSourceDefinition(invalidDefinitionId) }
    Assertions.assertThrows(
      RegistryDefinitionNotFoundException::class.java,
    ) { localDefinitionsProvider!!.getDestinationDefinition(invalidDefinitionId) }
  }

  @Test
  fun testGetSourceDefinitions() {
    val sourceDefinitions = localDefinitionsProvider!!.getSourceDefinitions()
    Assertions.assertFalse(sourceDefinitions.isEmpty())
    Assertions.assertTrue(
      sourceDefinitions.stream().allMatch { sourceDef: ConnectorRegistrySourceDefinition -> sourceDef.protocolVersion.length > 0 },
    )
  }

  @Test
  fun testGetDestinationDefinitions() {
    val destinationDefinitions = localDefinitionsProvider!!.getDestinationDefinitions()
    Assertions.assertFalse(destinationDefinitions.isEmpty())
    Assertions.assertTrue(
      destinationDefinitions.stream().allMatch { sourceDef: ConnectorRegistryDestinationDefinition -> sourceDef.protocolVersion.length > 0 },
    )
  }

  companion object {
    private lateinit var localDefinitionsProvider: LocalDefinitionsProvider

    @BeforeAll
    @Throws(IOException::class)
    @JvmStatic
    fun setup() {
      localDefinitionsProvider = LocalDefinitionsProvider()
    }
  }
}
