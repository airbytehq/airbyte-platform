/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.secrets.hydration.RealSecretsHydrator
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.protocol.models.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.util.UUID

internal class SecretsRepositoryWriterTest {
  private lateinit var secretPersistence: MemorySecretPersistence
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var secretsHydrator: RealSecretsHydrator
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var metricClient: MetricClient
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var jsonSchemaValidator: JsonSchemaValidator

  @BeforeEach
  fun setup() {
    secretPersistence = spyk(MemorySecretPersistence())
    metricClient = mockk()
    featureFlagClient = mockk()

    secretsRepositoryWriter =
      SecretsRepositoryWriter(
        secretPersistence,
        metricClient,
        featureFlagClient,
      )
    secretsHydrator = RealSecretsHydrator(secretPersistence)
    secretsRepositoryReader = SecretsRepositoryReader(secretsHydrator)
  }

  @Test
  fun testDeleteSecrets() {
    every { metricClient.count(any(), any()) } returns Unit
    every { metricClient.count(any(), any(), any()) } returns Unit
    val secret = "test-secret"
    val coordinate = "existing_coordinate_v1"
    secretPersistence.write(SecretCoordinate.fromFullCoordinate(coordinate), secret)
    val config = injectCoordinate(coordinate)
    secretsRepositoryWriter.deleteFromConfig(
      config,
      SPEC.connectionSpecification,
      null,
    )
    verify(exactly = 1) { secretPersistence.delete(SecretCoordinate.fromFullCoordinate(coordinate)) }
    assertEquals("", secretPersistence.read(SecretCoordinate.fromFullCoordinate(coordinate)))
    verify { metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "true")) }
  }

  @Nested
  inner class TestUpdateSecrets {
    @BeforeEach
    fun setup() {
      every { metricClient.count(any(), any()) } returns Unit
      every { metricClient.count(any(), any(), any()) } returns Unit
      every { featureFlagClient.boolVariation(any(), any()) } returns true
    }

    @Test
    fun testUpdateSecretSameValueShouldWriteNewCoordinateAndDelete() {
      val secret = "secret-1"
      val oldCoordinate = "existing_coordinate_v1"
      secretPersistence.write(SecretCoordinate.fromFullCoordinate(oldCoordinate), secret)

      val updatedFullConfigNoSecretChange =
        Jsons.deserialize(
          """
          { "username": "airbyte1", "password": "$secret"}
          """.trimIndent(),
        )

      val oldPartialConfig = injectCoordinate(oldCoordinate)
      val updatedPartialConfig =
        secretsRepositoryWriter.updateFromConfig(
          WORKSPACE_ID,
          oldPartialConfig,
          updatedFullConfigNoSecretChange,
          SPEC.connectionSpecification,
          null,
        )

      val newCoordinate = "existing_coordinate_v2"
      val expPartialConfig =
        Jsons.deserialize(
          """
          {"username":"airbyte1","password":{"_secret":"$newCoordinate"}}
          """.trimIndent(),
        )
      assertEquals(expPartialConfig, updatedPartialConfig)

      verify(exactly = 1) { secretPersistence.write(SecretCoordinate.fromFullCoordinate(newCoordinate), secret) }
      assertEquals(secret, secretPersistence.read(SecretCoordinate.fromFullCoordinate(newCoordinate)))
      verify { metricClient.count(OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE, 1) }

      verify(exactly = 1) { secretPersistence.delete(SecretCoordinate.fromFullCoordinate(oldCoordinate)) }
      assertEquals("", secretPersistence.read(SecretCoordinate.fromFullCoordinate(oldCoordinate)))
      verify { metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "true")) }
    }

    @Test
    fun testUpdateSecretNewValueShouldWriteNewCoordinateAndDelete() {
      val oldCoordinate = "existing_coordinate_v1"
      secretPersistence.write(SecretCoordinate.fromFullCoordinate(oldCoordinate), "secret-1")

      val newSecret = "secret-2"
      val updatedFullConfigSecretChange =
        Jsons.deserialize(
          """
          { "username": "airbyte", "password": "$newSecret"}
          """.trimIndent(),
        )

      val oldPartialConfig = injectCoordinate(oldCoordinate)
      val updatedPartialConfig =
        secretsRepositoryWriter.updateFromConfig(
          WORKSPACE_ID,
          oldPartialConfig,
          updatedFullConfigSecretChange,
          SPEC.connectionSpecification,
          null,
        )

      val newCoordinate = "existing_coordinate_v2"
      val expPartialConfig =
        Jsons.deserialize(
          """
          {"username":"airbyte","password":{"_secret":"$newCoordinate"}}
          """.trimIndent(),
        )
      assertEquals(expPartialConfig, updatedPartialConfig)

      verify(exactly = 1) { secretPersistence.write(SecretCoordinate.fromFullCoordinate(newCoordinate), newSecret) }
      assertEquals(newSecret, secretPersistence.read(SecretCoordinate.fromFullCoordinate(newCoordinate)))
      verify { metricClient.count(OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE, 1) }

      verify(exactly = 1) { secretPersistence.delete(SecretCoordinate.fromFullCoordinate(oldCoordinate)) }
      assertEquals("", secretPersistence.read(SecretCoordinate.fromFullCoordinate(oldCoordinate)))
      verify { metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "true")) }
    }

    @Test
    fun testUpdateSecretsComplexShouldWriteNewCoordinateAndDelete() {
      val spec =
        Jsons.deserialize(
          """
          { "properties": { "username": { "type": "string" }, "credentials": { "type" : "object", "properties" : { "client_id": { "type": "string", "airbyte_secret": true }, "password": { "type": "string", "airbyte_secret": true } } } } }  
          """.trimIndent(),
        )
      val oldCoordinate1 = "existing-coordinate-0_v1"
      val oldSecret1 = "abc"
      secretPersistence.write(SecretCoordinate.fromFullCoordinate(oldCoordinate1), oldSecret1)
      val oldCoordinate2 = "existing-coordinate-1_v1"
      val oldSecret2 = "def"
      secretPersistence.write(SecretCoordinate.fromFullCoordinate(oldCoordinate2), oldSecret2)
      val oldPartialConfig =
        Jsons.deserialize(
          """
          { "username": "airbyte", "credentials": { "client_id": { "_secret": "$oldCoordinate1" }, "password": { "_secret": "$oldCoordinate2" } } }
          """.trimIndent(),
        )

      val newSecret = "ghi"
      val newFullConfig =
        Jsons.deserialize(
          """
          { "username": "airbyte", "credentials": { "client_id": "$oldSecret1", "password": "$newSecret" } }
          """.trimIndent(),
        )

      val updatedPartialConfig =
        secretsRepositoryWriter.updateFromConfig(
          WORKSPACE_ID,
          oldPartialConfig,
          newFullConfig,
          spec,
          null,
        )

      val newCoordinate1 = "existing-coordinate-0_v2"
      val newCoordinate2 = "existing-coordinate-1_v2"
      val expPartialConfig =
        Jsons.deserialize(
          """
          { "username": "airbyte", "credentials": { "client_id": { "_secret": "$newCoordinate1" }, "password": { "_secret": "$newCoordinate2" } } }
          """.trimIndent(),
        )
      assertEquals(expPartialConfig, updatedPartialConfig)

      verify(exactly = 1) { secretPersistence.write(SecretCoordinate.fromFullCoordinate(newCoordinate1), oldSecret1) }
      verify(exactly = 1) { secretPersistence.write(SecretCoordinate.fromFullCoordinate(newCoordinate2), newSecret) }
      verify { metricClient.count(OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE, 1) }
      verify { metricClient.count(OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE, 1) }

      verify(exactly = 1) { secretPersistence.delete(SecretCoordinate.fromFullCoordinate(oldCoordinate1)) }
      verify(exactly = 1) { secretPersistence.delete(SecretCoordinate.fromFullCoordinate(oldCoordinate2)) }
      verify { metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "true")) }
      verify { metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "true")) }
    }

    @Test
    fun testUpdateSecretDeleteErrorShouldNotPropagate() {
      secretPersistence = mockk()
      secretsRepositoryWriter =
        SecretsRepositoryWriter(
          secretPersistence,
          metricClient,
          featureFlagClient,
        )

      every { secretPersistence.write(any(), any()) } returns Unit
      every { secretPersistence.read(any()) } returns "something"
      every { secretPersistence.delete(any()) } throws RuntimeException("disable error")

      val oldCoordinate = "existing_coordinate_v1"
      val oldPartialConfig = injectCoordinate(oldCoordinate)

      val newSecret = "secret-2"
      val updatedFullConfigSecretChange =
        Jsons.deserialize(
          """
          { "username": "airbyte", "password": "$newSecret"}
          """.trimIndent(),
        )

      assertDoesNotThrow {
        secretsRepositoryWriter.updateFromConfig(
          WORKSPACE_ID,
          oldPartialConfig,
          updatedFullConfigSecretChange,
          SPEC.connectionSpecification,
          null,
        )
      }

      // The new secret should still be written, despite the disable error.
      val newCoordinate = "existing_coordinate_v2"
      verify(exactly = 1) { secretPersistence.write(SecretCoordinate.fromFullCoordinate(newCoordinate), newSecret) }
      verify(exactly = 1) { metricClient.count(OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE, 1) }

      verify(exactly = 1) { secretPersistence.delete(SecretCoordinate.fromFullCoordinate(oldCoordinate)) }
      // No metric is emitted because we were not successful.
      verify(exactly = 1) { metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "false")) }
    }
  }

  // TODO - port this to source service test
//  @Test
//  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
//  fun testWriteSourceConnection() {
//    every { configRepository.getSourceConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
//    every { configRepository.writeSourceConnectionNoSecrets(any()) } returns Unit
//    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
//    secretsRepositoryWriter.writeSourceConnection(SOURCE_WITH_FULL_CONFIG, SPEC)
//    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
//    val partialSource = Jsons.clone(SOURCE_WITH_FULL_CONFIG).withConfiguration(injectCoordinate(coordinate.fullCoordinate))
//    Assertions.assertNotNull(coordinate)
//    verify { configRepository.writeSourceConnectionNoSecrets(partialSource) }
//    verify(exactly = 1) { jsonSchemaValidator.ensure(any(), any()) }
//    val persistedSecret = secretPersistence.read(coordinate)
//    Assertions.assertEquals(SECRET, persistedSecret)
//
//    // verify that the round trip works.
//    every { configRepository.getSourceConnection(UUID1) } returns partialSource
//    Assertions.assertEquals(
//      SOURCE_WITH_FULL_CONFIG,
//      secretsRepositoryReader.getSourceConnectionWithSecrets(
//        UUID1,
//      ),
//    )
//  }

  // TODO -port this to destinationservicetest
//  @Test
//  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
//  fun testWriteDestinationConnection() {
//    every { configRepository.getDestinationConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
//    every { configRepository.writeDestinationConnectionNoSecrets(any()) } returns Unit
//    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
//    secretsRepositoryWriter.writeDestinationConnection(DESTINATION_WITH_FULL_CONFIG, SPEC)
//    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
//    Assertions.assertNotNull(coordinate)
//    val partialDestination =
//      Jsons.clone(DESTINATION_WITH_FULL_CONFIG).withConfiguration(
//        injectCoordinate(coordinate.fullCoordinate),
//      )
//    verify { configRepository.writeDestinationConnectionNoSecrets(partialDestination) }
//    verify(exactly = 1) { jsonSchemaValidator.ensure(any(), any()) }
//    val persistedSecret = secretPersistence.read(coordinate)
//    Assertions.assertEquals(SECRET, persistedSecret)
//
//    // verify that the round trip works.
//    every { configRepository.getDestinationConnection(UUID1) } returns partialDestination
//    Assertions.assertEquals(
//      DESTINATION_WITH_FULL_CONFIG,
//      secretsRepositoryReader.getDestinationConnectionWithSecrets(
//        UUID1,
//      ),
//    )
//  }

  // TODO - port this to source service test
//  @Test
//  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
//  fun testWriteSourceConnectionWithTombstone() {
//    every { configRepository.getSourceConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
//    every { configRepository.writeSourceConnectionNoSecrets(any()) } returns Unit
//    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
//    val sourceWithTombstone =
//      SourceConnection()
//        .withSourceId(UUID1)
//        .withSourceDefinitionId(UUID.randomUUID())
//        .withConfiguration(FULL_CONFIG)
//        .withTombstone(true)
//        .withWorkspaceId(WORKSPACE_ID)
//    secretsRepositoryWriter.writeSourceConnection(sourceWithTombstone, SPEC)
//    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
//    Assertions.assertNotNull(coordinate)
//    val partialSource =
//      Jsons.clone(sourceWithTombstone).withConfiguration(injectCoordinate(coordinate.fullCoordinate))
//    verify { configRepository.writeSourceConnectionNoSecrets(partialSource) }
//    verify(exactly = 0) { jsonSchemaValidator.ensure(any(), any()) }
//    val persistedSecret = secretPersistence.read(coordinate)
//    Assertions.assertEquals(SECRET, persistedSecret)
//
//    // verify that the round trip works.
//    every { configRepository.getSourceConnection(UUID1) } returns partialSource
//    Assertions.assertEquals(
//      sourceWithTombstone,
//      secretsRepositoryReader.getSourceConnectionWithSecrets(
//        UUID1,
//      ),
//    )
//  }

  // TODO - port this to destination service test
//  @Test
//  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
//  fun testWriteDestinationConnectionWithTombstone() {
//    every { configRepository.getDestinationConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
//    every { configRepository.writeDestinationConnectionNoSecrets(any()) } returns Unit
//    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
//    val destinationWithTombstone =
//      DestinationConnection()
//        .withDestinationId(UUID1)
//        .withConfiguration(FULL_CONFIG)
//        .withTombstone(true)
//        .withWorkspaceId(WORKSPACE_ID)
//    secretsRepositoryWriter.writeDestinationConnection(destinationWithTombstone, SPEC)
//    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
//    Assertions.assertNotNull(coordinate)
//    val partialDestination =
//      Jsons.clone(destinationWithTombstone).withConfiguration(
//        injectCoordinate(coordinate.fullCoordinate),
//      )
//    verify { configRepository.writeDestinationConnectionNoSecrets(partialDestination) }
//    verify(exactly = 0) { jsonSchemaValidator.ensure(any(), any()) }
//    val persistedSecret = secretPersistence.read(coordinate)
//    Assertions.assertEquals(SECRET, persistedSecret)
//
//    // verify that the round trip works.
//    every { configRepository.getDestinationConnection(UUID1) } returns partialDestination
//    Assertions.assertEquals(
//      destinationWithTombstone,
//      secretsRepositoryReader.getDestinationConnectionWithSecrets(
//        UUID1,
//      ),
//    )
//  }

  // this only works if the secrets store has one secret.
  private fun getCoordinateFromSecretsStore(secretPersistence: MemorySecretPersistence): SecretCoordinate {
    return secretPersistence.map.keys.first()
  }

//  @Test
//  @Throws(JsonValidationException::class, IOException::class)
//  fun `writeWorkspace should ensure that secret fields are replaced`() {
//    val configRepository: ConfigRepository = mockk()
//    val secretPersistence: SecretPersistence = mockk()
//    val secretsRepositoryWriter = spyk(SecretsRepositoryWriter(configRepository, jsonSchemaValidator, secretPersistence))
//    val webhookConfigs =
//      WebhookOperationConfigs().withWebhookConfigs(
//        listOf(WebhookConfig().withName(TEST_WEBHOOK_NAME).withAuthToken(TEST_AUTH_TOKEN).withId(UUID.randomUUID())),
//      )
//    val workspace =
//      StandardWorkspace()
//        .withWorkspaceId(UUID.randomUUID())
//        .withCustomerId(UUID.randomUUID())
//        .withEmail(TEST_EMAIL)
//        .withName(TEST_WORKSPACE_NAME)
//        .withSlug(TEST_WORKSPACE_SLUG)
//        .withInitialSetupComplete(false)
//        .withDisplaySetupWizard(true)
//        .withNews(false)
//        .withAnonymousDataCollection(false)
//        .withSecurityUpdates(false)
//        .withTombstone(false)
//        .withNotifications(emptyList())
//        .withDefaultGeography(Geography.AUTO) // Serialize it to a string, then deserialize it to a JsonNode.
//        .withWebhookOperationConfigs(Jsons.jsonNode(webhookConfigs))
//
//    every { configRepository.getStandardWorkspaceNoSecrets(any(), any()) } returns workspace
//    every { configRepository.writeStandardWorkspaceNoSecrets(any()) } returns Unit
//    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
//    every { secretPersistence.write(any(), any()) } returns Unit
//
//    secretsRepositoryWriter.writeWorkspace(workspace)
//    val workspaceArgument = slot<StandardWorkspace>()
//    verify(exactly = 1) { configRepository.writeStandardWorkspaceNoSecrets(capture(workspaceArgument)) }
//    Assertions.assertFalse(
//      Jsons.serialize(workspaceArgument.captured.webhookOperationConfigs).contains(TEST_AUTH_TOKEN),
//    )
//  }

  companion object {
    private val UUID1 = UUID.randomUUID()
    private val SPEC =
      ConnectorSpecification()
        .withConnectionSpecification(
          Jsons.deserialize(
            """
            { "properties": { "username": { "type": "string" }, "password": { "type": "string", "airbyte_secret": true } } }  
            """.trimIndent(),
          ),
        )
    private const val SECRET = "abc"
    private val FULL_CONFIG =
      Jsons.deserialize(
        """
        { "username": "airbyte", "password": "$SECRET"}
        """.trimIndent(),
      )
    private val WORKSPACE_ID = UUID.randomUUID()
    private val SOURCE_WITH_FULL_CONFIG =
      SourceConnection()
        .withSourceId(UUID1)
        .withSourceDefinitionId(UUID.randomUUID())
        .withConfiguration(FULL_CONFIG)
        .withWorkspaceId(WORKSPACE_ID)
    private val DESTINATION_WITH_FULL_CONFIG =
      DestinationConnection()
        .withDestinationId(UUID1)
        .withConfiguration(FULL_CONFIG)
        .withWorkspaceId(WORKSPACE_ID)
    private const val PASSWORD_PROPERTY_NAME = "password"
    private const val PASSWORD_FIELD_NAME = "_secret"
    private const val TEST_EMAIL = "test-email"
    private const val TEST_WORKSPACE_NAME = "test-workspace-name"
    private const val TEST_WORKSPACE_SLUG = "test-workspace-slug"
    private const val TEST_WEBHOOK_NAME = "test-webhook-name"
    private const val TEST_AUTH_TOKEN = "test-auth-token"

    private fun injectCoordinate(coordinate: String): JsonNode {
      return Jsons.deserialize("{ \"username\": \"airbyte\", \"password\": { \"_secret\": \"$coordinate\" } }")
    }
  }
}
