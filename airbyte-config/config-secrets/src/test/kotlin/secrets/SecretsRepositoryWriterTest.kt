/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigSchema
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Geography
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import io.airbyte.config.WorkspaceServiceAccount
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.ConfigRepository
import io.airbyte.config.persistence.MockData
import io.airbyte.config.secrets.hydration.RealSecretsHydrator
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.protocol.models.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Map
import java.util.UUID

internal class SecretsRepositoryWriterTest {
  private lateinit var configRepository: ConfigRepository
  private lateinit var secretPersistence: MemorySecretPersistence
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var secretsHydrator: RealSecretsHydrator
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var jsonSchemaValidator: JsonSchemaValidator

  @BeforeEach
  fun setup() {
    configRepository = mockk()
    secretPersistence = MemorySecretPersistence()
    jsonSchemaValidator = mockk()
    secretsRepositoryWriter =
      SecretsRepositoryWriter(
        configRepository,
        jsonSchemaValidator,
        secretPersistence,
      )
    secretsHydrator = RealSecretsHydrator(secretPersistence)
    secretsRepositoryReader = SecretsRepositoryReader(configRepository, secretsHydrator)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testWriteSourceConnection() {
    every { configRepository.getSourceConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
    every { configRepository.writeSourceConnectionNoSecrets(any()) } returns Unit
    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
    secretsRepositoryWriter.writeSourceConnection(SOURCE_WITH_FULL_CONFIG, SPEC)
    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
    val partialSource = Jsons.clone(SOURCE_WITH_FULL_CONFIG).withConfiguration(injectCoordinate(coordinate.fullCoordinate))
    Assertions.assertNotNull(coordinate)
    verify { configRepository.writeSourceConnectionNoSecrets(partialSource) }
    verify(exactly = 1) { jsonSchemaValidator.ensure(any(), any()) }
    val persistedSecret = secretPersistence.read(coordinate)
    Assertions.assertEquals(SECRET, persistedSecret)

    // verify that the round trip works.
    every { configRepository.getSourceConnection(UUID1) } returns partialSource
    Assertions.assertEquals(
      SOURCE_WITH_FULL_CONFIG,
      secretsRepositoryReader.getSourceConnectionWithSecrets(
        UUID1,
      ),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testWriteDestinationConnection() {
    every { configRepository.getDestinationConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
    every { configRepository.writeDestinationConnectionNoSecrets(any()) } returns Unit
    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
    secretsRepositoryWriter.writeDestinationConnection(DESTINATION_WITH_FULL_CONFIG, SPEC)
    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
    Assertions.assertNotNull(coordinate)
    val partialDestination =
      Jsons.clone(DESTINATION_WITH_FULL_CONFIG).withConfiguration(
        injectCoordinate(coordinate.fullCoordinate),
      )
    verify { configRepository.writeDestinationConnectionNoSecrets(partialDestination) }
    verify(exactly = 1) { jsonSchemaValidator.ensure(any(), any()) }
    val persistedSecret = secretPersistence.read(coordinate)
    Assertions.assertEquals(SECRET, persistedSecret)

    // verify that the round trip works.
    every { configRepository.getDestinationConnection(UUID1) } returns partialDestination
    Assertions.assertEquals(
      DESTINATION_WITH_FULL_CONFIG,
      secretsRepositoryReader.getDestinationConnectionWithSecrets(
        UUID1,
      ),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testWriteSourceConnectionWithTombstone() {
    every { configRepository.getSourceConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
    every { configRepository.writeSourceConnectionNoSecrets(any()) } returns Unit
    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
    val sourceWithTombstone =
      SourceConnection()
        .withSourceId(UUID1)
        .withSourceDefinitionId(UUID.randomUUID())
        .withConfiguration(FULL_CONFIG)
        .withTombstone(true)
        .withWorkspaceId(WORKSPACE_ID)
    secretsRepositoryWriter.writeSourceConnection(sourceWithTombstone, SPEC)
    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
    Assertions.assertNotNull(coordinate)
    val partialSource =
      Jsons.clone(sourceWithTombstone).withConfiguration(injectCoordinate(coordinate.fullCoordinate))
    verify { configRepository.writeSourceConnectionNoSecrets(partialSource) }
    verify(exactly = 0) { jsonSchemaValidator.ensure(any(), any()) }
    val persistedSecret = secretPersistence.read(coordinate)
    Assertions.assertEquals(SECRET, persistedSecret)

    // verify that the round trip works.
    every { configRepository.getSourceConnection(UUID1) } returns partialSource
    Assertions.assertEquals(
      sourceWithTombstone,
      secretsRepositoryReader.getSourceConnectionWithSecrets(
        UUID1,
      ),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testWriteDestinationConnectionWithTombstone() {
    every { configRepository.getDestinationConnection(UUID1) } throws ConfigNotFoundException("test", UUID1.toString())
    every { configRepository.writeDestinationConnectionNoSecrets(any()) } returns Unit
    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
    val destinationWithTombstone =
      DestinationConnection()
        .withDestinationId(UUID1)
        .withConfiguration(FULL_CONFIG)
        .withTombstone(true)
        .withWorkspaceId(WORKSPACE_ID)
    secretsRepositoryWriter.writeDestinationConnection(destinationWithTombstone, SPEC)
    val coordinate = getCoordinateFromSecretsStore(secretPersistence)
    Assertions.assertNotNull(coordinate)
    val partialDestination =
      Jsons.clone(destinationWithTombstone).withConfiguration(
        injectCoordinate(coordinate.fullCoordinate),
      )
    verify { configRepository.writeDestinationConnectionNoSecrets(partialDestination) }
    verify(exactly = 0) { jsonSchemaValidator.ensure(any(), any()) }
    val persistedSecret = secretPersistence.read(coordinate)
    Assertions.assertEquals(SECRET, persistedSecret)

    // verify that the round trip works.
    every { configRepository.getDestinationConnection(UUID1) } returns partialDestination
    Assertions.assertEquals(
      destinationWithTombstone,
      secretsRepositoryReader.getDestinationConnectionWithSecrets(
        UUID1,
      ),
    )
  }

  // this only works if the secrets store has one secret.
  private fun getCoordinateFromSecretsStore(secretPersistence: MemorySecretPersistence): SecretCoordinate {
    return secretPersistence.map.keys.first()
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testWriteWorkspaceServiceAccount() {
    val workspaceId = WORKSPACE_ID
    val jsonSecretPayload: String = MockData.MOCK_SERVICE_ACCOUNT_1
    val workspaceServiceAccount =
      WorkspaceServiceAccount()
        .withWorkspaceId(workspaceId)
        .withHmacKey(MockData.HMAC_SECRET_PAYLOAD_1)
        .withServiceAccountId("a1e5ac98-7531-48e1-943b-b46636")
        .withServiceAccountEmail("a1e5ac98-7531-48e1-943b-b46636@random-gcp-project.abc.abcdefghijklmno.com")
        .withJsonCredential(Jsons.deserialize(jsonSecretPayload))
    every {
      configRepository.getWorkspaceServiceAccountNoSecrets(workspaceId)
    } throws ConfigNotFoundException(ConfigSchema.WORKSPACE_SERVICE_ACCOUNT, workspaceId.toString())
    every { configRepository.writeWorkspaceServiceAccountNoSecrets(any()) } returns Unit
    secretsRepositoryWriter.writeServiceAccountJsonCredentials(workspaceServiceAccount)
    Assertions.assertEquals(2, secretPersistence!!.map.size)
    var jsonPayloadInPersistence: String? = null
    var hmacPayloadInPersistence: String? = null
    var jsonSecretCoordinate: SecretCoordinate? = null
    var hmacSecretCoordinate: SecretCoordinate? = null
    for ((key, value) in secretPersistence.map) {
      if (key.fullCoordinate.contains("json")) {
        jsonSecretCoordinate = key
        jsonPayloadInPersistence = value
      } else if (key.fullCoordinate.contains("hmac")) {
        hmacSecretCoordinate = key
        hmacPayloadInPersistence = value
      } else {
        throw RuntimeException("")
      }
    }
    Assertions.assertNotNull(jsonPayloadInPersistence)
    Assertions.assertNotNull(hmacPayloadInPersistence)
    Assertions.assertNotNull(jsonSecretCoordinate)
    Assertions.assertNotNull(hmacSecretCoordinate)
    Assertions.assertEquals(jsonSecretPayload, jsonPayloadInPersistence)
    Assertions.assertEquals(
      MockData.HMAC_SECRET_PAYLOAD_1.toString(),
      hmacPayloadInPersistence,
    )
    var workspaceServiceAccountClone =
      Jsons.clone(
        workspaceServiceAccount.withJsonCredential(
          Jsons.jsonNode(
            Map.of(
              PASSWORD_FIELD_NAME,
              jsonSecretCoordinate!!.fullCoordinate,
            ),
          ),
        )
          .withHmacKey(Jsons.jsonNode(Map.of(PASSWORD_FIELD_NAME, hmacSecretCoordinate!!.fullCoordinate))),
      )
    verify { configRepository.writeWorkspaceServiceAccountNoSecrets(workspaceServiceAccountClone) }
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testWriteSameStagingConfiguration() {
    val configRepository: ConfigRepository = mockk()
    val secretPersistence: SecretPersistence = mockk()
    val secretsRepositoryWriter = spyk(SecretsRepositoryWriter(configRepository, jsonSchemaValidator, secretPersistence))
    val workspaceId = UUID.fromString("13fb9a84-6bfa-4801-8f5e-ce717677babf")
    val jsonSecretPayload: String = MockData.MOCK_SERVICE_ACCOUNT_1
    val workspaceServiceAccount =
      WorkspaceServiceAccount().withWorkspaceId(workspaceId).withHmacKey(
        MockData.HMAC_SECRET_PAYLOAD_1,
      )
        .withServiceAccountId("a1e5ac98-7531-48e1-943b-b46636")
        .withServiceAccountEmail("a1e5ac98-7531-48e1-943b-b46636@random-gcp-project.abc.abcdefghijklmno.com")
        .withJsonCredential(Jsons.deserialize(jsonSecretPayload))
    val jsonSecretCoordinate =
      SecretCoordinate(
        "service_account_json_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        1,
      )
    val hmacSecretCoordinate =
      SecretCoordinate(
        "service_account_hmac_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        1,
      )
    val cloned =
      Jsons.clone(workspaceServiceAccount)
        .withJsonCredential(Jsons.jsonNode(Map.of(PASSWORD_FIELD_NAME, jsonSecretCoordinate.fullCoordinate)))
        .withHmacKey(Jsons.jsonNode(Map.of(PASSWORD_FIELD_NAME, hmacSecretCoordinate.fullCoordinate)))
    every { configRepository.getWorkspaceServiceAccountNoSecrets(workspaceId) } returns cloned
    every { configRepository.writeWorkspaceServiceAccountNoSecrets(any()) } returns Unit
    every { secretPersistence.read(jsonSecretCoordinate) } returns jsonSecretPayload
    every { secretPersistence.read(hmacSecretCoordinate) } returns MockData.HMAC_SECRET_PAYLOAD_1.toString()
    every { secretPersistence.write(any(), any()) } returns Unit

    secretsRepositoryWriter.writeServiceAccountJsonCredentials(workspaceServiceAccount)
    val coordinates = mutableListOf<SecretCoordinate>()
    val payloads = mutableListOf<String>()
    verify(exactly = 2) { secretPersistence.write(capture(coordinates), capture(payloads)) }
    Assertions.assertEquals(2, coordinates.size)
    Assertions.assertEquals(listOf(jsonSecretCoordinate, hmacSecretCoordinate), coordinates)
    Assertions.assertEquals(2, payloads.size)
    Assertions.assertTrue(payloads.contains(jsonSecretPayload))
    Assertions.assertTrue(payloads.contains(MockData.HMAC_SECRET_PAYLOAD_1.toString()))
    verify { secretPersistence.write(hmacSecretCoordinate, MockData.HMAC_SECRET_PAYLOAD_1.toString()) }
    verify { configRepository.writeWorkspaceServiceAccountNoSecrets(cloned) }
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testWriteDifferentStagingConfiguration() {
    val configRepository: ConfigRepository = mockk()
    val secretPersistence: SecretPersistence = mockk()
    val secretsRepositoryWriter = spyk(SecretsRepositoryWriter(configRepository, jsonSchemaValidator, secretPersistence))
    val workspaceId = UUID.fromString("13fb9a84-6bfa-4801-8f5e-ce717677babf")
    val jsonSecretOldPayload: String = MockData.MOCK_SERVICE_ACCOUNT_1
    val jsonSecretNewPayload: String = MockData.MOCK_SERVICE_ACCOUNT_2
    val workspaceServiceAccount =
      WorkspaceServiceAccount()
        .withWorkspaceId(workspaceId)
        .withHmacKey(MockData.HMAC_SECRET_PAYLOAD_2)
        .withServiceAccountId("a1e5ac98-7531-48e1-943b-b46636")
        .withServiceAccountEmail("a1e5ac98-7531-48e1-943b-b46636@random-gcp-project.abc.abcdefghijklmno.com")
        .withJsonCredential(Jsons.deserialize(jsonSecretNewPayload))
    val jsonSecretOldCoordinate =
      SecretCoordinate(
        "service_account_json_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        1,
      )
    val hmacSecretOldCoordinate =
      SecretCoordinate(
        "service_account_hmac_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        1,
      )
    val cloned =
      Jsons.clone(workspaceServiceAccount)
        .withJsonCredential(Jsons.jsonNode(Map.of(PASSWORD_FIELD_NAME, jsonSecretOldCoordinate.fullCoordinate)))
        .withHmacKey(Jsons.jsonNode(Map.of(PASSWORD_FIELD_NAME, hmacSecretOldCoordinate.fullCoordinate)))
    every { configRepository.getWorkspaceServiceAccountNoSecrets(workspaceId) } returns cloned
    every { configRepository.writeWorkspaceServiceAccountNoSecrets(any()) } returns Unit
    every { secretPersistence.read(hmacSecretOldCoordinate) } returns MockData.HMAC_SECRET_PAYLOAD_1.toString()
    every { secretPersistence.read(jsonSecretOldCoordinate) } returns jsonSecretOldPayload
    every { secretPersistence.write(any(), any()) } returns Unit
    secretsRepositoryWriter.writeServiceAccountJsonCredentials(workspaceServiceAccount)
    val jsonSecretNewCoordinate =
      SecretCoordinate(
        "service_account_json_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        2,
      )
    val hmacSecretNewCoordinate =
      SecretCoordinate(
        "service_account_hmac_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        2,
      )

    val coordinates = mutableListOf<SecretCoordinate>()
    val payloads = mutableListOf<String>()
    verify(exactly = 2) { secretPersistence.write(capture(coordinates), capture(payloads)) }
    Assertions.assertEquals(2, coordinates.size)
    Assertions.assertEquals(listOf(jsonSecretNewCoordinate, hmacSecretNewCoordinate), coordinates)
    Assertions.assertEquals(2, payloads.size)
    Assertions.assertTrue(payloads.contains(jsonSecretNewPayload))
    Assertions.assertTrue(payloads.contains(MockData.HMAC_SECRET_PAYLOAD_2.toString()))
    var workspaceServiceAccountClone =
      Jsons.clone(workspaceServiceAccount).withJsonCredential(
        Jsons.jsonNode(
          Map.of(PASSWORD_FIELD_NAME, jsonSecretNewCoordinate.fullCoordinate),
        ),
      ).withHmacKey(
        Jsons.jsonNode(
          Map.of(PASSWORD_FIELD_NAME, hmacSecretNewCoordinate.fullCoordinate),
        ),
      )
    verify { configRepository.writeWorkspaceServiceAccountNoSecrets(workspaceServiceAccountClone) }
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun `writeWorkspace should ensure that secret fields are replaced`() {
    val configRepository: ConfigRepository = mockk()
    val secretPersistence: SecretPersistence = mockk()
    val secretsRepositoryWriter = spyk(SecretsRepositoryWriter(configRepository, jsonSchemaValidator, secretPersistence))
    val webhookConfigs =
      WebhookOperationConfigs().withWebhookConfigs(
        listOf(WebhookConfig().withName(TEST_WEBHOOK_NAME).withAuthToken(TEST_AUTH_TOKEN).withId(UUID.randomUUID())),
      )
    val workspace =
      StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withCustomerId(UUID.randomUUID())
        .withEmail(TEST_EMAIL)
        .withName(TEST_WORKSPACE_NAME)
        .withSlug(TEST_WORKSPACE_SLUG)
        .withInitialSetupComplete(false)
        .withDisplaySetupWizard(true)
        .withNews(false)
        .withAnonymousDataCollection(false)
        .withSecurityUpdates(false)
        .withTombstone(false)
        .withNotifications(emptyList())
        .withDefaultGeography(Geography.AUTO) // Serialize it to a string, then deserialize it to a JsonNode.
        .withWebhookOperationConfigs(Jsons.jsonNode(webhookConfigs))

    every { configRepository.getStandardWorkspaceNoSecrets(any(), any()) } returns workspace
    every { configRepository.writeStandardWorkspaceNoSecrets(any()) } returns Unit
    every { jsonSchemaValidator.ensure(any(), any()) } returns Unit
    every { secretPersistence.write(any(), any()) } returns Unit

    secretsRepositoryWriter.writeWorkspace(workspace)
    val workspaceArgument = slot<StandardWorkspace>()
    verify(exactly = 1) { configRepository.writeStandardWorkspaceNoSecrets(capture(workspaceArgument)) }
    Assertions.assertFalse(
      Jsons.serialize(workspaceArgument.captured.webhookOperationConfigs).contains(TEST_AUTH_TOKEN),
    )
  }

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
