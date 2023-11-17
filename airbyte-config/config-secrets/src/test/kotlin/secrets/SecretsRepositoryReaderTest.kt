/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.WorkspaceServiceAccount
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.ConfigRepository
import io.airbyte.config.persistence.MockData
import io.airbyte.config.secrets.hydration.RealSecretsHydrator
import io.airbyte.config.secrets.hydration.SecretsHydrator
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

private val UUID1 = UUID.randomUUID()
private val COORDINATE = SecretCoordinate("pointer", 2)
private const val SECRET = "abc"
private val PARTIAL_CONFIG = Jsons.deserialize("{ \"username\": \"airbyte\", \"password\": { \"_secret\": \"${COORDINATE.fullCoordinate}\" } }")
private val FULL_CONFIG = Jsons.deserialize("{ \"username\": \"airbyte\", \"password\": \"${SECRET}\"}")
private const val KEY = "_secret"
private const val SERVICE_ACCT_EMAIL = "a1e5ac98-7531-48e1-943b-b46636@random-gcp-project.abc.abcdefghijklmno.com"
private const val SERVICE_ACCT_ID = "a1e5ac98-7531-48e1-943b-b46636"
private val SOURCE_WITH_PARTIAL_CONFIG =
  SourceConnection()
    .withSourceId(UUID1)
    .withConfiguration(PARTIAL_CONFIG)
private val SOURCE_WITH_FULL_CONFIG =
  Jsons.clone(SOURCE_WITH_PARTIAL_CONFIG)
    .withConfiguration(FULL_CONFIG)
private val DESTINATION_WITH_PARTIAL_CONFIG =
  DestinationConnection()
    .withDestinationId(UUID1)
    .withConfiguration(PARTIAL_CONFIG)
private val DESTINATION_WITH_FULL_CONFIG =
  Jsons.clone(DESTINATION_WITH_PARTIAL_CONFIG)
    .withConfiguration(FULL_CONFIG)

class SecretsRepositoryReaderTest {
  private lateinit var configRepository: ConfigRepository
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var secretPersistence: MemorySecretPersistence

  @BeforeEach
  fun setup() {
    configRepository = mockk()
    secretPersistence = MemorySecretPersistence()
    val secretsHydrator: SecretsHydrator = RealSecretsHydrator(secretPersistence)
    secretsRepositoryReader = SecretsRepositoryReader(configRepository, secretsHydrator)
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testGetSourceWithSecrets() {
    secretPersistence.write(COORDINATE, SECRET)
    every { configRepository.getSourceConnection(UUID1) } returns SOURCE_WITH_PARTIAL_CONFIG
    Assertions.assertEquals(
      SOURCE_WITH_FULL_CONFIG,
      secretsRepositoryReader.getSourceConnectionWithSecrets(UUID1),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testListSourcesWithSecrets() {
    secretPersistence.write(COORDINATE, SECRET)
    every { configRepository.listSourceConnection() } returns listOf(SOURCE_WITH_PARTIAL_CONFIG)
    Assertions.assertEquals(
      listOf(SOURCE_WITH_FULL_CONFIG),
      secretsRepositoryReader.listSourceConnectionWithSecrets(),
    )
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testGetDestinationWithSecrets() {
    secretPersistence.write(COORDINATE, SECRET)
    every { configRepository.getDestinationConnection(UUID1) } returns DESTINATION_WITH_PARTIAL_CONFIG
    Assertions.assertEquals(
      DESTINATION_WITH_FULL_CONFIG,
      secretsRepositoryReader.getDestinationConnectionWithSecrets(UUID1),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testListDestinationsWithSecrets() {
    secretPersistence.write(COORDINATE, SECRET)
    every { configRepository.listDestinationConnection() } returns listOf(DESTINATION_WITH_PARTIAL_CONFIG)
    Assertions.assertEquals(
      listOf(DESTINATION_WITH_FULL_CONFIG),
      secretsRepositoryReader.listDestinationConnectionWithSecrets(),
    )
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testReadingServiceAccount() {
    val configRepository: ConfigRepository = mockk()
    val secretPersistence: SecretPersistence = mockk()
    val realSecretsHydrator = RealSecretsHydrator(secretPersistence)
    val secretsRepositoryReader = spyk(SecretsRepositoryReader(configRepository, realSecretsHydrator))
    val workspaceId = UUID.fromString("13fb9a84-6bfa-4801-8f5e-ce717677babf")
    val jsonSecretPayload = MockData.MOCK_SERVICE_ACCOUNT_1
    val secretCoordinateHmac =
      SecretCoordinate(
        "service_account_hmac_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        1,
      )
    val secretCoordinateJson =
      SecretCoordinate(
        "service_account_json_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_6b894c2b-71dc-4481-bd9f-572402643cf9",
        1,
      )
    val workspaceServiceAccount =
      WorkspaceServiceAccount()
        .withWorkspaceId(workspaceId)
        .withHmacKey(Jsons.jsonNode(mapOf(KEY to secretCoordinateHmac.fullCoordinate)))
        .withJsonCredential(
          Jsons.jsonNode(mapOf(KEY to secretCoordinateJson.fullCoordinate)),
        )
        .withServiceAccountEmail(SERVICE_ACCT_EMAIL)
        .withServiceAccountId(SERVICE_ACCT_ID)
    every { configRepository.getWorkspaceServiceAccountNoSecrets(workspaceId) } returns workspaceServiceAccount
    every { secretPersistence.read(secretCoordinateHmac) } returns MockData.HMAC_SECRET_PAYLOAD_1.toString()
    every { secretPersistence.read(secretCoordinateJson) } returns jsonSecretPayload
    val actual = secretsRepositoryReader.getWorkspaceServiceAccountWithSecrets(workspaceId)
    val expected =
      WorkspaceServiceAccount().withWorkspaceId(workspaceId)
        .withJsonCredential(Jsons.deserialize(jsonSecretPayload)).withHmacKey(MockData.HMAC_SECRET_PAYLOAD_1)
        .withServiceAccountId(SERVICE_ACCT_ID)
        .withServiceAccountEmail(SERVICE_ACCT_EMAIL)
    Assertions.assertEquals(expected, actual)
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testReadingServiceAccountWithJsonNull() {
    val configRepository: ConfigRepository = mockk()
    val secretPersistence: SecretPersistence = mockk()
    val realSecretsHydrator = RealSecretsHydrator(secretPersistence)
    val secretsRepositoryReader = spyk(SecretsRepositoryReader(configRepository, realSecretsHydrator))
    val workspaceId = UUID.fromString("13fb9a84-6bfa-4801-8f5e-ce717677babf")
    val secretCoordinateHmac =
      SecretCoordinate(
        "service_account_hmac_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_e86e2eab-af9b-42a3-b074-b923b4fa617e",
        1,
      )
    val workspaceServiceAccount =
      WorkspaceServiceAccount()
        .withWorkspaceId(workspaceId)
        .withHmacKey(Jsons.jsonNode(mapOf(KEY to secretCoordinateHmac.fullCoordinate)))
        .withServiceAccountEmail(SERVICE_ACCT_EMAIL)
        .withServiceAccountId(SERVICE_ACCT_ID)
    every { configRepository.getWorkspaceServiceAccountNoSecrets(workspaceId) } returns workspaceServiceAccount
    every { secretPersistence.read(secretCoordinateHmac) } returns MockData.HMAC_SECRET_PAYLOAD_1.toString()
    val actual = secretsRepositoryReader.getWorkspaceServiceAccountWithSecrets(workspaceId)
    val expected =
      WorkspaceServiceAccount().withWorkspaceId(workspaceId)
        .withHmacKey(MockData.HMAC_SECRET_PAYLOAD_1)
        .withServiceAccountId(SERVICE_ACCT_ID)
        .withServiceAccountEmail(SERVICE_ACCT_EMAIL)
    Assertions.assertEquals(expected, actual)
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun testReadingServiceAccountWithHmacNull() {
    val configRepository: ConfigRepository = mockk()
    val secretPersistence: SecretPersistence = mockk()
    val realSecretsHydrator = RealSecretsHydrator(secretPersistence)
    val secretsRepositoryReader = spyk(SecretsRepositoryReader(configRepository, realSecretsHydrator))
    val workspaceId = UUID.fromString("13fb9a84-6bfa-4801-8f5e-ce717677babf")
    val jsonSecretPayload = MockData.MOCK_SERVICE_ACCOUNT_1
    val secretCoordinateJson =
      SecretCoordinate(
        "service_account_json_13fb9a84-6bfa-4801-8f5e-ce717677babf_secret_6b894c2b-71dc-4481-bd9f-572402643cf9",
        1,
      )
    val workspaceServiceAccount =
      WorkspaceServiceAccount()
        .withWorkspaceId(workspaceId)
        .withJsonCredential(Jsons.jsonNode(mapOf(KEY to secretCoordinateJson.fullCoordinate)))
        .withServiceAccountEmail(SERVICE_ACCT_EMAIL)
        .withServiceAccountId(SERVICE_ACCT_ID)
    every { configRepository.getWorkspaceServiceAccountNoSecrets(workspaceId) } returns workspaceServiceAccount
    every { secretPersistence.read(secretCoordinateJson) } returns jsonSecretPayload
    val actual = secretsRepositoryReader.getWorkspaceServiceAccountWithSecrets(workspaceId)
    val expected =
      WorkspaceServiceAccount().withWorkspaceId(workspaceId)
        .withJsonCredential(Jsons.deserialize(jsonSecretPayload))
        .withServiceAccountId(SERVICE_ACCT_ID)
        .withServiceAccountEmail(SERVICE_ACCT_EMAIL)
    Assertions.assertEquals(expected, actual)
  }
}
