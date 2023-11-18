/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.persistence.ConfigRepository
import io.airbyte.config.secrets.hydration.RealSecretsHydrator
import io.airbyte.config.secrets.hydration.SecretsHydrator
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
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
    secretsRepositoryReader = SecretsRepositoryReader(secretsHydrator)
  }
}
