/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.function.Consumer

class OneOfSecretTestCase : SecretsTestCase {
  override val name: String
    get() = "oneof_secret"

  override val firstSecretMap: Map<AirbyteManagedSecretCoordinate, String>
    get() =
      mapOf(
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1) to "access_token_1",
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1) to "clientId_1",
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 2), 1) to "client_secret_1",
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 3), 1) to "refresh_token_1",
      )

  override val secondSecretMap: Map<AirbyteManagedSecretCoordinate, String>
    get() {
      return mapOf(
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "access_token_2",
      )
    }

  override val persistenceUpdater: Consumer<SecretPersistence>
    get() {
      return Consumer<SecretPersistence> { secretPersistence: SecretPersistence ->
        secretPersistence.write(AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1), "access_token_1")
        secretPersistence.write(AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1), "clientId_1")
        secretPersistence.write(AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 2), 1), "client_secret_1")
        secretPersistence.write(AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 3), 1), "refresh_token_1")
      }
    }
}
