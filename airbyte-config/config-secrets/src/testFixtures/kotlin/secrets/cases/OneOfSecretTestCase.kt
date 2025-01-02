/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.function.Consumer

class OneOfSecretTestCase : SecretsTestCase {
  override val name: String
    get() = "oneof_secret"

  override val firstSecretMap: Map<SecretCoordinate, String>
    get() =
      mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1) to "access_token_1",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1) to "clientId_1",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 2), 1) to "client_secret_1",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 3), 1) to "refresh_token_1",
      )

  override val secondSecretMap: Map<SecretCoordinate, String>
    get() {
      return mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "access_token_2",
      )
    }

  override val persistenceUpdater: Consumer<SecretPersistence>
    get() {
      return Consumer<SecretPersistence> { secretPersistence: SecretPersistence ->
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1), "access_token_1")
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1), "clientId_1")
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 2), 1), "client_secret_1")
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 3), 1), "refresh_token_1")
      }
    }
}
