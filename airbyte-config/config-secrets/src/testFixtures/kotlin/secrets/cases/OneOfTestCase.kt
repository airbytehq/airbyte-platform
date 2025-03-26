/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.function.Consumer

class OneOfTestCase : SecretsTestCase {
  override val name: String
    get() = "oneof"

  override val firstSecretMap: Map<SecretCoordinate, String>
    get() =
      mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1) to "hunter1",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1) to "hunter2",
      )

  override val secondSecretMap: Map<SecretCoordinate, String>
    get() {
      return mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "hunter3",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 2) to "hunter4",
      )
    }

  override val persistenceUpdater: Consumer<SecretPersistence>
    get() {
      return Consumer<SecretPersistence> { secretPersistence: SecretPersistence ->
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1), "hunter1")
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1), "hunter2")
      }
    }
}
