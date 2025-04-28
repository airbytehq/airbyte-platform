/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.function.Consumer

class ArrayOneOfTestCase : SecretsTestCase {
  override val name: String
    get() = "array_of_oneof"

  override val firstSecretMap: Map<AirbyteManagedSecretCoordinate, String>
    get() =
      mapOf(
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1) to "hunter1",
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1) to "hunter2",
      )
  override val secondSecretMap: Map<AirbyteManagedSecretCoordinate, String>
    get() {
      return mapOf(
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "hunter3",
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 2) to "hunter4",
      )
    }
  override val persistenceUpdater: Consumer<SecretPersistence>
    get() {
      return Consumer<SecretPersistence> { secretPersistence: SecretPersistence ->
        secretPersistence.write(
          AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1),
          "hunter1",
        )
        secretPersistence.write(
          AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1),
          "hunter2",
        )
      }
    }
}
