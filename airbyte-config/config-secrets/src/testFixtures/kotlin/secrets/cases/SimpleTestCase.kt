/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.function.Consumer

class SimpleTestCase : SecretsTestCase {
  override val name: String
    get() = "simple"

  override val firstSecretMap: Map<AirbyteManagedSecretCoordinate, String>
    get() =
      mapOf(
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(prefix = "airbyte_workspace_", secret = "_secret_", uuidIndex = 0), 1) to
          "hunter1",
      )

  override val secondSecretMap: Map<AirbyteManagedSecretCoordinate, String>
    get() {
      return mapOf(
        AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(prefix = "airbyte_workspace_", secret = "_secret_", uuidIndex = 0), 2) to
          "hunter2",
      )
    }

  override val persistenceUpdater: Consumer<SecretPersistence>
    get() {
      return Consumer<SecretPersistence> { secretPersistence: SecretPersistence ->
        secretPersistence.write(
          AirbyteManagedSecretCoordinate(SecretsTestCase.buildBaseCoordinate(prefix = "airbyte_workspace_", secret = "_secret_", uuidIndex = 0), 1),
          "hunter1",
        )
      }
    }
}
