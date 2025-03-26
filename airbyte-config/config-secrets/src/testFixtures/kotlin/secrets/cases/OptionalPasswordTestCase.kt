/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.function.Consumer

class OptionalPasswordTestCase : SecretsTestCase {
  override val name: String
    get() = "optional_password"

  override val firstSecretMap: Map<SecretCoordinate, String>
    get() = mapOf()

  override val secondSecretMap: Map<SecretCoordinate, String>
    get() = mapOf()

  override val persistenceUpdater: Consumer<SecretPersistence>
    get() = Consumer<SecretPersistence> { }
}
