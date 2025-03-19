/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.concurrent.Callable
import java.util.function.Consumer

class NestedObjectTestCase : SecretsTestCase {
  override val name: String
    get() = "nested_object"

  override val firstSecretMap: Map<SecretCoordinate, String>
    get() =
      mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1) to "hunter1",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1) to "hunter2",
      )

  override val secondSecretMap: Map<SecretCoordinate, String>
    get() {
      return mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 2) to "hunter3",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "hunter4",
      )
    }

  override val persistenceUpdater: Consumer<SecretPersistence>
    get() {
      return Consumer<SecretPersistence> { secretPersistence: SecretPersistence ->
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1), "hunter1")
        secretPersistence.write(SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 1), "hunter2")
      }
    }
  val updatedPartialConfigAfterUpdateTopLevel: JsonNode
    // the following helpers are for the custom test suite for evaluating updating individual secret
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(
          name,
          "updated_partial_config_update1.json",
        )
      }
    }
  val updatedPartialConfigAfterUpdateNested: JsonNode
    get() {
      return Exceptions.toRuntime(
        Callable {
          getNodeResource(
            name,
            "updated_partial_config_update2.json",
          )
        },
      )
    }
  val fullConfigUpdateTopLevel: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(
          name,
          "full_config_update1.json",
        )
      }
    }
  val fullConfigUpdateNested: JsonNode
    get() {
      return Exceptions.toRuntime(
        Callable {
          getNodeResource(
            name,
            "full_config_update2.json",
          )
        },
      )
    }
  val secretMapAfterUpdateTopLevel: Map<SecretCoordinate, String>
    get() {
      return mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 2) to "hunter3",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "hunter2",
      )
    }
  val secretMapAfterUpdateNested: Map<SecretCoordinate, String>
    get() {
      return mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 3) to "hunter3",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 3) to "hunter4",
      )
    }
}
