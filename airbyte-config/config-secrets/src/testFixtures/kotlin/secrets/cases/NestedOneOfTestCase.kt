/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.test.cases

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsTestCase
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.function.Consumer

class NestedOneOfTestCase : SecretsTestCase {
  override val name: String
    get() = "nested_oneof"

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
  val updatedPartialConfigAfterUpdate1: JsonNode
    // the following helpers are for the custom test suite for evaluating updating individual secret
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(
          name,
          "updated_partial_config_update1.json",
        )
      }
    }
  val updatedPartialConfigAfterUpdate2: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(
          name,
          "updated_partial_config_update2.json",
        )
      }
    }
  val fullConfigUpdate1: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(
          name,
          "full_config_update1.json",
        )
      }
    }
  val fullConfigUpdate2: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(
          name,
          "full_config_update2.json",
        )
      }
    }
  val secretMapAfterUpdate1: Map<SecretCoordinate, String>
    get() {
      return mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "hunter3",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 1) to "hunter2",
      )
    }
  val secretMapAfterUpdate2: Map<SecretCoordinate, String>
    get() {
      return mapOf(
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 0), 2) to "hunter3",
        SecretCoordinate(SecretsTestCase.buildBaseCoordinate(uuidIndex = 1), 2) to "hunter4",
      )
    }
}
