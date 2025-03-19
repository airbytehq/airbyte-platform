/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.JsonPaths
import io.airbyte.commons.json.JsonSchemas
import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.persistence.ReadOnlySecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import secrets.persistence.SecretCoordinateException
import java.util.UUID
import java.util.function.Supplier

private val logger = KotlinLogging.logger {}

/**
 * Contains most of the core logic surrounding secret coordinate extraction and insertion.
 *
 * These are the three main helpers provided by this class:
 * [SecretsHelpers.splitConfig]
 * [SecretsHelpers.splitAndUpdateConfig]
 * [SecretsHelpers.combineConfig]
 *
 * Here's an overview on some terminology used in this class:
 *
 * A "full config" represents an entire config as specified by an end user.
 *
 * A "partial config" represents a config where any string that was specified as an airbyte_secret
 * in the specification is replaced by a JSON object {"_secret": "secret coordinate"} that can later
 * be used to reconstruct the "full config".
 *
 * A [SecretPersistence] provides the ability to read and write secrets to a backing store
 * such as Google Secrets Manager.
 *
 * A [SecretCoordinate] is a reference to a specific secret at a specific version in a
 * [SecretPersistence].
 */
object SecretsHelpers {
  private const val COORDINATE_FIELD = "_secret"

  /**
   * Used to separate secrets out of some configuration. This will output a partial config that
   * includes pointers to secrets instead of actual secret values and a map that can be used to update
   * a [SecretPersistence] at coordinates with values from the full config.
   *
   * @param workspaceId workspace used for this config
   * @param fullConfig config including secrets
   * @param spec specification for the config
   * @param secretPersistence secret persistence to be used for r/w. Could be a runtime secret persistence.
   * @return a partial config + a map of coordinates to secret payloads
   */
  fun splitConfig(
    workspaceId: UUID,
    fullConfig: JsonNode,
    spec: JsonNode?,
    secretPersistence: SecretPersistence,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      { UUID.randomUUID() },
      workspaceId,
      secretPersistence,
      Jsons.emptyObject(),
      fullConfig,
      spec,
    )

  /**
   * Used to separate secrets out of some configuration. This will output a partial config that
   * includes pointers to secrets instead of actual secret values and a map that can be used to update
   * a [SecretPersistence] at coordinates with values from the full config.
   *
   * @param uuidSupplier provided to allow a test case to produce known UUIDs in order for easy
   * fixture creation.
   * @param workspaceId workspace used for this config
   * @param fullConfig config including secrets
   * @param spec specification for the config
   * @return a partial config + a map of coordinates to secret payloads
   */
  fun splitConfig(
    uuidSupplier: Supplier<UUID>,
    workspaceId: UUID,
    fullConfig: JsonNode,
    spec: JsonNode?,
    secretPersistence: SecretPersistence,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      uuidSupplier,
      workspaceId,
      secretPersistence,
      Jsons.emptyObject(),
      fullConfig,
      spec,
    )

  /**
   * Used to separate secrets out of a configuration and output a partial config that includes
   * pointers to secrets instead of actual secret values and a map that can be used to update a
   * [SecretPersistence] at coordinates with values from the full config. If a previous config
   * for this configuration is provided, this method attempts to use the same base coordinates to
   * refer to the same secret and increment the version of the coordinate used to reference a secret.
   *
   * @param workspaceId workspace used for this config
   * @param oldPartialConfig previous partial config for this specific configuration
   * @param newFullConfig new config containing secrets that will be used to update the partial config
   * @param spec specification that should match both the previous partial config after filling in
   * coordinates and the new full config.
   * @param secretReader provides a way to determine if a secret is the same or updated at a specific
   * location in a config
   * @return a partial config + a map of coordinates to secret payloads
   */
  fun splitAndUpdateConfig(
    workspaceId: UUID,
    oldPartialConfig: JsonNode?,
    newFullConfig: JsonNode,
    spec: JsonNode?,
    secretReader: ReadOnlySecretPersistence,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      { UUID.randomUUID() },
      workspaceId,
      secretReader,
      oldPartialConfig,
      newFullConfig,
      spec,
    )

  /**
   * Identical to [SecretsHelpers.splitAndUpdateConfig] with UUID supplier for testing.
   */
  @VisibleForTesting
  fun splitAndUpdateConfig(
    uuidSupplier: Supplier<UUID>,
    workspaceId: UUID,
    oldPartialConfig: JsonNode?,
    newFullConfig: JsonNode,
    spec: JsonNode?,
    secretReader: ReadOnlySecretPersistence,
  ): SplitSecretConfig = internalSplitAndUpdateConfig(uuidSupplier, workspaceId, secretReader, oldPartialConfig, newFullConfig, spec)

  /**
   * Replaces {"_secret": "full_coordinate"} objects in the partial config with the string secret
   * payloads loaded from the secret persistence at those coordinates.
   *
   * @param partialConfig configuration containing secret coordinates (references to secrets)
   * @param secretPersistence secret storage mechanism
   * @return full config including actual secret values
   */
  fun combineConfig(
    partialConfig: JsonNode?,
    secretPersistence: ReadOnlySecretPersistence,
  ): JsonNode {
    return if (partialConfig != null) {
      val config = partialConfig.deepCopy<JsonNode>()

      // if the entire config is a secret coordinate object
      if (config.has(COORDINATE_FIELD)) {
        val coordinateNode = config[COORDINATE_FIELD]
        val coordinate: SecretCoordinate = getCoordinateFromTextNode(coordinateNode)
        return TextNode(getOrThrowSecretValue(secretPersistence, coordinate))
      }

      // otherwise iterate through all object fields
      config.fields().forEachRemaining { (fieldName, fieldNode): Map.Entry<String, JsonNode> ->
        if (fieldNode is ArrayNode) {
          for (i in 0 until fieldNode.size()) {
            fieldNode[i] = combineConfig(fieldNode[i], secretPersistence)
          }
        } else if (fieldNode is ObjectNode) {
          (config as ObjectNode).replace(fieldName, combineConfig(fieldNode, secretPersistence))
        }
      }
      config
    } else {
      JsonNodeFactory.instance.objectNode()
    }
  }

  /**
   * This returns all the unique path to the airbyte secrets based on a schema spec. The path will be
   * return in an ascending alphabetical order.
   */
  @VisibleForTesting
  fun getSortedSecretPaths(spec: JsonNode?): List<String> =
    JsonSchemas
      .collectPathsThatMeetCondition(
        spec,
      ) { node: JsonNode ->
        node
          .fields()
          .asSequence()
          .toList()
          .stream()
          .anyMatch { (key): Map.Entry<String, JsonNode> -> AirbyteSecretConstants.AIRBYTE_SECRET_FIELD == key }
      }.stream()
      .map { jsonSchemaPath: List<JsonSchemas.FieldNameOrList?>? ->
        JsonPaths.mapJsonSchemaPathToJsonPath(
          jsonSchemaPath,
        )
      }.distinct()
      .sorted()
      .toList()

  fun getExistingCoordinateIfExists(json: JsonNode?): String? =
    if (json != null && json.has(COORDINATE_FIELD)) {
      json[COORDINATE_FIELD].asText()
    } else {
      null
    }

  /**
   * Internal method used to support both "split config" and "split and update config" operations.
   *
   * For splits that don't have a prior partial config (such as when a connector is created for a
   * source or destination for the first time), the secret reader and old partial config can be set to
   * empty (see [SecretsHelpers.splitConfig]).
   *
   * IMPORTANT: This is used recursively. In the process, the partial config, full config, and spec
   * inputs will represent internal json structures, not the entire configs/specs.
   *
   * @param uuidSupplier provided to allow a test case to produce known UUIDs in order for easy
   * fixture creation
   * @param workspaceId workspace that will contain the source or destination this config will be
   * stored for
   * @param secretReader provides a way to determine if a secret is the same or updated at a specific
   * location in a config
   * @param persistedPartialConfig previous partial config for this specific configuration
   * @param newFullConfig new config containing secrets that will be used to update the partial config
   * @param spec config specification
   * @return a partial config + a map of coordinates to secret payloads
   */
  private fun internalSplitAndUpdateConfig(
    uuidSupplier: Supplier<UUID>,
    workspaceId: UUID,
    secretReader: ReadOnlySecretPersistence,
    persistedPartialConfig: JsonNode?,
    newFullConfig: JsonNode,
    spec: JsonNode?,
  ): SplitSecretConfig {
    var fullConfigCopy = newFullConfig.deepCopy<JsonNode>()
    val secretMap: HashMap<SecretCoordinate, String> = HashMap()
    val paths = getSortedSecretPaths(spec)
    logger.debug { "SortedSecretPaths: $paths" }
    for (path in paths) {
      fullConfigCopy =
        JsonPaths.replaceAt(fullConfigCopy, path) { json: JsonNode, pathOfNode: String? ->
          val persistedNode = JsonPaths.getSingleValue(persistedPartialConfig, pathOfNode).orElse(null)
          val existingCoordinate = getExistingCoordinateIfExists(persistedNode)
          val coordinate: SecretCoordinate = getCoordinate(secretReader, workspaceId, uuidSupplier, existingCoordinate)
          secretMap[coordinate] = json.asText()
          Jsons.jsonNode(mapOf(COORDINATE_FIELD to coordinate.fullCoordinate))
        }
    }
    return SplitSecretConfig(fullConfigCopy, secretMap)
  }

  /**
   * Extracts a secret value from the persistence and throws an exception if the secret is not
   * available.
   *
   * @param secretPersistence storage layer for secrets
   * @param coordinate reference to a secret in the persistence
   * @return a json string containing the secret value or a JSON
   * @throws SecretCoordinateException when a secret at that coordinate is not available in the persistence
   */
  @Throws(SecretCoordinateException::class)
  private fun getOrThrowSecretValue(
    secretPersistence: ReadOnlySecretPersistence,
    coordinate: SecretCoordinate,
  ): String {
    val secret: String = secretPersistence.read(coordinate)
    if (secret.isNotBlank()) {
      return secret
    } else {
      throw SecretCoordinateException(
        String.format(
          "That secret was not found in the store! Coordinate: %s",
          coordinate.fullCoordinate,
        ),
      )
    }
  }

  private fun getCoordinateFromTextNode(node: JsonNode): SecretCoordinate = SecretCoordinate.fromFullCoordinate(node.asText())

  /**
   * Same as #getCoordinate but with a consistent base. For Production use.
   */
  internal fun getCoordinate(
    secretReader: ReadOnlySecretPersistence,
    workspaceId: UUID,
    uuidSupplier: Supplier<UUID>,
    oldSecretFullCoordinate: String?,
  ): SecretCoordinate =
    getSecretCoordinate(
      "airbyte_workspace_",
      secretReader,
      workspaceId,
      uuidSupplier,
      oldSecretFullCoordinate,
    )

  /**
   * Determines which coordinate base and version to use based off of an old version that may exist in
   * the secret persistence.
   *
   * If the secret does not exist in the persistence, version 1 will be used.
   *
   * If the previous secret exists, return the version incremented by 1.
   *
   * @param secretBasePrefix prefix for the secret base
   * @param secretReader secret persistence
   * @param secretBaseId workspace used for this config
   * @param uuidSupplier provided to allow a test case to produce known UUIDs in order for easy
   * fixture creation.
   * @param oldSecretFullCoordinate a nullable full coordinate (base+version) retrieved from the
   * previous config
   * @return a coordinate (versioned reference to where the secret is stored in the persistence)
   */
  @VisibleForTesting
  fun getSecretCoordinate(
    secretBasePrefix: String,
    secretReader: ReadOnlySecretPersistence,
    secretBaseId: UUID,
    uuidSupplier: Supplier<UUID>,
    oldSecretFullCoordinate: String?,
  ): SecretCoordinate {
    var coordinateBase: String? = null
    var version = 1L

    if (oldSecretFullCoordinate != null) {
      val oldCoordinate: SecretCoordinate = SecretCoordinate.fromFullCoordinate(oldSecretFullCoordinate)
      coordinateBase = oldCoordinate.coordinateBase
      val oldSecretValue: String = secretReader.read(oldCoordinate)
      if (oldSecretValue.isNotEmpty()) {
        version = oldCoordinate.version.inc()
      }
    }

    if (coordinateBase == null) {
      // IMPORTANT: format of this cannot be changed without introducing migrations for secrets
      // persistence
      coordinateBase = getCoordinatorBase(secretBasePrefix, secretBaseId, uuidSupplier)
    }

    return SecretCoordinate(coordinateBase, version)
  }

  fun getCoordinatorBase(
    secretBasePrefix: String,
    secretBaseId: UUID,
    uuidSupplier: Supplier<UUID>,
  ): String = "${secretBasePrefix}${secretBaseId}_secret_${uuidSupplier.get()}"

  /**
   * Takes in the secret coordinate in form of a JSON and fetches the secret from the store.
   *
   * @param secretCoordinateAsJson The co-ordinate at which we expect the secret value to be present
   * in the secret persistence
   * @param readOnlySecretPersistence The secret persistence
   * @return Original secret value as JsonNode
   */
  fun hydrateSecretCoordinate(
    secretCoordinateAsJson: JsonNode,
    readOnlySecretPersistence: ReadOnlySecretPersistence,
  ): JsonNode {
    val secretCoordinate: SecretCoordinate =
      getCoordinateFromTextNode(
        secretCoordinateAsJson[COORDINATE_FIELD],
      )
    return Jsons.deserialize(getOrThrowSecretValue(readOnlySecretPersistence, secretCoordinate))
  }
}
