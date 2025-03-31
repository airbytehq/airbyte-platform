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
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
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
  private const val SECRET_STORAGE_ID_FIELD = "_secret_storage_id"
  private const val SECRET_REF_ID_FIELD = "_secret_reference_id"

  /**
   * Used to separate secrets out of some configuration. This will output a partial config that
   * includes pointers to secrets instead of actual secret values and a map that can be used to update
   * a [SecretPersistence] at coordinates with values from the full config.
   *
   * @param secretBaseId id used for this config
   * @param fullConfig config including secrets
   * @param spec specification for the config
   * @param secretPersistence secret persistence to be used for r/w. Could be a runtime secret persistence.
   * @return a partial config + a map of coordinates to secret payloads
   */
  fun splitConfig(
    secretBaseId: UUID,
    fullConfig: JsonNode,
    spec: JsonNode?,
    secretPersistence: SecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      { UUID.randomUUID() },
      secretBaseId,
      secretPersistence,
      Jsons.emptyObject(),
      fullConfig,
      spec,
      secretBasePrefix,
    )

  /**
   * Used to separate secrets out of some configuration. This will output a partial config that
   * includes pointers to secrets instead of actual secret values and a map that can be used to update
   * a [SecretPersistence] at coordinates with values from the full config.
   *
   * @param uuidSupplier provided to allow a test case to produce known UUIDs in order for easy
   * fixture creation.
   * @param secretBaseId id used for this config
   * @param fullConfig config including secrets
   * @param spec specification for the config
   * @return a partial config + a map of coordinates to secret payloads
   */
  fun splitConfig(
    uuidSupplier: Supplier<UUID>,
    secretBaseId: UUID,
    fullConfig: JsonNode,
    spec: JsonNode?,
    secretPersistence: SecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      uuidSupplier,
      secretBaseId,
      secretPersistence,
      Jsons.emptyObject(),
      fullConfig,
      spec,
      secretBasePrefix,
    )

  /**
   * Used to separate secrets out of a configuration and output a partial config that includes
   * pointers to secrets instead of actual secret values and a map that can be used to update a
   * [SecretPersistence] at coordinates with values from the full config. If a previous config
   * for this configuration is provided, this method attempts to use the same base coordinates to
   * refer to the same secret and increment the version of the coordinate used to reference a secret.
   *
   * @param secretBaseId id used for this config
   * @param oldPartialConfig previous partial config for this specific configuration
   * @param newFullConfig new config containing secrets that will be used to update the partial config
   * @param spec specification that should match both the previous partial config after filling in
   * coordinates and the new full config.
   * @param secretReader provides a way to determine if a secret is the same or updated at a specific
   * location in a config
   * @return a partial config + a map of coordinates to secret payloads
   */
  fun splitAndUpdateConfig(
    secretBaseId: UUID,
    oldPartialConfig: JsonNode?,
    newFullConfig: JsonNode,
    spec: JsonNode?,
    secretReader: ReadOnlySecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      { UUID.randomUUID() },
      secretBaseId,
      secretReader,
      oldPartialConfig,
      newFullConfig,
      spec,
      secretBasePrefix,
    )

  /**
   * Identical to [SecretsHelpers.splitAndUpdateConfig] with UUID supplier for testing.
   */
  @VisibleForTesting
  fun splitAndUpdateConfig(
    uuidSupplier: Supplier<UUID>,
    secretBaseId: UUID,
    oldPartialConfig: JsonNode?,
    newFullConfig: JsonNode,
    spec: JsonNode?,
    secretReader: ReadOnlySecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(uuidSupplier, secretBaseId, secretReader, oldPartialConfig, newFullConfig, spec, secretBasePrefix)

  /**
   * Replaces {"_secret": "full_coordinate"} objects in the partial config with the string secret
   * payloads loaded from the secret persistence at those coordinates.
   *
   * @param configWithRefs configuration containing secret coordinates (references to secrets)
   * @param secretPersistence secret storage mechanism
   * @return full config including actual secret values
   */
  fun combineConfig(
    configWithRefs: ConfigWithSecretReferences?,
    secretPersistence: ReadOnlySecretPersistence,
  ): JsonNode = combineInlinedConfig(configWithRefs?.toInlined()?.value, secretPersistence)

  @VisibleForTesting
  internal fun combineInlinedConfig(
    partialConfig: JsonNode?,
    secretPersistence: ReadOnlySecretPersistence,
  ): JsonNode {
    // This should be updated to operate on ConfigWithSecretReferences instead of raw json nodes after legacy secrets are gone
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
            fieldNode[i] = combineInlinedConfig(fieldNode[i], secretPersistence)
          }
        } else if (fieldNode is ObjectNode) {
          (config as ObjectNode).replace(fieldName, combineInlinedConfig(fieldNode, secretPersistence))
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
   * @param secretBaseId base id that will contain the source or destination this config will be
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
    secretBaseId: UUID,
    secretReader: ReadOnlySecretPersistence,
    persistedPartialConfig: JsonNode?,
    newFullConfig: JsonNode,
    spec: JsonNode?,
    secretBasePrefix: String,
  ): SplitSecretConfig {
    var fullConfigCopy = newFullConfig.deepCopy<JsonNode>()
    val secretMap: HashMap<AirbyteManagedSecretCoordinate, String> = HashMap()
    val paths = getSortedSecretPaths(spec)
    logger.debug { "SortedSecretPaths: $paths" }
    for (path in paths) {
      fullConfigCopy =
        JsonPaths.replaceAt(fullConfigCopy, path) { json: JsonNode, pathOfNode: String? ->
          val persistedNode = JsonPaths.getSingleValue(persistedPartialConfig, pathOfNode).orElse(null)
          val existingCoordinate = getExistingCoordinateIfExists(persistedNode)
          val coordinate = getAirbyteManagedSecretCoordinate(secretBasePrefix, secretReader, secretBaseId, uuidSupplier, existingCoordinate)
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
   * Determines which coordinate base and version to use based off of an old version that may exist in
   * the secret persistence.
   *
   * If the secret does not exist in the persistence, version 1 will be used.
   *
   * If the previous secret exists, return the version incremented by 1.
   *
   * @param secretBasePrefix prefix for the secret base
   * @param secretReader secret persistence
   * @param secretBaseId id used for this config
   * @param uuidSupplier provided to allow a test case to produce known UUIDs in order for easy
   * fixture creation.
   * @param oldSecretFullCoordinate a nullable full coordinate (base+version) retrieved from the
   * previous config
   * @return a coordinate (versioned reference to where the secret is stored in the persistence)
   */
  @VisibleForTesting
  fun getAirbyteManagedSecretCoordinate(
    secretBasePrefix: String,
    secretReader: ReadOnlySecretPersistence,
    secretBaseId: UUID,
    uuidSupplier: Supplier<UUID>,
    oldSecretFullCoordinate: String?,
  ): AirbyteManagedSecretCoordinate {
    // Convert the full coordinate to a SecretCoordinate (if present) and ensure it’s an AirbyteManagedSecretCoordinate.
    val oldCoordinate = oldSecretFullCoordinate?.let { SecretCoordinate.fromFullCoordinate(it) } as? AirbyteManagedSecretCoordinate

    // If an old coordinate exists and the secret value isn't empty, increment its version.
    if (oldCoordinate != null && secretReader.read(oldCoordinate).isNotEmpty()) {
      return oldCoordinate.copy(version = oldCoordinate.version.inc())
    }

    // Otherwise, create a new coordinate with the default version.
    return AirbyteManagedSecretCoordinate(
      secretBasePrefix = secretBasePrefix,
      secretBaseId = secretBaseId,
      version = AirbyteManagedSecretCoordinate.DEFAULT_VERSION,
      uuidSupplier = uuidSupplier,
    )
  }

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

  /**
   * Internal helper object for specifically dealing with SecretReferences in the context of
   * configs.
   */
  object SecretReferenceHelpers {
    fun getSecretStorageIdFromConfig(config: ConfigWithSecretReferences): UUID? =
      config.referencedSecrets.values
        .mapNotNull { it.secretStorageId }
        .toSet()
        .let { secretStorageIds ->
          when {
            secretStorageIds.size > 1 -> throw IllegalStateException("Multiple secret storage IDs found in the config: $secretStorageIds")
            secretStorageIds.isNotEmpty() -> secretStorageIds.first()
            else -> null
          }
        }

    fun inlineSecretReferences(
      config: JsonNode,
      secretRefConfigs: Map<String, SecretReferenceConfig>,
    ): InlinedConfigWithSecretRefs {
      var inlinedJson = Jsons.clone(config)
      secretRefConfigs.forEach { (hydrationPath, secretRefConfig) ->
        val secretNode =
          Jsons.jsonNode(
            buildMap {
              put(COORDINATE_FIELD, secretRefConfig.secretCoordinate.fullCoordinate)
              secretRefConfig.secretStorageId?.let { put(SECRET_STORAGE_ID_FIELD, it) }
            },
          )

        inlinedJson =
          when (hydrationPath) {
            "$" -> secretNode
            else -> JsonPaths.replaceAt(inlinedJson, hydrationPath) { _: JsonNode, _: String? -> secretNode }
          }
      }

      return InlinedConfigWithSecretRefs(inlinedJson)
    }

    /**
     * Extracts all secret references from a config.
     */
    fun getReferenceMapFromConfig(config: InlinedConfigWithSecretRefs): Map<String, SecretReferenceConfig> =
      getReferenceMapFromInlinedConfig(config.value)

    private fun getReferenceMapFromInlinedConfig(
      config: JsonNode,
      path: String = "$",
    ): Map<String, SecretReferenceConfig> {
      if (config.isObject && config.has(COORDINATE_FIELD)) {
        val secretRefConfig =
          SecretReferenceConfig(
            secretCoordinate = SecretCoordinate.fromFullCoordinate(config[COORDINATE_FIELD].asText()),
            secretStorageId = config[SECRET_STORAGE_ID_FIELD]?.takeIf { it.isTextual }?.let { UUID.fromString(it.asText()) },
          )
        return mapOf(path to secretRefConfig)
      }

      val secretRefConfigs = mutableMapOf<String, SecretReferenceConfig>()
      config.fields().forEachRemaining { (key, value) ->
        if (value.isObject) {
          secretRefConfigs.putAll(getReferenceMapFromInlinedConfig(value, "$path.$key"))
        } else if (value.isArray) {
          for (i in 0 until value.size()) {
            secretRefConfigs.putAll(getReferenceMapFromInlinedConfig(value[i], "$path.$key[$i]"))
          }
        }
      }

      return secretRefConfigs
    }

    fun getSecretReferenceIdsFromConfig(config: JsonNode): Set<UUID> {
      val secretRefIds = mutableSetOf<UUID>()
      config.fields().forEach { (key, value) ->
        if (key == SECRET_REF_ID_FIELD) {
          secretRefIds.add(UUID.fromString(value.asText()))
        } else if (value.isObject) {
          secretRefIds.addAll(getSecretReferenceIdsFromConfig(value))
        }
      }
      return secretRefIds
    }
  }
}
