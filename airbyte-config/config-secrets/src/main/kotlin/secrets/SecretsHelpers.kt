/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.JsonPaths
import io.airbyte.commons.json.JsonPaths.getExpandedPaths
import io.airbyte.commons.json.JsonSchemas
import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.ExternalSecretCoordinate
import io.airbyte.config.secrets.persistence.ReadOnlySecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretStorageId
import io.github.oshai.kotlinlogging.KotlinLogging
import secrets.persistence.SecretCoordinateException
import java.util.UUID
import java.util.function.Supplier
import kotlin.collections.set

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

  // The prefix used to recognize secret references in a config. End users can use this prefix to
  // specify a coordinate to a secret in their configured secret storage.
  @InternalForTesting
  internal const val SECRET_REF_PREFIX = "secret_coordinate::"

  /**
   * Used to separate secrets out of some configuration. This will output a partial config that
   * includes pointers to secrets instead of actual secret values and a map that can be used to update
   * a [SecretPersistence] at coordinates with values from the full config.
   *
   * @param secretBaseId id used for this config
   * @param fullConfig config including secrets
   * @param secretPersistence secret persistence to be used for r/w. Could be a runtime secret persistence.
   * @return a partial config + a map of coordinates to secret payloads
   */
  fun splitConfig(
    secretBaseId: UUID,
    fullConfig: ConfigWithProcessedSecrets,
    secretPersistence: SecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      uuidSupplier = { UUID.randomUUID() },
      secretBaseId = secretBaseId,
      secretReader = secretPersistence,
      persistedPartialConfig = null,
      newFullConfig = fullConfig,
      secretBasePrefix = secretBasePrefix,
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
    fullConfig: ConfigWithProcessedSecrets,
    secretPersistence: SecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      uuidSupplier = uuidSupplier,
      secretBaseId = secretBaseId,
      secretReader = secretPersistence,
      persistedPartialConfig = null,
      newFullConfig = fullConfig,
      secretBasePrefix = secretBasePrefix,
    )

  @Deprecated("Use splitConfig() that takes in InputConfigWithProcessedSecrets instead.")
  fun splitConfig(
    uuidSupplier: Supplier<UUID>,
    secretBaseId: UUID,
    fullConfig: JsonNode,
    spec: JsonNode,
    secretPersistence: SecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig {
    val fullConfigWithProcessedSecrets = SecretReferenceHelpers.processConfigSecrets(fullConfig, spec, null)
    return splitConfig(
      uuidSupplier = uuidSupplier,
      secretBaseId = secretBaseId,
      fullConfig = fullConfigWithProcessedSecrets,
      secretPersistence = secretPersistence,
      secretBasePrefix = secretBasePrefix,
    )
  }

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
    oldPartialConfig: ConfigWithSecretReferences?,
    newFullConfig: ConfigWithProcessedSecrets,
    secretReader: ReadOnlySecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig =
    internalSplitAndUpdateConfig(
      uuidSupplier = { UUID.randomUUID() },
      secretBaseId = secretBaseId,
      secretReader = secretReader,
      persistedPartialConfig = oldPartialConfig,
      newFullConfig = newFullConfig,
      secretBasePrefix = secretBasePrefix,
    )

  /**
   * Identical to [SecretsHelpers.splitAndUpdateConfig] with UUID supplier for testing.
   */
  @Deprecated("Use splitAndUpdateConfig() that takes in ConfigWithSecretReferences instead.")
  @VisibleForTesting
  fun splitAndUpdateConfig(
    uuidSupplier: Supplier<UUID>,
    secretBaseId: UUID,
    oldPartialConfig: JsonNode?,
    newFullConfig: JsonNode,
    spec: JsonNode,
    secretReader: ReadOnlySecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): SplitSecretConfig {
    val oldPartialConfigWithSecretReferences = oldPartialConfig?.let { buildConfigWithSecretRefsJava(it) }
    val newFullConfigWithProcessedSecrets = SecretReferenceHelpers.processConfigSecrets(newFullConfig, spec, null)
    return internalSplitAndUpdateConfig(
      uuidSupplier = uuidSupplier,
      secretBaseId = secretBaseId,
      secretReader = secretReader,
      persistedPartialConfig = oldPartialConfigWithSecretReferences,
      newFullConfig = newFullConfigWithProcessedSecrets,
      secretBasePrefix = secretBasePrefix,
    )
  }

  /**
   * Replaces referenced secrets in the config with the string secret payloads loaded from the secret persistence at those coordinates.
   * All referenced coordinates are expected to live in the passed-in secretPersistence.
   *
   * @param configWithRefs configuration containing secret coordinates (references to secrets)
   * @param secretPersistence secret storage mechanism
   * @return full config including actual secret values
   */
  fun combineConfig(
    configWithRefs: ConfigWithSecretReferences?,
    secretPersistence: ReadOnlySecretPersistence,
  ): JsonNode =
    combineConfigInternal(configWithRefs) {
      secretPersistence
    }

  /**
   * Replaces referenced secrets in the config with their values from the corresponding secret persistence.
   *
   * @param configWithRefs configuration containing secret coordinates (references to secrets)
   * @param secretPersistenceMap map of secret storage IDs to their corresponding secret persistence
   * @return full config including actual secret values
   */
  fun combineConfig(
    configWithRefs: ConfigWithSecretReferences?,
    secretPersistenceMap: Map<UUID?, ReadOnlySecretPersistence>,
  ): JsonNode =
    combineConfigInternal(
      configWithRefs,
      resolvePersistence = { secretStorageId ->
        secretPersistenceMap[secretStorageId]
          ?: throw IllegalStateException("No persistence found for secret storage ID: $secretStorageId")
      },
    )

  private fun combineConfigInternal(
    configWithRefs: ConfigWithSecretReferences?,
    resolvePersistence: (UUID?) -> ReadOnlySecretPersistence,
  ): JsonNode {
    if (configWithRefs == null) {
      return JsonNodeFactory.instance.objectNode()
    }

    var config = configWithRefs.config
    for ((hydrationPath, secretRefConfig) in configWithRefs.referencedSecrets) {
      val secretPersistence = resolvePersistence(secretRefConfig.secretStorageId)
      val secretValue = getOrThrowSecretValue(secretPersistence, secretRefConfig.secretCoordinate)
      config =
        when (hydrationPath) {
          "$" -> TextNode(secretValue)
          else -> JsonPaths.replaceAt(config, hydrationPath) { _, _ -> TextNode(secretValue) }
        }
    }

    return config
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
    persistedPartialConfig: ConfigWithSecretReferences?,
    newFullConfig: ConfigWithProcessedSecrets,
    secretBasePrefix: String,
  ): SplitSecretConfig {
    val airbyteCoordinateAndValueMap =
      getAirbyteCoordinatesToWriteAndReplace(
        uuidSupplier = uuidSupplier,
        secretBaseId = secretBaseId,
        secretReader = secretReader,
        persistedPartialConfig = persistedPartialConfig,
        newFullConfig = newFullConfig,
        secretBasePrefix = secretBasePrefix,
      )
    val updatedConfigWithReplacedCoordinateNodes =
      replaceSecretValuesWithCoordinateNodes(
        newFullConfig = newFullConfig,
        airbyteCoordinateAndValueMap = airbyteCoordinateAndValueMap,
      )
    val airbyteCoordinateToRawValueMap =
      airbyteCoordinateAndValueMap.values
        .associate { it.airbyteCoordinate to it.rawValue }
    return SplitSecretConfig(
      updatedConfigWithReplacedCoordinateNodes,
      airbyteCoordinateToRawValueMap,
    )
  }

  /**
   * Helper class to group an AirbyteManagedSecretCoordinate and its raw value
   */
  private data class AirbyteManagedCoordinateAndRawValue(
    val airbyteCoordinate: AirbyteManagedSecretCoordinate,
    val rawValue: String,
  )

  /**
   * Given a config that may contain raw secret values, returns a map of each path to the new
   * [AirbyteManagedSecretCoordinate] and the raw value to be written to the secret persistence.
   * - If a path containing a raw value is associated with an existing airbyte-managed secret
   * coordinate, the new coordinate will be created with the same base and an incremented version.
   */
  private fun getAirbyteCoordinatesToWriteAndReplace(
    uuidSupplier: Supplier<UUID>,
    secretBaseId: UUID,
    secretReader: ReadOnlySecretPersistence,
    persistedPartialConfig: ConfigWithSecretReferences?,
    newFullConfig: ConfigWithProcessedSecrets,
    secretBasePrefix: String,
  ): Map<String, AirbyteManagedCoordinateAndRawValue> =
    buildMap {
      newFullConfig.processedSecrets.forEach { (path, processedSecretNode) ->
        processedSecretNode.rawValue?.let { rawValue ->
          // If there is an existing secret reference, attempt to extract its AirbyteManagedSecretCoordinate.
          val persistedNode = persistedPartialConfig?.referencedSecrets?.get(path)
          val existingCoordinate = persistedNode?.secretCoordinate as? AirbyteManagedSecretCoordinate

          // Create a new coordinate.
          val coordinate =
            createNewAirbyteManagedSecretCoordinate(
              secretBasePrefix = secretBasePrefix,
              secretReader = secretReader,
              secretBaseId = secretBaseId,
              uuidSupplier = uuidSupplier,
              oldCoordinate = existingCoordinate,
            )
          put(path, AirbyteManagedCoordinateAndRawValue(coordinate, rawValue))
        }
      }
    }

  /**
   * Given a config that may contain raw secret values and/or prefixed secret references, returns
   * a new config with those secret values replaced with secret coordinate nodes.
   * - If the config contains a raw secret value, it will be replaced with an airbyte-managed
   * coordinate node
   * - If the config contains a prefixed secret reference, it will be replaced with an external
   * secret coordinate node, along with the secret storage ID pertaining to that reference.
   */
  private fun replaceSecretValuesWithCoordinateNodes(
    newFullConfig: ConfigWithProcessedSecrets,
    airbyteCoordinateAndValueMap: Map<String, AirbyteManagedCoordinateAndRawValue>,
  ): JsonNode {
    var fullConfigCopy = newFullConfig.originalConfig.deepCopy<JsonNode>()
    newFullConfig.processedSecrets.forEach { (path, processedSecretNode) ->
      if (processedSecretNode.rawValue != null) {
        val airbyteManagedCoordAndValue =
          airbyteCoordinateAndValueMap[path] ?: throw IllegalStateException(
            "Expected to find a coordinate for path $path, but none was found.",
          )
        fullConfigCopy =
          JsonPaths.replaceAt(fullConfigCopy, path) { json: JsonNode, pathOfNode: String? ->
            Jsons.jsonNode(
              buildMap {
                put(COORDINATE_FIELD, airbyteManagedCoordAndValue.airbyteCoordinate.fullCoordinate)
                processedSecretNode.secretStorageId?.let { put(SECRET_STORAGE_ID_FIELD, it.value.toString()) }
              },
            )
          }
      } else if (processedSecretNode.secretCoordinate is ExternalSecretCoordinate) {
        fullConfigCopy =
          JsonPaths.replaceAt(fullConfigCopy, path) { json: JsonNode, pathOfNode: String? ->
            Jsons.jsonNode(
              buildMap {
                put(COORDINATE_FIELD, processedSecretNode.secretCoordinate.fullCoordinate)
                processedSecretNode.secretStorageId?.let { put(SECRET_STORAGE_ID_FIELD, it.value.toString()) }
              },
            )
          }
      }
    }
    return fullConfigCopy
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
  fun createNewAirbyteManagedSecretCoordinate(
    secretBasePrefix: String,
    secretReader: ReadOnlySecretPersistence,
    secretBaseId: UUID,
    uuidSupplier: Supplier<UUID>,
    oldCoordinate: AirbyteManagedSecretCoordinate?,
  ): AirbyteManagedSecretCoordinate {
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
    fun getSecretStorageIdsFromConfig(config: ConfigWithSecretReferences): Set<UUID?> =
      config.referencedSecrets.values
        .map { it.secretStorageId }
        .toSet()

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
              secretRefConfig.secretReferenceId?.let { put(SECRET_REF_ID_FIELD, it) }
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
            secretReferenceId = config[SECRET_REF_ID_FIELD]?.takeIf { it.isTextual }?.let { UUID.fromString(it.asText()) },
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

    fun getSecretReferenceIdAtPath(
      config: JsonNode,
      nodePath: String,
    ): SecretReferenceId? =
      JsonPaths
        .getSingleValue(config, nodePath)
        .map { it.get(SECRET_REF_ID_FIELD) }
        .filter { it.isTextual }
        .map { SecretReferenceId(UUID.fromString(it.asText())) }
        .orElse(null)

    private enum class SecretNodeType {
      AIRBYTE_MANAGED_SECRET_COORDINATE,
      EXTERNAL_SECRET_COORDINATE,
      PREFIXED_SECRET_REFERENCE,
      RAW_SECRET_VALUE,
      SECRET_REFERENCE_ID,
    }

    private fun determineSecretNodeType(secretNode: JsonNode): SecretNodeType =
      when {
        secretNode.has(SECRET_REF_ID_FIELD) -> SecretNodeType.SECRET_REFERENCE_ID
        secretNode.has(COORDINATE_FIELD) -> {
          val secretCoord = SecretCoordinate.fromFullCoordinate(secretNode.get(COORDINATE_FIELD).asText())
          when (secretCoord) {
            is AirbyteManagedSecretCoordinate -> SecretNodeType.AIRBYTE_MANAGED_SECRET_COORDINATE
            else -> SecretNodeType.EXTERNAL_SECRET_COORDINATE
          }
        }
        secretNode.asText().startsWith(SECRET_REF_PREFIX) -> SecretNodeType.PREFIXED_SECRET_REFERENCE
        else -> SecretNodeType.RAW_SECRET_VALUE
      }

    private fun processSecretNode(
      secretNode: JsonNode,
      secretStorageId: SecretStorageId?,
    ): ProcessedSecretNode {
      if (secretNode.isArray || secretNode.isMissingNode || secretNode.isNull) {
        throw IllegalStateException("Cannot process node that is an array, missing, or null")
      }
      return when (determineSecretNodeType(secretNode)) {
        SecretNodeType.PREFIXED_SECRET_REFERENCE ->
          ProcessedSecretNode(
            secretCoordinate = ExternalSecretCoordinate(secretNode.asText().removePrefix(SECRET_REF_PREFIX)),
            secretStorageId = secretStorageId,
          )
        SecretNodeType.AIRBYTE_MANAGED_SECRET_COORDINATE ->
          ProcessedSecretNode(
            secretCoordinate = AirbyteManagedSecretCoordinate.fromFullCoordinate(secretNode.get(COORDINATE_FIELD).asText()),
            secretStorageId = secretStorageId,
          )
        SecretNodeType.EXTERNAL_SECRET_COORDINATE ->
          ProcessedSecretNode(
            secretCoordinate = ExternalSecretCoordinate(secretNode.get(COORDINATE_FIELD).asText()),
            secretStorageId = secretStorageId,
          )
        SecretNodeType.RAW_SECRET_VALUE ->
          ProcessedSecretNode(
            rawValue = secretNode.asText(),
            secretStorageId = secretStorageId,
          )
        SecretNodeType.SECRET_REFERENCE_ID ->
          ProcessedSecretNode(
            secretReferenceId = secretNode.get(SECRET_REF_ID_FIELD)?.let { SecretReferenceId(UUID.fromString(it.asText())) },
          )
      }
    }

    private fun getProcessedSecretMapFromInputConfig(
      config: JsonNode,
      spec: JsonNode,
      secretStorageId: SecretStorageId?,
    ): Map<String, ProcessedSecretNode> {
      val processedSecretMap = mutableMapOf<String, ProcessedSecretNode>()

      getSortedSecretPaths(spec).forEach { pathTemplate ->
        getExpandedPaths(config, pathTemplate).forEach { path ->
          val secretNode = JsonPaths.getSingleValue(config, path)
          if (secretNode.isPresent) {
            val processedSecretNode = processSecretNode(secretNode.get(), secretStorageId)
            processedSecretMap[path] = processedSecretNode
          }
        }
      }
      return processedSecretMap
    }

    /**
     * Process a config that may contain raw secret values or prefixed secret references.
     * If a secretStorageId is provided, it will be associated with all processed secrets nodes,
     * to indicate the storage where raw secrets should be written, or where prefixed secret
     * references can be resolved.
     *
     * @return A [ConfigWithProcessedSecrets] object containing the original, unmodified
     * actorConfig and a map of secret paths to [ProcessedSecretNode] objects.
     */
    @JvmName("processConfigSecrets")
    fun processConfigSecrets(
      actorConfig: JsonNode,
      spec: JsonNode,
      secretStorageId: SecretStorageId?,
    ): ConfigWithProcessedSecrets {
      val processedSecrets =
        getProcessedSecretMapFromInputConfig(
          config = actorConfig,
          spec = spec,
          secretStorageId = secretStorageId,
        )
      return ConfigWithProcessedSecrets(actorConfig, processedSecrets)
    }

    /**
     * A wrapper around a [JsonNode] config that contains secret reference IDs alongside the
     * secret coordinates.
     */
    @JvmInline
    value class ConfigWithSecretReferenceIdsInjected(
      val value: JsonNode,
    )

    /**
     * Given a [config] and [secretReferenceIdsByPath], for each path in the map, inject the
     * secret reference id into the node at that path.
     */
    fun updateSecretNodesWithSecretReferenceIds(
      config: JsonNode,
      secretReferenceIdsByPath: Map<String, SecretReferenceId>,
    ): ConfigWithSecretReferenceIdsInjected {
      var updatedConfig = Jsons.clone(config)
      secretReferenceIdsByPath.forEach { (path, secretRefId) ->
        // While we're dual-writing, make sure to keep the _secret coord in the config
        val nodeToUpdate =
          JsonPaths.getSingleValue(updatedConfig, path).orElseThrow {
            IllegalStateException("Secret reference not found at path: $path")
          }
        val existingCoordinateField =
          (nodeToUpdate as? ObjectNode)
            ?.get(COORDINATE_FIELD)
            ?.takeIf { it.isTextual }
            ?.asText()
        val secretRefIdNode =
          Jsons.jsonNode(
            buildMap {
              put(SECRET_REF_ID_FIELD, secretRefId.value.toString())
              existingCoordinateField?.let { put(COORDINATE_FIELD, it) }
            },
          )
        updatedConfig = JsonPaths.replaceAtJsonNodeLoud(updatedConfig, path, secretRefIdNode)
      }
      return ConfigWithSecretReferenceIdsInjected(updatedConfig)
    }

    /**
     * Given a config and spec, replace all secret nodes with a placeholder text value.
     * This is useful for preparing a config with secret nodes for validation against a spec
     * that expects string values instead of object nodes at each secret path.
     */
    fun configWithTextualSecretPlaceholders(
      config: JsonNode,
      spec: JsonNode,
    ): JsonNode {
      var configCopy = Jsons.clone(config)
      val secretPaths = getSortedSecretPaths(spec)
      secretPaths.forEach { path ->
        getExpandedPaths(configCopy, path).forEach { expandedPath ->
          val nodeAtPath = JsonPaths.getSingleValue(configCopy, expandedPath)
          if (nodeAtPath.isPresent && nodeAtPath.get().isObject) {
            configCopy = Jsons.clone(JsonPaths.replaceAtString(configCopy, expandedPath, "secret_placeholder"))
          }
        }
      }
      return configCopy
    }
  }

  /**
   * Merge nodes but don't merge keys with secret JSON values.
   */
  fun mergeNodesExceptForSecrets(
    mainNode: JsonNode,
    updateNode: JsonNode,
  ): JsonNode {
    val fieldNames = updateNode.fieldNames()
    while (fieldNames.hasNext()) {
      val fieldName = fieldNames.next()
      val jsonNode = mainNode[fieldName]
      // if field exists and is an embedded object
      if (isStandardObjectNode(jsonNode)) {
        mergeNodesExceptForSecrets(jsonNode, updateNode[fieldName])
      } else {
        // if the value of the JsonNode in `fieldName` is either a secret or a plain value
        // we replace it with the passed in value
        if (mainNode is ObjectNode) {
          // Overwrite field
          val value = updateNode[fieldName]
          mainNode.replace(fieldName, value)
        }
      }
    }
    return mainNode
  }

  /**
   * Checks if the given JsonNode is a regular object. If it's a secret node, returns false.
   */
  private fun isStandardObjectNode(node: JsonNode?): Boolean =
    node != null && node.isObject && node[COORDINATE_FIELD] == null && node[SECRET_REF_ID_FIELD] == null
}
