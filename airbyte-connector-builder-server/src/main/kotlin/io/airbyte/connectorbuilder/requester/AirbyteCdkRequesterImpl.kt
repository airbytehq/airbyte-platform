/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.requester

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.google.common.annotations.VisibleForTesting
import com.google.common.hash.Hashing
import datadog.trace.api.Trace
import io.airbyte.commons.json.Jsons
import io.airbyte.connectorbuilder.TracingHelper
import io.airbyte.connectorbuilder.api.model.generated.AuxiliaryRequest
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.api.model.generated.StreamRead
import io.airbyte.connectorbuilder.api.model.generated.StreamReadLogsInner
import io.airbyte.connectorbuilder.api.model.generated.StreamReadSlicesInner
import io.airbyte.connectorbuilder.commandrunner.SynchronousCdkCommandRunner
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.exceptions.CdkProcessException
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.apache.commons.lang3.StringUtils
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Collections

/**
 * Construct and send requests to the CDK's Connector Builder handler.
 */
@Singleton
class AirbyteCdkRequesterImpl(
  private val commandRunner: SynchronousCdkCommandRunner,
) : AirbyteCdkRequester {
  /**
   * Launch a CDK process responsible for handling resolve_manifest requests.
   */
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  @Throws(
    IOException::class,
    AirbyteCdkInvalidInputException::class,
    CdkProcessException::class,
  )
  override fun readStream(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    state: List<JsonNode>?,
    stream: String?,
    recordLimit: Int?,
    pageLimit: Int?,
    sliceLimit: Int?,
  ): StreamRead {
    if (stream == null) {
      throw AirbyteCdkInvalidInputException("Missing required `stream` field.")
    }
    val record =
      request(manifest, customComponentsCode, config, state, STREAM_READ_COMMAND, stream, recordLimit, pageLimit, sliceLimit)
    return recordToResponse(record)
  }

  private fun recordToResponse(record: AirbyteRecordMessage): StreamRead {
    val response = StreamRead()
    val data = record.data
    val logList = convertToList(data["logs"], object : TypeReference<List<StreamReadLogsInner>>() {})
    val sliceList = convertToList(data["slices"], object : TypeReference<List<StreamReadSlicesInner>>() {})
    response.logs = logList
    response.slices = sliceList
    response.inferredSchema = data["inferred_schema"]
    response.testReadLimitReached = data["test_read_limit_reached"].asBoolean()
    response.latestConfigUpdate = data["latest_config_update"]
    response.inferredDatetimeFormats = data["inferred_datetime_formats"]
    response.auxiliaryRequests =
      data["auxiliary_requests"]?.let {
        convertToList(it, object : TypeReference<List<AuxiliaryRequest>>() {})
      }
    return response
  }

  /**
   * Launch a CDK process responsible for handling resolve_manifest requests.
   */
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  @Throws(
    IOException::class,
    AirbyteCdkInvalidInputException::class,
    CdkProcessException::class,
  )
  override fun resolveManifest(manifest: JsonNode): ResolveManifest {
    val record =
      request(
        manifest,
        null, // As of now, we don't validate custom python when resolving manifests.
        CONFIG_NODE,
        RESOLVE_MANIFEST_COMMAND,
      )
    return ResolveManifest().manifest(record.data["manifest"])
  }

  /**
   * Launch a CDK process responsible for handling full_resolve_manifest requests.
   */
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  @Throws(
    IOException::class,
    AirbyteCdkInvalidInputException::class,
    CdkProcessException::class,
  )
  override fun fullResolveManifest(
    manifest: JsonNode,
    config: JsonNode,
    streamLimit: Int?,
  ): ResolveManifest {
    val record =
      request(
        manifest,
        null, // As of now, we don't validate custom python when resolving manifests.
        config,
        FULL_RESOLVED_MANIFEST_COMMAND,
        streamLimit,
      )
    return ResolveManifest().manifest(record.data["manifest"])
  }

  /**
   * Launch a CDK process responsible for handling requests.
   */
  @Throws(IOException::class, AirbyteCdkInvalidInputException::class, CdkProcessException::class)
  private fun request(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    cdkCommand: String,
  ): AirbyteRecordMessage {
    log.debug { "Creating CDK process: $cdkCommand." }
    return commandRunner.runCommand(
      cdkCommand,
      this.adaptConfig(
        manifest,
        customComponentsCode,
        config,
        cdkCommand,
      ),
      "",
      "",
    )
  }

  @Throws(IOException::class, AirbyteCdkInvalidInputException::class, CdkProcessException::class)
  private fun request(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    cdkCommand: String,
    streamLimit: Int?,
  ): AirbyteRecordMessage {
    log.debug { "Creating CDK process: $cdkCommand." }
    return commandRunner.runCommand(
      cdkCommand,
      this.adaptConfig(
        manifest,
        customComponentsCode,
        config,
        cdkCommand,
        streamLimit,
      ),
      "",
      "",
    )
  }

  @Throws(IOException::class, AirbyteCdkInvalidInputException::class, CdkProcessException::class)
  private fun request(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    state: List<JsonNode>?,
    cdkCommand: String,
    stream: String,
    recordLimit: Int?,
    pageLimit: Int?,
    sliceLimit: Int?,
  ): AirbyteRecordMessage {
    log.debug { "Creating CDK process: $cdkCommand." }
    return commandRunner.runCommand(
      cdkCommand,
      this.adaptConfig(manifest, customComponentsCode, config, cdkCommand, recordLimit, pageLimit, sliceLimit),
      this.adaptCatalog(stream),
      this.adaptState(state),
    )
  }

  private fun adaptCatalog(stream: String): String = String.format(CATALOG_TEMPLATE, stream)

  @VisibleForTesting
  @Throws(IOException::class)
  fun adaptState(state: List<JsonNode>?): String =
    if (state == null || state.isEmpty()) {
      OBJECT_WRITER.writeValueAsString(emptyList<Any>())
    } else {
      OBJECT_WRITER.writeValueAsString(state)
    }

  /**
   * Calculates the MD5 checksum of the provided custom components code.
   *
   * Currently, this only calculates the MD5 but in future it may calculate other checksums in
   * addition or instead.
   *
   * @param customComponentsCode the custom components code to calculate the checksum for
   * @return a JsonNode containing the checksum(s)
   */
  private fun calculateChecksums(customComponentsCode: String?): JsonNode {
    val hashFunction = Hashing.md5()
    val md5Checksum = hashFunction.hashString(customComponentsCode ?: "", StandardCharsets.UTF_8).toString()
    return Jsons.jsonNode(Collections.singletonMap("md5", md5Checksum))
  }

  @Throws(IOException::class)
  private fun adaptConfig(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    command: String,
  ): String {
    val adaptedConfig = Jsons.deserializeIfText(config).deepCopy<JsonNode>()
    (adaptedConfig as ObjectNode).set<JsonNode>(MANIFEST_KEY, Jsons.deserializeIfText(manifest))

    moveNormalizeAndMigrateFlagsToConfig(manifest, adaptedConfig)

    if (!StringUtils.isBlank(customComponentsCode)) {
      adaptedConfig.put(CUSTOM_COMPONENT_KEY, customComponentsCode)
      adaptedConfig.set<JsonNode>(CUSTOM_COMPONENT_CHECKSUM_KEY, calculateChecksums(customComponentsCode))
    }
    adaptedConfig.put(COMMAND_KEY, command)

    return OBJECT_WRITER.writeValueAsString(adaptedConfig)
  }

  @Throws(IOException::class)
  private fun adaptConfig(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    command: String,
    userProvidedStreamLimit: Int?,
  ): String {
    val adaptedConfig = Jsons.deserializeIfText(config).deepCopy<JsonNode>()
    (adaptedConfig as ObjectNode).set<JsonNode>(MANIFEST_KEY, Jsons.deserializeIfText(manifest))

    moveNormalizeAndMigrateFlagsToConfig(manifest, adaptedConfig)

    adaptedConfig.put(COMMAND_KEY, command)
    if (!StringUtils.isBlank(customComponentsCode)) {
      adaptedConfig.put(CUSTOM_COMPONENT_KEY, customComponentsCode)
      adaptedConfig.set<JsonNode>(CUSTOM_COMPONENT_CHECKSUM_KEY, calculateChecksums(customComponentsCode))
    }
    val mapper = ObjectMapper()
    val commandConfig = mapper.createObjectNode()

    if (userProvidedStreamLimit != null) {
      if (userProvidedStreamLimit > MAX_STREAM_LIMIT) {
        throw AirbyteCdkInvalidInputException(
          "Requested stream limit of " + userProvidedStreamLimit + " exceeded maximum of " + MAX_STREAM_LIMIT + ".",
        )
      }
      commandConfig.put(STREAM_LIMIT_KEY, userProvidedStreamLimit)
    }

    adaptedConfig.set<JsonNode>(COMMAND_CONFIG_KEY, commandConfig)

    return OBJECT_WRITER.writeValueAsString(adaptedConfig)
  }

  @Throws(IOException::class)
  private fun adaptConfig(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    command: String,
    userProvidedRecordLimit: Int?,
    userProvidedPageLimit: Int?,
    userProvidedSliceLimit: Int?,
  ): String {
    val adaptedConfig = Jsons.deserializeIfText(config).deepCopy<JsonNode>()
    (adaptedConfig as ObjectNode).set<JsonNode>(MANIFEST_KEY, Jsons.deserializeIfText(manifest))

    moveNormalizeAndMigrateFlagsToConfig(manifest, adaptedConfig)

    adaptedConfig.put(COMMAND_KEY, command)
    if (!StringUtils.isBlank(customComponentsCode)) {
      adaptedConfig.put(CUSTOM_COMPONENT_KEY, customComponentsCode)
      adaptedConfig.set<JsonNode>(CUSTOM_COMPONENT_CHECKSUM_KEY, calculateChecksums(customComponentsCode))
    }
    val mapper = ObjectMapper()
    val commandConfig = mapper.createObjectNode()

    // TODO it would be nicer to collect all applicable error messages and throw a single time with all
    // violations
    // listed than the current strategy of throwing at the first issue you happen to check
    if (userProvidedRecordLimit != null) {
      if (userProvidedRecordLimit > MAX_RECORD_LIMIT) {
        throw AirbyteCdkInvalidInputException(
          "Requested record limit of " + userProvidedRecordLimit + " exceeds maximum of " + MAX_RECORD_LIMIT + ".",
        )
      }
      commandConfig.put(RECORD_LIMIT_KEY, userProvidedRecordLimit)
    }

    if (userProvidedPageLimit != null) {
      if (userProvidedPageLimit > MAX_PAGE_LIMIT) {
        throw AirbyteCdkInvalidInputException(
          "Requested page limit of " + userProvidedPageLimit + " exceeds maximum of " + MAX_PAGE_LIMIT + ".",
        )
      }
      commandConfig.put(PAGE_LIMIT_KEY, userProvidedPageLimit)
    }

    if (userProvidedSliceLimit != null) {
      if (userProvidedSliceLimit > MAX_SLICE_LIMIT) {
        throw AirbyteCdkInvalidInputException(
          "Requested slice limit of " + userProvidedSliceLimit + " exceeds maximum of " + MAX_SLICE_LIMIT + ".",
        )
      }
      commandConfig.put(SLICE_LIMIT_KEY, userProvidedSliceLimit)
    }

    adaptedConfig.set<JsonNode>(COMMAND_CONFIG_KEY, commandConfig)

    return OBJECT_WRITER.writeValueAsString(adaptedConfig)
  }

  private fun moveNormalizeAndMigrateFlagsToConfig(
    manifest: JsonNode,
    config: JsonNode,
  ) {
    // the hack to include additional flags to control the Form-based UI rendering

    // add `__should_normalize: bool` set by UI and remove it from the manifest for the time of the call
    if (manifest.has(SHOULD_NORMALIZE_KEY)) {
      (config as ObjectNode).set<JsonNode>(SHOULD_NORMALIZE_KEY, manifest.get(SHOULD_NORMALIZE_KEY))
      // remove the flag from the manifest JsonNode once it's there
      ((config as ObjectNode).get(MANIFEST_KEY) as ObjectNode).remove(SHOULD_NORMALIZE_KEY)
    }

    // add `__should_migrate: bool` set by UI and remove it from the manifest for the time of the call
    if (manifest.has(SHOULD_MIGRATE_KEY)) {
      (config as ObjectNode).set<JsonNode>(SHOULD_MIGRATE_KEY, manifest.get(SHOULD_MIGRATE_KEY))
      // now remove the flag from the manifest JsonNode once it's there
      ((config as ObjectNode).get(MANIFEST_KEY) as ObjectNode).remove(SHOULD_MIGRATE_KEY)
    }

    // end of hack
  }

  private fun <T> convertToList(
    `object`: JsonNode,
    typeReference: TypeReference<List<T>>,
  ): List<T> {
    val objectMapper = ObjectMapper()
    objectMapper.registerModule(JavaTimeModule())
    return objectMapper.convertValue(`object`, typeReference)
  }

  companion object {
    const val MAX_RECORD_LIMIT: Int = 5000
    const val MAX_SLICE_LIMIT: Int = 20
    const val MAX_PAGE_LIMIT: Int = 20
    const val MAX_STREAM_LIMIT: Int = 100
    private const val COMMAND_KEY = "__command"
    private const val COMMAND_CONFIG_KEY = "__test_read_config"
    private const val MANIFEST_KEY = "__injected_declarative_manifest"
    private const val CUSTOM_COMPONENT_KEY = "__injected_components_py"
    private const val CUSTOM_COMPONENT_CHECKSUM_KEY = "__injected_components_py_checksums"
    private const val RECORD_LIMIT_KEY = "max_records"
    private const val SLICE_LIMIT_KEY = "max_slices"
    private const val STREAM_LIMIT_KEY = "max_streams"
    private const val PAGE_LIMIT_KEY = "max_pages_per_slice"
    private const val RESOLVE_MANIFEST_COMMAND = "resolve_manifest"
    private const val FULL_RESOLVED_MANIFEST_COMMAND = "full_resolve_manifest"
    private const val STREAM_READ_COMMAND = "test_read"
    private const val SHOULD_NORMALIZE_KEY = "__should_normalize"
    private const val SHOULD_MIGRATE_KEY = "__should_migrate"
    private val CATALOG_TEMPLATE =
      """
      {
        "streams": [
          {
            "stream": {
              "name": "%s",
              "json_schema": {},
              "supported_sync_modes": ["full_refresh", "incremental"]
            },
            "sync_mode": "incremental",
            "destination_sync_mode": "overwrite"
          }
        ]
      }
      """.trimIndent()
    private val OBJECT_WRITER: ObjectWriter = ObjectMapper().writer().withDefaultPrettyPrinter()
    private val CONFIG_NODE: ObjectNode = ObjectMapper().createObjectNode()
    private val log = KotlinLogging.logger {}
  }
}
