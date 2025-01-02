/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import java.io.Serializable

/**
 * AirbyteStream.
 */
@JsonDeserialize(builder = AirbyteStream.Builder::class)
@JsonInclude(JsonInclude.Include.NON_NULL)
// Left for backward compatibility and test stability.
@JsonPropertyOrder(
  "name",
  "json_schema",
  "supported_sync_modes",
  "source_defined_cursor",
  "default_cursor_field",
  "source_defined_primary_key",
  "namespace",
  "is_resumable",
)
data class AirbyteStream
  @JvmOverloads
  constructor(
    @JsonProperty("name")
    var name: String,
    @JsonProperty("json_schema")
    var jsonSchema: JsonNode,
    @JsonProperty("supported_sync_modes")
    var supportedSyncModes: List<SyncMode>,
    @JsonProperty("source_defined_cursor")
    var sourceDefinedCursor: Boolean? = null,
    @JsonProperty("default_cursor_field")
    var defaultCursorField: List<String>? = ArrayList(),
    @JsonProperty("source_defined_primary_key")
    var sourceDefinedPrimaryKey: List<List<String>>? = ArrayList(),
    @JsonProperty("namespace")
    var namespace: String? = null,
    @JsonProperty("is_resumable")
    var isResumable: Boolean? = null,
  ) : Serializable {
    fun withName(name: String): AirbyteStream {
      this.name = name
      return this
    }

    fun withJsonSchema(jsonSchema: JsonNode): AirbyteStream {
      this.jsonSchema = jsonSchema
      return this
    }

    fun withSupportedSyncModes(supportedSyncModes: List<SyncMode>): AirbyteStream {
      this.supportedSyncModes = supportedSyncModes
      return this
    }

    fun withSourceDefinedCursor(sourceDefinedCursor: Boolean?): AirbyteStream {
      this.sourceDefinedCursor = sourceDefinedCursor
      return this
    }

    fun withDefaultCursorField(defaultCursorField: List<String>?): AirbyteStream {
      this.defaultCursorField = defaultCursorField
      return this
    }

    fun withSourceDefinedPrimaryKey(sourceDefinedPrimaryKey: List<List<String>>?): AirbyteStream {
      this.sourceDefinedPrimaryKey = sourceDefinedPrimaryKey
      return this
    }

    fun withNamespace(namespace: String?): AirbyteStream {
      this.namespace = namespace
      return this
    }

    fun withIsResumable(isResumable: Boolean?): AirbyteStream {
      this.isResumable = isResumable
      return this
    }

    @get:JsonIgnore
    val streamDescriptor: StreamDescriptor
      get() = StreamDescriptor().withName(name).withNamespace(namespace)

    class Builder(
      @JsonProperty("name")
      var name: String? = null,
      @JsonProperty("json_schema")
      var jsonSchema: JsonNode? = null,
      @JsonProperty("supported_sync_modes")
      var supportedSyncModes: List<SyncMode>? = null,
      @JsonProperty("source_defined_cursor")
      var sourceDefinedCursor: Boolean? = null,
      @JsonProperty("default_cursor_field")
      var defaultCursorField: List<String>? = ArrayList(),
      @JsonProperty("source_defined_primary_key")
      var sourceDefinedPrimaryKey: List<List<String>>? = ArrayList(),
      @JsonProperty("namespace")
      var namespace: String? = null,
      @JsonProperty("is_resumable")
      var isResumable: Boolean? = null,
    ) {
      fun build(): AirbyteStream =
        AirbyteStream(
          name = name ?: throw IllegalArgumentException("name cannot be null"),
          jsonSchema = jsonSchema ?: throw IllegalArgumentException("jsonSchema cannot be null"),
          supportedSyncModes = supportedSyncModes ?: throw IllegalArgumentException("supportedSyncModes cannot be null"),
          sourceDefinedCursor = sourceDefinedCursor,
          defaultCursorField = defaultCursorField,
          sourceDefinedPrimaryKey = sourceDefinedPrimaryKey,
          namespace = namespace,
          isResumable = isResumable,
        )
    }

    companion object {
      private const val serialVersionUID = 602929458758090299L
    }
  }
