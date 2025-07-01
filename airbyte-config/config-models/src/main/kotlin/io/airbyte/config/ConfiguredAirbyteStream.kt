/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import java.io.Serializable

/**
 * ConfiguredAirbyteStream.
 */
@JsonDeserialize(builder = ConfiguredAirbyteStream.Builder::class)
@JsonInclude(JsonInclude.Include.NON_NULL)
// Left for backward compatibility and test stability.
@JsonPropertyOrder(
  "stream",
  "sync_mode",
  "cursor_field",
  "destination_sync_mode",
  "primary_key",
  "generation_id",
  "minimum_generation_id",
  "sync_id",
)
data class ConfiguredAirbyteStream
  @JvmOverloads
  constructor(
    // TODO Deprecate this
    @JsonProperty("stream")
    var stream: AirbyteStream,
    @JsonProperty("sync_mode")
    var syncMode: SyncMode = SyncMode.FULL_REFRESH,
    @JsonProperty("destination_sync_mode")
    var destinationSyncMode: DestinationSyncMode = DestinationSyncMode.APPEND,
    // The Previous generated code used empty arrays as defaults, preserving the behavior
    @JsonProperty("cursor_field")
    var cursorField: List<String>? = ArrayList(),
    // The Previous generated code used empty arrays as defaults, preserving the behavior
    @JsonProperty("primary_key")
    var primaryKey: List<List<String>>? = ArrayList(),
    @JsonProperty("generation_id")
    var generationId: Long? = null,
    @JsonProperty("minimum_generation_id")
    var minimumGenerationId: Long? = null,
    @JsonProperty("sync_id")
    var syncId: Long? = null,
    // TODO this should become required, for backwards compat, generate from stream?
    var fields: List<Field>? = null,
    var mappers: List<MapperConfig> = listOf(),
    // Because this was introduced with a typo
    @JsonAlias("includesFiles")
    var includeFiles: Boolean = false,
    @JsonProperty("destination_object_name")
    var destinationObjectName: String? = null,
  ) : Serializable {
    fun withStream(stream: AirbyteStream): ConfiguredAirbyteStream {
      this.stream = stream
      return this
    }

    fun withSyncMode(syncMode: SyncMode): ConfiguredAirbyteStream {
      this.syncMode = syncMode
      return this
    }

    fun withCursorField(cursorField: List<String>?): ConfiguredAirbyteStream {
      this.cursorField = cursorField
      return this
    }

    fun withDestinationSyncMode(destinationSyncMode: DestinationSyncMode): ConfiguredAirbyteStream {
      this.destinationSyncMode = destinationSyncMode
      return this
    }

    fun withPrimaryKey(primaryKey: List<List<String>>?): ConfiguredAirbyteStream {
      this.primaryKey = primaryKey
      return this
    }

    fun withGenerationId(generationId: Long?): ConfiguredAirbyteStream {
      this.generationId = generationId
      return this
    }

    fun withMinimumGenerationId(minimumGenerationId: Long?): ConfiguredAirbyteStream {
      this.minimumGenerationId = minimumGenerationId
      return this
    }

    fun withSyncId(syncId: Long?): ConfiguredAirbyteStream {
      this.syncId = syncId
      return this
    }

    fun withDestinationObjectName(destinationObjectName: String?): ConfiguredAirbyteStream =
      apply {
        this.destinationObjectName = destinationObjectName
      }

    @get:JsonIgnore
    val streamDescriptor: StreamDescriptor
      get() = stream.streamDescriptor

    class Builder(
      @JsonProperty("stream")
      var stream: AirbyteStream? = null,
      @JsonProperty("sync_mode")
      var syncMode: SyncMode? = null,
      @JsonProperty("destination_sync_mode")
      var destinationSyncMode: DestinationSyncMode? = null,
      @JsonProperty("cursor_field")
      var cursorField: List<String>? = ArrayList(),
      @JsonProperty("primary_key")
      var primaryKey: List<List<String>>? = ArrayList(),
      @JsonProperty("generation_id")
      var generationId: Long? = null,
      @JsonProperty("minimum_generation_id")
      var minimumGenerationId: Long? = null,
      @JsonProperty("sync_id")
      var syncId: Long? = null,
      var fields: List<Field>? = null,
      var mappers: List<MapperConfig> = listOf(),
      // Because this was introduced with a typo
      @JsonAlias("includesFiles")
      var includeFiles: Boolean? = null,
      @JsonProperty("destination_object_name")
      var destinationObjectName: String? = null,
    ) {
      fun stream(stream: AirbyteStream) = apply { this.stream = stream }

      fun syncMode(syncMode: SyncMode) = apply { this.syncMode = syncMode }

      fun destinationSyncMode(destinationSyncMode: DestinationSyncMode) = apply { this.destinationSyncMode = destinationSyncMode }

      fun cursorField(cursorField: List<String>?) = apply { this.cursorField = cursorField }

      fun primaryKey(primaryKey: List<List<String>>?) = apply { this.primaryKey = primaryKey }

      fun generationId(generationId: Long?) = apply { this.generationId = generationId }

      fun minimumGenerationId(minimumGenerationId: Long?) = apply { this.minimumGenerationId = minimumGenerationId }

      fun syncId(syncId: Long?) = apply { this.syncId = syncId }

      fun fields(fields: List<Field>?) = apply { this.fields = fields }

      fun mappers(mappers: List<MapperConfig>) = apply { this.mappers = mappers }

      fun includeFiles(includeFiles: Boolean) = apply { this.includeFiles = includeFiles }

      fun destinationObjectName(destinationObjectName: String?) = apply { this.destinationObjectName = destinationObjectName }

      fun build(): ConfiguredAirbyteStream =
        ConfiguredAirbyteStream(
          stream = stream ?: throw IllegalArgumentException("stream cannot be null"),
          syncMode = syncMode ?: throw IllegalArgumentException("syncMode cannot be null"),
          destinationSyncMode = destinationSyncMode ?: throw IllegalArgumentException("destinationSyncMode cannot be null"),
          cursorField = cursorField,
          primaryKey = primaryKey,
          generationId = generationId,
          minimumGenerationId = minimumGenerationId,
          syncId = syncId,
          fields = fields,
          mappers = mappers,
          includeFiles = includeFiles ?: false,
          destinationObjectName = destinationObjectName,
        )
    }

    companion object {
      private const val serialVersionUID = 3961017355969088418L
    }
  }
