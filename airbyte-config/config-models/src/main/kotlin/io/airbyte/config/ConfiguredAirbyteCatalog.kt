/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import java.io.Serializable

/**
 * ConfiguredAirbyteCatalog.
 *
 *
 * Airbyte stream schema catalog
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
  "streams",
)
data class ConfiguredAirbyteCatalog
  @JvmOverloads
  constructor(
    @JsonProperty("streams")
    var streams: List<ConfiguredAirbyteStream> = ArrayList(),
  ) : Serializable {
    fun withStreams(streams: List<ConfiguredAirbyteStream>): ConfiguredAirbyteCatalog {
      this.streams = streams
      return this
    }

    companion object {
      private const val serialVersionUID = 3093736788188579672L
    }
  }
