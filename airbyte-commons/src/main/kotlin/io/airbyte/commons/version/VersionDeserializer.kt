/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import java.io.IOException

/**
 * Deserializer for [Version].
 */
class VersionDeserializer
  @JvmOverloads
  constructor(
    vc: Class<*>? = null,
  ) : com.fasterxml.jackson.databind.deser.std.StdDeserializer<Version>(vc) {
    @Throws(IOException::class, JacksonException::class)
    override fun deserialize(
      p: JsonParser,
      ctxt: DeserializationContext,
    ): Version {
      val node = p.codec.readTree<JsonNode>(p)
      val v = node["version"].asText()
      return Version(v)
    }
  }
