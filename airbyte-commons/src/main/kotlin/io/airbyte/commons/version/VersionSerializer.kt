/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import java.io.IOException

/**
 * Serialize a [Version].
 */
class VersionSerializer
  @JvmOverloads
  constructor(
    t: Class<Version>? = null,
  ) : com.fasterxml.jackson.databind.ser.std.StdSerializer<Version>(t) {
    @Throws(IOException::class)
    override fun serialize(
      value: Version,
      gen: JsonGenerator,
      provider: SerializerProvider,
    ) {
      gen.writeStartObject()
      gen.writeStringField("version", value.serialize())
      gen.writeEndObject()
    }
  }
