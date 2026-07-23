/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

@JsonDeserialize(using = AirbyteSecret.Deserializer::class)
@JsonSerialize(using = AirbyteSecret.Serializer::class)
sealed class AirbyteSecret {
  companion object {
    const val SECRET_REFERENCE_FIELD_NAME = "_secret"
  }

  data class Hydrated(
    val value: String,
  ) : AirbyteSecret()

  data class Reference(
    val reference: String,
  ) : AirbyteSecret()

  class Deserializer : StdDeserializer<AirbyteSecret>(AirbyteSecret::class.java) {
    override fun deserialize(
      parser: JsonParser,
      ctxt: DeserializationContext,
    ): AirbyteSecret {
      val node = parser.codec.readTree<JsonNode>(parser)
      return if (node.isTextual) {
        Hydrated(node.asText())
      } else {
        Reference(node.get(SECRET_REFERENCE_FIELD_NAME).asText())
      }
    }
  }

  class Serializer : StdSerializer<AirbyteSecret>(AirbyteSecret::class.java) {
    override fun serialize(
      secret: AirbyteSecret,
      gen: JsonGenerator,
      provider: SerializerProvider,
    ) {
      when (secret) {
        is Hydrated -> gen.writeString(secret.value)
        is Reference -> {
          gen.writeStartObject()
          gen.writeStringField(SECRET_REFERENCE_FIELD_NAME, secret.reference)
          gen.writeEndObject()
        }
      }
    }
  }
}
