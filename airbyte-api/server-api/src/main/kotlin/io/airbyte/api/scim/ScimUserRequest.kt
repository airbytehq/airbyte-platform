/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.scim

import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode

@JsonDeserialize(using = ScimUserRequestDeserializer::class)
data class ScimUserRequest
  constructor(
    @get:JsonValue
    val body: ObjectNode,
  )

class ScimUserRequestDeserializer : JsonDeserializer<ScimUserRequest>() {
  override fun deserialize(
    parser: JsonParser,
    context: DeserializationContext,
  ): ScimUserRequest {
    val body = parser.readValueAsTree<JsonNode>()
    if (body !is ObjectNode) {
      return context.reportInputMismatch(ScimUserRequest::class.java, "SCIM User request must be an object")
    }
    return ScimUserRequest(body)
  }

  override fun getNullValue(context: DeserializationContext): ScimUserRequest =
    context.reportInputMismatch(ScimUserRequest::class.java, "SCIM User request must be an object")
}
