package io.airbyte.commons.protocol.transformmodels

import com.fasterxml.jackson.databind.JsonNode

data class AddFieldTransform(
  val schema: JsonNode?,
)
