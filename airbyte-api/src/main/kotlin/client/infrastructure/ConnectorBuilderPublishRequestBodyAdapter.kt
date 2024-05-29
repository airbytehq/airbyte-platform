/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.FromJson
import com.squareup.moshi.ToJson
import io.airbyte.api.client.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.commons.json.Jsons

/**
 * Custom Moshi adapter to handle the [Any] type "manifest" property of the
 * [io.airbyte.api.client.model.generated.DeclarativeSourceManifest] generated client model class that is
 * really a [com.fasterxml.jackson.databind.JsonNode] object and needs
 * a custom adapter to handle the conversion to/from JSON.  This is necessary
 * because Moshi attempts to convert the [Any] type to a [LinkedHashMap],
 * which does not align with the actually provided value for the property.
 */
class ConnectorBuilderPublishRequestBodyAdapter {
  @ToJson
  fun toJson(value: ConnectorBuilderPublishRequestBody): Map<String, Any> {
    return Jsons.`object`(Jsons.jsonNode(value), Map::class.java) as Map<String, Any>
  }

  @FromJson
  fun fromJson(value: Map<String, Any>): ConnectorBuilderPublishRequestBody {
    return Jsons.`object`(Jsons.jsonNode(value), ConnectorBuilderPublishRequestBody::class.java)
  }
}
