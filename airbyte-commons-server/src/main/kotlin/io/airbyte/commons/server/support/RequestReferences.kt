/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.micronaut.http.BasicHttpAttributes
import io.micronaut.http.HttpRequest
import kotlin.jvm.optionals.getOrNull

/** IDs from the bound request body and route, never from client-supplied reference headers. */
data class RequestReferences(
  val values: Map<AuthenticationId, String>,
) {
  fun asScopeProperties(): Map<String, String> = values.mapKeys { it.key.httpHeader }

  fun traceValues(): Map<String, String> = values.filterKeys { !it.fieldName.contains('_') }.mapKeys { it.key.fieldName }

  companion object {
    fun from(request: HttpRequest<*>): RequestReferences {
      val bodyValues = bodyReferences(request.body.getOrNull())
      val routeValues =
        BasicHttpAttributes
          .getRouteMatchInfo(request)
          .getOrNull()
          ?.variableValues
          ?.mapNotNull { (name, value) -> AuthenticationId.entries.firstOrNull { it.fieldName == name }?.let { it to value.toString() } }
          ?.toMap()
          .orEmpty()
      return RequestReferences(bodyValues + routeValues)
    }

    private fun bodyReferences(body: Any?): Map<AuthenticationId, String> {
      val node =
        try {
          when (body) {
            null -> return emptyMap()
            is String -> Jsons.deserialize(body)
            is ByteArray -> Jsons.deserialize(body.toString(Charsets.UTF_8))
            else -> Jsons.jsonNode(body)
          }
        } catch (e: RuntimeException) {
          return emptyMap()
        }
      return AuthenticationId.entries.mapNotNull { id -> referenceValue(node, id)?.let { id to it } }.toMap()
    }

    private fun referenceValue(
      body: JsonNode,
      id: AuthenticationId,
    ): String? {
      val value = body[id.fieldName]?.takeUnless { it.isNull } ?: return null
      val text =
        if (id == AuthenticationId.WORKSPACE_IDS || id == AuthenticationId.CONNECTION_IDS) {
          Jsons.serialize(value)
        } else {
          value.asText()
        }
      return text.takeIf(String::isNotBlank)
    }
  }
}
