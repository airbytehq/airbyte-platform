/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.domain.models.scim.ScimGroupWrite
import java.util.UUID

object ScimGroupRequestParser {
  private val topLevelFields =
    mapOf(
      "schemas" to "schemas",
      "id" to "id",
      "externalid" to "externalId",
      "displayname" to "displayName",
      "members" to "members",
      "meta" to "meta",
    )
  private val memberFields =
    mapOf(
      "value" to "value",
      "${'$'}ref" to "${'$'}ref",
      "display" to "display",
    )

  fun parse(body: ObjectNode): ScimGroupWrite {
    val fields = canonicalFields(body, topLevelFields, "Group")
    validateSchemas(fields["schemas"])

    return ScimGroupWrite(
      displayName = displayName(fields["displayName"]),
      externalId = optionalExternalId(fields["externalId"]),
      memberIds = members(fields["members"]),
    )
  }

  private fun displayName(node: JsonNode?): String =
    requiredNonBlankString(node, "displayName").also {
      if (it.codePointCount(0, it.length) > MAX_DISPLAY_NAME_LENGTH) {
        throw ScimErrors.invalidValue("displayName cannot exceed $MAX_DISPLAY_NAME_LENGTH characters")
      }
    }

  private fun members(node: JsonNode?): List<UUID> {
    if (node == null) return emptyList()
    if (!node.isArray) throw ScimErrors.invalidValue("members must be an array")
    val seen = linkedSetOf<UUID>()
    node.forEach { memberNode ->
      val member = memberNode as? ObjectNode ?: throw ScimErrors.invalidValue("Each member must be an object")
      val fields = canonicalFields(member, memberFields, "member")
      val value = requiredNonBlankString(fields["value"], "members.value").trim()
      val id =
        try {
          UUID.fromString(value)
        } catch (_: IllegalArgumentException) {
          throw ScimErrors.invalidValue("members.value must be a User mapping id")
        }
      seen += id
    }
    return seen.toList()
  }

  private fun validateSchemas(node: JsonNode?) {
    if (node == null || !node.isArray || node.size() != 1 || !node[0].isTextual || node[0].asText() != SCIM_GROUP_SCHEMA) {
      throw ScimErrors.invalidValue("Group requires the core Group schema")
    }
  }

  private fun optionalExternalId(node: JsonNode?): String? =
    when {
      node == null || node.isNull -> null
      else -> requiredNonBlankString(node, "externalId")
    }

  private fun canonicalFields(
    node: ObjectNode,
    allowlist: Map<String, String>,
    container: String,
  ): Map<String, JsonNode> {
    val fields = linkedMapOf<String, JsonNode>()
    node.properties().forEach { (submittedName, value) ->
      val normalizedName = submittedName.asciiLowercaseOrNull() ?: throw ScimErrors.invalidValue("$container contains an unsupported field")
      val canonicalName = allowlist[normalizedName] ?: throw ScimErrors.invalidValue("$container contains an unsupported field")
      if (fields.put(canonicalName, value) != null) {
        throw ScimErrors.invalidValue("$container contains a duplicate field")
      }
    }
    return fields
  }

  private fun requiredString(
    node: JsonNode?,
    field: String,
  ): String {
    if (node == null || !node.isTextual) throw ScimErrors.invalidValue("$field must be a string")
    return node.asText()
  }

  private fun requiredNonBlankString(
    node: JsonNode?,
    field: String,
  ): String = requiredString(node, field).also { if (it.isBlank()) throw ScimErrors.invalidValue("$field must be non-empty") }

  private const val MAX_DISPLAY_NAME_LENGTH = 256
}
