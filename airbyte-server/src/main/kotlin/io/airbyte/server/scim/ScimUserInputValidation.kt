/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.micronaut.core.annotation.Introspected
import io.micronaut.validation.validator.Validator
import jakarta.validation.constraints.Email
import java.net.URI
import java.net.URISyntaxException

@Introspected
internal data class ScimEmailAddress(
  @field:Email val value: String,
)

internal object ScimUserInputValidation {
  private val validator = Validator.getInstance()

  fun requireValidEmail(value: String) {
    if (value.length > MAX_EMAIL_LENGTH || validator.validate(ScimEmailAddress(value)).isNotEmpty()) {
      throw ScimErrors.invalidValue("Email value is invalid")
    }
  }

  fun requireValidProfileUrl(value: String) {
    val uri =
      try {
        URI(value)
      } catch (_: URISyntaxException) {
        throw ScimErrors.invalidValue("profileUrl must be a valid HTTP or HTTPS URI")
      }
    if (!uri.isAbsolute ||
      uri.isOpaque ||
      (!uri.scheme.equals("http", ignoreCase = true) && !uri.scheme.equals("https", ignoreCase = true)) ||
      uri.host.isNullOrBlank()
    ) {
      throw ScimErrors.invalidValue("profileUrl must be a valid HTTP or HTTPS URI")
    }
  }

  fun validateIgnoredAttribute(
    field: String,
    value: JsonNode?,
    allowSingleGroup: Boolean = false,
  ) {
    when (field) {
      "id", "password" -> requireString(value, field)
      "meta" -> validateMeta(value)
      "groups" -> validateGroups(value, allowSingleGroup)
      else -> throw IllegalArgumentException("Unsupported ignored User attribute: $field")
    }
  }

  private fun validateMeta(value: JsonNode?) {
    val fields = objectFields(value, META_FIELDS, "meta")
    fields.forEach { (field, fieldValue) -> requireString(fieldValue, "meta.$field") }
  }

  private fun validateGroups(
    value: JsonNode?,
    allowSingleGroup: Boolean,
  ) {
    val groups =
      when {
        value?.isArray == true -> value.toList()
        allowSingleGroup && value is ObjectNode -> listOf(value)
        else -> throw ScimErrors.invalidValue("groups must be an array")
      }
    groups.forEach { groupValue ->
      val fields = objectFields(groupValue, GROUP_FIELDS, "groups entry")
      requireString(fields["value"], "groups.value")
      fields["\$ref"]?.let { requireString(it, "groups.\$ref") }
      fields["display"]?.let { requireString(it, "groups.display") }
    }
  }

  private fun objectFields(
    value: JsonNode?,
    allowlist: Map<String, String>,
    container: String,
  ): Map<String, JsonNode> {
    val submitted = value as? ObjectNode ?: throw ScimErrors.invalidValue("$container must be an object")
    val fields = linkedMapOf<String, JsonNode>()
    submitted.properties().forEach { (submittedName, fieldValue) ->
      val normalizedName = submittedName.asciiLowercaseOrNull() ?: throw ScimErrors.invalidValue("$container contains an unsupported field")
      val canonicalName = allowlist[normalizedName] ?: throw ScimErrors.invalidValue("$container contains an unsupported field")
      if (fields.put(canonicalName, fieldValue) != null) {
        throw ScimErrors.invalidValue("$container contains a duplicate field")
      }
    }
    return fields
  }

  private fun requireString(
    value: JsonNode?,
    field: String,
  ) {
    if (value == null || !value.isTextual) {
      throw ScimErrors.invalidValue("$field must be a string")
    }
  }

  private val META_FIELDS =
    mapOf(
      "resourcetype" to "resourceType",
      "created" to "created",
      "lastmodified" to "lastModified",
      "location" to "location",
    )

  private val GROUP_FIELDS =
    mapOf(
      "value" to "value",
      "\$ref" to "\$ref",
      "display" to "display",
    )

  private const val MAX_EMAIL_LENGTH = 254
}
