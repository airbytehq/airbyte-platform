/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.domain.models.scim.ScimUserWrite
import java.util.Locale

object ScimUserRequestParser {
  private val topLevelFields =
    mapOf(
      "schemas" to "schemas",
      "id" to "id",
      "externalid" to "externalId",
      "username" to "userName",
      "name" to "name",
      "displayname" to "displayName",
      "nickname" to "nickName",
      "profileurl" to "profileUrl",
      "title" to "title",
      "usertype" to "userType",
      "preferredlanguage" to "preferredLanguage",
      "locale" to "locale",
      "timezone" to "timezone",
      "active" to "active",
      "emails" to "emails",
      "groups" to "groups",
      "meta" to "meta",
      "password" to "password",
    )
  private val nameFields =
    mapOf(
      "formatted" to "formatted",
      "familyname" to "familyName",
      "givenname" to "givenName",
      "middlename" to "middleName",
      "honorificprefix" to "honorificPrefix",
      "honorificsuffix" to "honorificSuffix",
    )
  private val simpleProfileFields =
    listOf(
      "displayName",
      "nickName",
      "profileUrl",
      "title",
      "userType",
      "preferredLanguage",
      "locale",
      "timezone",
    )

  fun parse(body: ObjectNode): ScimUserWrite {
    val fields = canonicalFields(body, topLevelFields, "User")
    validateSchema(fields["schemas"])
    listOf("id", "meta", "groups", "password").forEach { field ->
      fields[field]?.let { ScimUserInputValidation.validateIgnoredAttribute(field, it) }
    }

    val userName = requiredNonBlankString(fields["userName"], "userName").trim()
    val externalId = optionalExternalId(fields["externalId"])
    val active = optionalBoolean(fields["active"], "active") ?: true
    val emails = normalizeEmails(fields["emails"])
    val primaryEmail = selectPrimaryEmail(emails)

    val attributes = body.objectNode()
    fields["name"]?.let { attributes.replace("name", normalizeName(it, body)) }
    simpleProfileFields.forEach { field ->
      fields[field]?.let {
        val value = requiredString(it, field)
        if (field == "profileUrl") validateProfileUrl(value)
        attributes.put(field, value)
      }
    }
    attributes.replace("emails", emails)

    return ScimUserWrite(
      userName = userName,
      externalId = externalId,
      primaryEmail = primaryEmail,
      active = active,
      attributes = attributes,
    )
  }

  private fun normalizeName(
    node: JsonNode,
    factory: ObjectNode,
  ): ObjectNode {
    val submitted = node as? ObjectNode ?: throw ScimErrors.invalidValue("name must be an object")
    val fields = canonicalFields(submitted, nameFields, "name")
    return factory.objectNode().also { normalized ->
      nameFields.values.forEach { field ->
        fields[field]?.let { normalized.put(field, requiredString(it, "name.$field")) }
      }
    }
  }

  private fun normalizeEmails(node: JsonNode?): ArrayNode {
    if (node == null || !node.isArray || node.isEmpty) {
      throw ScimErrors.invalidValue("At least one email is required")
    }
    val source = node as ArrayNode
    val seenValues = mutableSetOf<String>()
    return source.arrayNode().also { normalized ->
      source.forEach { submittedNode ->
        val submitted = submittedNode as? ObjectNode ?: throw ScimErrors.invalidValue("Each email must be an object")
        val fields = canonicalFields(submitted, EMAIL_FIELDS, "email")
        val value = requiredNonBlankString(fields["value"], "Email value").trim()
        ScimUserInputValidation.requireValidEmail(value)
        if (!seenValues.add(value.lowercase(Locale.ROOT))) {
          throw ScimErrors.invalidValue("Email values must be unique")
        }
        fields["display"]?.let { requiredString(it, "Email display") }
        val email = normalized.objectNode().put("value", value)
        fields["type"]?.let { email.put("type", requiredNonBlankString(it, "Email type")) }
        fields["primary"]?.let { email.put("primary", requiredBoolean(it, "Email primary")) }
        normalized.add(email)
      }
    }
  }

  private fun validateProfileUrl(value: String) {
    ScimUserInputValidation.requireValidProfileUrl(value)
  }

  private fun selectPrimaryEmail(emails: ArrayNode): String {
    val primary = emails.filter { it.get("primary")?.asBoolean() == true }
    if (primary.size > 1) {
      throw ScimErrors.invalidValue("Only one primary email is allowed")
    }
    if (primary.size == 1) {
      return primary.single().get("value").asText()
    }
    val work = emails.filter { it.get("type")?.asText()?.equals("work", ignoreCase = true) == true }
    if (work.size != 1) {
      throw ScimErrors.invalidValue("Exactly one selectable work email is required when no primary email is present")
    }
    return work.single().get("value").asText()
  }

  private fun canonicalFields(
    node: ObjectNode,
    allowlist: Map<String, String>,
    container: String,
  ): Map<String, JsonNode> {
    val result = linkedMapOf<String, JsonNode>()
    node.properties().forEach { (submittedName, value) ->
      val normalizedName = submittedName.asciiLowercaseOrNull() ?: throw ScimErrors.invalidValue("$container contains an unsupported field")
      val canonicalName = allowlist[normalizedName] ?: throw ScimErrors.invalidValue("$container contains an unsupported field")
      if (result.put(canonicalName, value) != null) {
        throw ScimErrors.invalidValue("$container contains a duplicate field")
      }
    }
    return result
  }

  private fun optionalExternalId(node: JsonNode?): String? =
    when {
      node == null || node.isNull -> null
      else -> requiredNonBlankString(node, "externalId")
    }

  private fun optionalBoolean(
    node: JsonNode?,
    field: String,
  ): Boolean? =
    when {
      node == null -> null
      else -> requiredBoolean(node, field)
    }

  private fun requiredString(
    node: JsonNode?,
    field: String,
  ): String {
    if (node == null || !node.isTextual) {
      throw ScimErrors.invalidValue("$field must be a string")
    }
    return node.asText()
  }

  private fun requiredNonBlankString(
    node: JsonNode?,
    field: String,
  ): String {
    val value = requiredString(node, field)
    if (value.isBlank()) {
      throw ScimErrors.invalidValue("$field must be non-empty")
    }
    return value
  }

  private fun requiredBoolean(
    node: JsonNode?,
    field: String,
  ): Boolean {
    if (node == null || !node.isBoolean) {
      throw ScimErrors.invalidValue("$field must be a boolean")
    }
    return node.asBoolean()
  }

  private fun validateSchema(node: JsonNode?) {
    if (node == null || !node.isArray || node.size() != 1 || !node[0].isTextual || node[0].asText() != SCIM_USER_SCHEMA) {
      throw ScimErrors.invalidValue("User requires the core User schema")
    }
  }

  private val EMAIL_FIELDS =
    mapOf(
      "value" to "value",
      "type" to "type",
      "primary" to "primary",
      "display" to "display",
    )
}
