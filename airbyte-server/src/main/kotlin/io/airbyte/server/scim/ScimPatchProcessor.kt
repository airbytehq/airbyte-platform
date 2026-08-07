/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.Locale
import java.util.UUID
import java.util.regex.Pattern

data class ScimTenant(
  val configurationId: UUID,
  val organizationId: UUID,
)

fun interface ScimMemberValidator {
  fun areActiveUsers(
    configurationId: UUID,
    organizationId: UUID,
    memberIds: Set<String>,
  ): Boolean
}

data class ScimUserActiveTransition(
  val from: Boolean,
  val to: Boolean,
)

data class ScimUserPatchResult(
  val resource: ObjectNode,
  val activeTransitions: List<ScimUserActiveTransition>,
)

object ScimPatchProcessor {
  private val userStrings =
    mapOf(
      "username" to "userName",
      "externalid" to "externalId",
      "displayname" to "displayName",
      "nickname" to "nickName",
      "profileurl" to "profileUrl",
      "title" to "title",
      "usertype" to "userType",
      "preferredlanguage" to "preferredLanguage",
      "locale" to "locale",
      "timezone" to "timezone",
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
  private val readOnlyUserFields = setOf("schemas", "id", "meta", "groups", "password")
  private val pathlessUserFields =
    (userStrings.keys + readOnlyUserFields + setOf("active", "name", "emails")).associateWith { it }
  private val emailFields =
    mapOf(
      "value" to "value",
      "type" to "type",
      "primary" to "primary",
      "display" to "display",
    )
  private val readOnlyGroupFields = setOf("schemas", "id", "meta")
  private val userEmailPath =
    Pattern.compile("""^emails\[type eq "work"\]\.value$""", Pattern.CASE_INSENSITIVE)
  private val groupMemberPath =
    Pattern.compile("""^members\[value eq "([^"\\]+)"\]$""", Pattern.CASE_INSENSITIVE)
  private val groupMemberValuePath =
    Pattern.compile("""^members\[value eq "[^"\\]+"\]\.value$""", Pattern.CASE_INSENSITIVE)

  fun applyUser(
    current: ObjectNode,
    request: ObjectNode,
  ): ScimUserPatchResult {
    val working = current.deepCopy()
    val activeTransitions = mutableListOf<ScimUserActiveTransition>()
    operations(request).forEach { operation ->
      val previousActive = activeValue(working)
      applyUserOperation(working, operation)
      validateUser(working)
      val updatedActive = activeValue(working)
      if (previousActive != null && updatedActive != null && previousActive != updatedActive) {
        activeTransitions += ScimUserActiveTransition(from = previousActive, to = updatedActive)
      }
    }
    return ScimUserPatchResult(resource = working, activeTransitions = activeTransitions.toList())
  }

  fun validateUserPatch(request: ObjectNode) {
    operations(request).forEach(::validateUserOperation)
  }

  fun applyGroup(
    current: ObjectNode,
    request: ObjectNode,
    tenant: ScimTenant,
    memberValidator: ScimMemberValidator,
  ): ObjectNode {
    val working = current.deepCopy()
    val referencedMembers = linkedSetOf<String>()
    operations(request).forEach { operation ->
      applyGroupOperation(working, operation, referencedMembers)
      validateGroup(working)
    }
    referencedMembers += memberIds(working.get("members"))
    if (referencedMembers.isNotEmpty() &&
      !memberValidator.areActiveUsers(tenant.configurationId, tenant.organizationId, referencedMembers)
    ) {
      throw ScimErrors.invalidValue("Group members must be active Users in the authenticated SCIM tenant")
    }
    return working
  }

  private fun operations(request: ObjectNode): List<PatchOperation> {
    val requestFields =
      collectAsciiCaseInsensitiveFields(request) {
        ScimErrors.invalidValue("The PatchOp request contains an unsupported field")
      }
    if (requestFields.keys.any { it !in setOf("schemas", "operations") }) {
      throw ScimErrors.invalidValue("The PatchOp request contains an unsupported field")
    }
    val schemas = requestFields["schemas"]
    if (schemas == null || !schemas.isArray || schemas.size() != 1 || schemas[0].asText() != SCIM_PATCH_OP_SCHEMA) {
      throw ScimErrors.invalidValue("PATCH requires the PatchOp schema")
    }
    val operations = requestFields["operations"]
    if (operations == null || !operations.isArray || operations.isEmpty) {
      throw ScimErrors.invalidValue("PATCH requires one or more operations")
    }
    return operations.map { node ->
      if (node !is ObjectNode) {
        throw ScimErrors.invalidValue("Each PATCH operation must be an object")
      }
      val operationFields =
        collectAsciiCaseInsensitiveFields(node) {
          ScimErrors.invalidValue("A PATCH operation contains an unsupported field")
        }
      if (operationFields.keys.any { it !in setOf("op", "path", "value") }) {
        throw ScimErrors.invalidValue("A PATCH operation contains an unsupported field")
      }
      val op =
        when (
          operationFields["op"]
            ?.takeIf(JsonNode::isTextual)
            ?.asText()
            ?.asciiLowercaseOrNull()
        ) {
          "add" -> PatchVerb.ADD
          "replace" -> PatchVerb.REPLACE
          "remove" -> PatchVerb.REMOVE
          else -> throw ScimErrors.invalidValue("PATCH operation is missing or unsupported")
        }
      val pathNode = operationFields["path"]
      val path =
        when {
          pathNode == null || pathNode.isNull -> null
          !pathNode.isTextual || pathNode.asText().isBlank() -> throw ScimErrors.invalidPath()
          else -> pathNode.asText()
        }
      if (op != PatchVerb.REMOVE && "value" !in operationFields) {
        throw ScimErrors.invalidValue("PATCH add and replace operations require a value")
      }
      PatchOperation(op, path, operationFields["value"])
    }
  }

  private fun applyUserOperation(
    working: ObjectNode,
    operation: PatchOperation,
  ) {
    if (operation.path == null) {
      if (operation.op == PatchVerb.REMOVE) {
        throw ScimErrors.noTarget("A remove operation requires a path")
      }
      val value = operation.value as? ObjectNode ?: throw ScimErrors.invalidValue("A pathless operation requires an object value")
      if (value.isEmpty) throw ScimErrors.invalidValue("A pathless operation requires a non-empty object value")
      canonicalFields(value, pathlessUserFields, "Pathless User operation").forEach { (field, fieldValue) ->
        applyPathlessUserField(working, operation.op, field, fieldValue)
      }
      return
    }

    val path = normalizeScimAttributePath(operation.path, SCIM_USER_SCHEMA)
    if (path.contains('[')) {
      requireValuePathRoot(path, "emails")
      if (!userEmailPath.matcher(path).matches()) {
        throw ScimErrors.invalidFilter("The User PATCH value path is malformed or unsupported")
      }
      applyWorkEmailPath(working, operation)
      return
    }

    val segments = path.split('.')
    val root = segments.first().asciiLowercaseOrNull() ?: throw ScimErrors.invalidPath()
    if (segments.size == 1 && root == "schemas") {
      if (operation.op != PatchVerb.REMOVE) validateResourceSchema(operation.value, SCIM_USER_SCHEMA)
      return
    }
    if (segments.size == 1 && root in readOnlyUserFields) {
      if (operation.op != PatchVerb.REMOVE) {
        ScimUserInputValidation.validateIgnoredAttribute(root, operation.value, allowSingleGroup = root == "groups")
      }
      return
    }
    when {
      segments.size == 1 && root in userStrings -> applyString(working, userStrings.getValue(root), operation)
      segments.size == 1 && root == "active" -> applyBoolean(working, "active", operation)
      segments.size == 1 && root == "name" -> applyName(working, operation.op, operation.value)
      segments.size == 2 && root == "name" -> applyNameSubattribute(working, operation.op, segments[1], operation.value)
      segments.size == 1 && root == "emails" -> applyEmails(working, operation)
      else -> throw ScimErrors.invalidPath()
    }
  }

  private fun validateUserOperation(operation: PatchOperation) {
    if (operation.path == null) {
      if (operation.op == PatchVerb.REMOVE) {
        throw ScimErrors.noTarget("A remove operation requires a path")
      }
      val value = operation.value as? ObjectNode ?: throw ScimErrors.invalidValue("A pathless operation requires an object value")
      if (value.isEmpty) throw ScimErrors.invalidValue("A pathless operation requires a non-empty object value")
      canonicalFields(value, pathlessUserFields, "Pathless User operation").forEach { (field, fieldValue) ->
        validatePathlessUserField(operation.op, field, fieldValue)
      }
      return
    }

    val path = normalizeScimAttributePath(operation.path, SCIM_USER_SCHEMA)
    if (path.contains('[')) {
      requireValuePathRoot(path, "emails")
      if (!userEmailPath.matcher(path).matches()) {
        throw ScimErrors.invalidFilter("The User PATCH value path is malformed or unsupported")
      }
      validateWorkEmailPath(operation)
      return
    }

    val segments = path.split('.')
    val root = segments.first().asciiLowercaseOrNull() ?: throw ScimErrors.invalidPath()
    if (segments.size == 1 && root == "schemas") {
      if (operation.op != PatchVerb.REMOVE) validateResourceSchema(operation.value, SCIM_USER_SCHEMA)
      return
    }
    if (segments.size == 1 && root in readOnlyUserFields) {
      if (operation.op != PatchVerb.REMOVE) {
        ScimUserInputValidation.validateIgnoredAttribute(root, operation.value, allowSingleGroup = root == "groups")
      }
      return
    }
    when {
      segments.size == 1 && root in userStrings -> validateUserString(userStrings.getValue(root), operation)
      segments.size == 1 && root == "active" -> applyBoolean(JsonNodeFactory.instance.objectNode(), "active", operation)
      segments.size == 1 && root == "name" -> applyName(JsonNodeFactory.instance.objectNode(), operation.op, operation.value)
      segments.size == 2 && root == "name" ->
        applyNameValue(JsonNodeFactory.instance.objectNode(), segments[1], operation.value, operation.op == PatchVerb.REMOVE)
      segments.size == 1 && root == "emails" -> validateEmails(operation)
      else -> throw ScimErrors.invalidPath()
    }
  }

  private fun validatePathlessUserField(
    op: PatchVerb,
    field: String,
    value: JsonNode,
  ) {
    val normalized = field.asciiLowercaseOrNull() ?: throw ScimErrors.invalidPath()
    val operation = PatchOperation(op, field, value)
    when {
      normalized == "schemas" -> validateResourceSchema(value, SCIM_USER_SCHEMA)
      normalized in readOnlyUserFields -> ScimUserInputValidation.validateIgnoredAttribute(normalized, value)
      normalized in userStrings -> validateUserString(userStrings.getValue(normalized), operation)
      normalized == "active" -> applyBoolean(JsonNodeFactory.instance.objectNode(), "active", operation)
      normalized == "name" -> applyName(JsonNodeFactory.instance.objectNode(), op, value)
      normalized == "emails" -> validateEmails(operation)
      else -> throw ScimErrors.invalidPath()
    }
  }

  private fun validateUserString(
    field: String,
    operation: PatchOperation,
  ) {
    if (operation.op == PatchVerb.REMOVE && field == "userName") {
      throw ScimErrors.invalidValue("userName is required")
    }
    val target = JsonNodeFactory.instance.objectNode()
    applyString(target, field, operation)
    when {
      field == "userName" && target.path(field).asText().isBlank() -> throw ScimErrors.invalidValue("userName is required")
      field == "externalId" && target.has(field) && target.path(field).asText().isBlank() ->
        throw ScimErrors.invalidValue("externalId must be a non-empty string or null")
      field == "profileUrl" && target.has(field) -> ScimUserInputValidation.requireValidProfileUrl(target.path(field).asText())
    }
  }

  private fun validateEmails(operation: PatchOperation) {
    if (operation.op == PatchVerb.REMOVE) {
      throw ScimErrors.invalidValue("At least one email is required")
    }
    val submitted =
      if (operation.op == PatchVerb.ADD && operation.value is ObjectNode) {
        JsonNodeFactory.instance
          .arrayNode()
          .add(operation.value)
          .let(::normalizeEmails)
      } else {
        normalizeEmails(operation.value)
      }
    val primaryCount = submitted.count { it.get("primary")?.asBoolean() == true }
    if (primaryCount > 1) throw ScimErrors.invalidValue("Only one primary email is allowed")
    if (operation.op == PatchVerb.REPLACE) validateSelectableEmails(submitted)
  }

  private fun validateWorkEmailPath(operation: PatchOperation) {
    if (operation.op == PatchVerb.REMOVE) return
    val value = operation.value
    if (value == null || !value.isTextual || value.asText().isBlank()) {
      throw ScimErrors.invalidValue("Work email value must be a non-empty string")
    }
    ScimUserInputValidation.requireValidEmail(value.asText().trim())
  }

  private fun applyPathlessUserField(
    working: ObjectNode,
    op: PatchVerb,
    field: String,
    value: JsonNode,
  ) {
    val normalized = field.asciiLowercaseOrNull() ?: throw ScimErrors.invalidPath()
    when {
      normalized == "schemas" -> {
        validateResourceSchema(value, SCIM_USER_SCHEMA)
      }
      normalized in readOnlyUserFields -> ScimUserInputValidation.validateIgnoredAttribute(normalized, value)
      normalized in userStrings -> {
        applyString(working, userStrings.getValue(normalized), PatchOperation(op, field, value))
      }
      normalized == "active" -> {
        applyBoolean(working, "active", PatchOperation(op, field, value))
      }
      normalized == "name" -> {
        applyName(working, op, value)
      }
      normalized == "emails" -> {
        applyEmails(working, PatchOperation(op, field, value))
      }
      else -> throw ScimErrors.invalidPath()
    }
  }

  private fun applyString(
    working: ObjectNode,
    field: String,
    operation: PatchOperation,
  ) {
    if (operation.op == PatchVerb.REMOVE) {
      working.remove(field)
      return
    }
    val value = operation.value
    if (value == null || value.isNull) {
      if (field == "externalId") {
        working.remove(field)
        return
      }
      throw ScimErrors.invalidValue("$field must be a string")
    }
    if (!value.isTextual) {
      throw ScimErrors.invalidValue("$field must be a string")
    }
    working.put(field, value.asText())
  }

  private fun applyBoolean(
    working: ObjectNode,
    field: String,
    operation: PatchOperation,
  ) {
    if (operation.op == PatchVerb.REMOVE) {
      working.put(field, true)
      return
    }
    val value = operation.value
    if (value == null || !value.isBoolean) {
      throw ScimErrors.invalidValue("$field must be a boolean")
    }
    working.put(field, value.asBoolean())
  }

  private fun applyName(
    working: ObjectNode,
    op: PatchVerb,
    value: JsonNode?,
  ) {
    if (op == PatchVerb.REMOVE) {
      working.remove("name")
      return
    }
    val submitted = value as? ObjectNode ?: throw ScimErrors.invalidValue("name must be an object")
    val target = (working.get("name") as? ObjectNode)?.deepCopy() ?: working.objectNode()
    canonicalFields(submitted, nameFields, "name").forEach { (field, fieldValue) ->
      applyNameValue(target, field, fieldValue, false)
    }
    if (target.isEmpty) working.remove("name") else working.replace("name", target)
  }

  private fun applyNameSubattribute(
    working: ObjectNode,
    op: PatchVerb,
    field: String,
    value: JsonNode?,
  ) {
    val target = (working.get("name") as? ObjectNode)?.deepCopy() ?: working.objectNode()
    applyNameValue(target, field, value, op == PatchVerb.REMOVE)
    if (target.isEmpty) working.remove("name") else working.replace("name", target)
  }

  private fun applyNameValue(
    target: ObjectNode,
    submittedField: String,
    value: JsonNode?,
    remove: Boolean,
  ) {
    val normalizedField = submittedField.asciiLowercaseOrNull() ?: throw ScimErrors.invalidPath()
    val field = nameFields[normalizedField] ?: throw ScimErrors.invalidPath()
    if (remove) {
      target.remove(field)
    } else {
      if (value == null || value.isNull) throw ScimErrors.invalidValue("name.$field must be a string")
      if (!value.isTextual) throw ScimErrors.invalidValue("name.$field must be a string")
      target.put(field, value.asText())
    }
  }

  private fun applyEmails(
    working: ObjectNode,
    operation: PatchOperation,
  ) {
    if (operation.op == PatchVerb.REMOVE) {
      working.remove("emails")
      return
    }
    val submitted =
      if (operation.op == PatchVerb.ADD && operation.value is ObjectNode) {
        normalizeEmails(working.arrayNode().add(operation.value))
      } else {
        normalizeEmails(operation.value)
      }
    if (operation.op == PatchVerb.ADD) {
      val combined = normalizeEmails(working.get("emails"))
      if (submitted.count { it.get("primary")?.asBoolean() == true } == 1) {
        combined.filterIsInstance<ObjectNode>().forEach { it.put("primary", false) }
      }
      combined.addAll(submitted)
      working.replace("emails", combined)
    } else {
      working.replace("emails", submitted)
    }
  }

  private fun applyWorkEmailPath(
    working: ObjectNode,
    operation: PatchOperation,
  ) {
    val emails = normalizeEmails(working.get("emails"))
    val matching = emails.filterIsInstance<ObjectNode>().filter { it.get("type")?.asText()?.equals("work", ignoreCase = true) == true }
    if (operation.op == PatchVerb.REMOVE) {
      matching.forEach { it.remove("value") }
    } else {
      val value = operation.value
      if (value == null || !value.isTextual || value.asText().isBlank()) {
        throw ScimErrors.invalidValue("Work email value must be a non-empty string")
      }
      if (matching.isEmpty()) {
        if (operation.op == PatchVerb.REPLACE) {
          throw ScimErrors.noTarget("The User PATCH value path matched no values")
        }
        emails.addObject().put("value", value.asText().trim()).put("type", "work")
      } else {
        matching.forEach { it.put("value", value.asText().trim()) }
      }
    }
    working.replace("emails", emails)
  }

  private fun normalizeEmails(value: JsonNode?): ArrayNode {
    if (value == null || !value.isArray) {
      throw ScimErrors.invalidValue("emails must be an array")
    }
    val result = (value as ArrayNode).arrayNode()
    val seenValues = mutableSetOf<String>()
    value.forEach { emailNode ->
      val email = emailNode as? ObjectNode ?: throw ScimErrors.invalidValue("Each email must be an object")
      val normalized = result.objectNode()
      canonicalFields(email, emailFields, "email").forEach { (field, fieldValue) ->
        when (field) {
          "value" -> {
            if (!fieldValue.isTextual || fieldValue.asText().isBlank()) throw ScimErrors.invalidValue("Email value is required")
            normalized.put("value", fieldValue.asText().trim())
          }
          "type" -> {
            if (!fieldValue.isTextual || fieldValue.asText().isBlank()) throw ScimErrors.invalidValue("Email type must be a string")
            normalized.put("type", fieldValue.asText())
          }
          "primary" -> {
            if (!fieldValue.isBoolean) throw ScimErrors.invalidValue("Email primary must be a boolean")
            normalized.put("primary", fieldValue.asBoolean())
          }
          "display" -> {
            if (!fieldValue.isTextual) throw ScimErrors.invalidValue("Email display must be a string")
          }
          else -> throw ScimErrors.invalidPath("Unsupported email subattribute")
        }
      }
      if (!normalized.has("value")) throw ScimErrors.invalidValue("Email value is required")
      val emailValue = normalized.get("value").asText()
      ScimUserInputValidation.requireValidEmail(emailValue)
      if (!seenValues.add(emailValue.lowercase(Locale.ROOT))) throw ScimErrors.invalidValue("Email values must be unique")
      result.add(normalized)
    }
    return result
  }

  private fun canonicalFields(
    node: ObjectNode,
    allowlist: Map<String, String>,
    container: String,
  ): Map<String, JsonNode> {
    val result = linkedMapOf<String, JsonNode>()
    node.properties().forEach { (submittedName, value) ->
      val normalizedName = submittedName.asciiLowercaseOrNull() ?: throw ScimErrors.invalidPath()
      val canonicalName = allowlist[normalizedName] ?: throw ScimErrors.invalidPath()
      if (result.put(canonicalName, value) != null) {
        throw ScimErrors.invalidValue("$container contains a duplicate field")
      }
    }
    return result
  }

  private fun validateUser(working: ObjectNode) {
    val userName = working.get("userName")
    if (userName == null || !userName.isTextual || userName.asText().isBlank()) {
      throw ScimErrors.invalidValue("userName is required")
    }
    validateOptionalNonBlankString(working, "externalId")
    validateProfileUrl(working.get("profileUrl"))
    val emails = normalizeEmails(working.get("emails"))
    validateSelectableEmails(emails)
    working.replace("emails", emails)
  }

  private fun validateSelectableEmails(emails: ArrayNode) {
    if (emails.isEmpty) throw ScimErrors.invalidValue("At least one email is required")
    val primaryCount = emails.count { it.get("primary")?.asBoolean() == true }
    if (primaryCount > 1) throw ScimErrors.invalidValue("Only one primary email is allowed")
    if (primaryCount == 0 && emails.count { it.get("type")?.asText()?.equals("work", ignoreCase = true) == true } != 1) {
      throw ScimErrors.invalidValue("Exactly one selectable work email is required when no primary email is present")
    }
  }

  private fun validateProfileUrl(value: JsonNode?) {
    if (value == null) return
    if (!value.isTextual) throw ScimErrors.invalidValue("profileUrl must be a string")
    ScimUserInputValidation.requireValidProfileUrl(value.asText())
  }

  private fun applyGroupOperation(
    working: ObjectNode,
    operation: PatchOperation,
    referencedMembers: MutableSet<String>,
  ) {
    if (operation.path == null) {
      if (operation.op == PatchVerb.REMOVE) throw ScimErrors.noTarget("A remove operation requires a path")
      val value = operation.value as? ObjectNode ?: throw ScimErrors.invalidValue("A pathless operation requires an object value")
      if (value.isEmpty) throw ScimErrors.invalidValue("A pathless operation requires a non-empty object value")
      var hasMutableField = false
      collectAsciiCaseInsensitiveFields(value) { ScimErrors.invalidPath() }.forEach { (field, fieldValue) ->
        when (field) {
          "displayname" -> {
            applyString(working, "displayName", PatchOperation(operation.op, field, fieldValue))
            hasMutableField = true
          }
          "externalid" -> {
            applyString(working, "externalId", PatchOperation(operation.op, field, fieldValue))
            hasMutableField = true
          }
          "schemas" -> validateResourceSchema(fieldValue, SCIM_GROUP_SCHEMA)
          in readOnlyGroupFields -> Unit
          else -> throw ScimErrors.invalidPath()
        }
      }
      if (!hasMutableField) throw ScimErrors.invalidValue("A pathless operation requires a mutable attribute")
      return
    }

    val path = normalizeScimAttributePath(operation.path, SCIM_GROUP_SCHEMA)
    if (path.contains('[')) {
      requireValuePathRoot(path, "members")
      if (groupMemberValuePath.matcher(path).matches()) {
        throw ScimErrors.mutability("Group member values are immutable")
      }
      groupMemberPath.matcher(path).takeIf { it.matches() }?.let { match ->
        if (operation.op != PatchVerb.REMOVE) throw ScimErrors.invalidPath("Filtered member paths support remove only")
        val memberId = match.group(1)
        referencedMembers += memberId
        val currentMembers = working.get("members") ?: return
        val remaining = normalizeMembers(currentMembers).filterNot { it.get("value").asText() == memberId }
        working.replace("members", working.arrayNode().addAll(remaining))
        return
      }
      throw ScimErrors.invalidFilter("The Group PATCH value path is malformed or unsupported")
    }
    when (path.asciiLowercaseOrNull()) {
      "schemas" -> if (operation.op != PatchVerb.REMOVE) validateResourceSchema(operation.value, SCIM_GROUP_SCHEMA)
      "displayname" -> applyString(working, "displayName", operation)
      "externalid" -> applyString(working, "externalId", operation)
      "members" -> applyMembers(working, operation, referencedMembers)
      in readOnlyGroupFields -> Unit
      else -> throw ScimErrors.invalidPath()
    }
  }

  private fun applyMembers(
    working: ObjectNode,
    operation: PatchOperation,
    referencedMembers: MutableSet<String>,
  ) {
    if (operation.op == PatchVerb.REMOVE) {
      val removals = normalizeMembers(operation.value)
      val removalIds = removals.mapTo(linkedSetOf()) { it.get("value").asText() }
      referencedMembers += removalIds
      val currentMembers = working.get("members") ?: return
      val remaining = normalizeMembers(currentMembers).filterNot { it.get("value").asText() in removalIds }
      working.replace("members", working.arrayNode().addAll(remaining))
      return
    }
    val submitted = normalizeMembers(operation.value)
    referencedMembers += submitted.map { it.get("value").asText() }
    val result =
      if (operation.op == PatchVerb.ADD) {
        working.get("members")?.let(::normalizeMembers) ?: working.arrayNode()
      } else {
        working.arrayNode()
      }
    val seen = result.mapTo(linkedSetOf()) { it.get("value").asText() }
    submitted.forEach { if (seen.add(it.get("value").asText())) result.add(it) }
    working.replace("members", result)
  }

  private fun normalizeMembers(value: JsonNode?): ArrayNode {
    if (value == null || value.isNull) throw ScimErrors.invalidValue("members requires a value array")
    val submitted = if (value.isArray) value.toList() else listOf(value)
    val result = JsonNodeFactory.instance.arrayNode()
    val seen = linkedSetOf<String>()
    submitted.forEach { memberNode ->
      val member = memberNode as? ObjectNode ?: throw ScimErrors.invalidValue("Each member must be an object")
      val fields = collectAsciiCaseInsensitiveFields(member) { ScimErrors.invalidPath("Unsupported member subattribute") }
      if (fields.keys.any { it !in setOf("value", "display", "\$ref") }) {
        throw ScimErrors.invalidPath("Unsupported member subattribute")
      }
      val valueNode = fields["value"]
      if (valueNode == null || !valueNode.isTextual || valueNode.asText().isBlank()) {
        throw ScimErrors.invalidValue("Member value is required")
      }
      val memberId = valueNode.asText().trim()
      if (seen.add(memberId)) result.addObject().put("value", memberId)
    }
    return result
  }

  private fun memberIds(value: JsonNode?): Set<String> =
    if (value == null || value.isNull) emptySet() else normalizeMembers(value).mapTo(linkedSetOf()) { it.get("value").asText() }

  private fun validateGroup(working: ObjectNode) {
    val displayName = working.get("displayName")
    if (displayName == null || !displayName.isTextual || displayName.asText().isBlank()) {
      throw ScimErrors.invalidValue("displayName is required")
    }
    validateOptionalNonBlankString(working, "externalId")
    if (working.has("members")) working.replace("members", normalizeMembers(working.get("members")))
  }

  private fun validateOptionalNonBlankString(
    working: ObjectNode,
    field: String,
  ) {
    val value = working.get(field) ?: return
    if (!value.isTextual || value.asText().isBlank()) throw ScimErrors.invalidValue("$field must be a non-empty string or null")
  }

  private fun collectAsciiCaseInsensitiveFields(
    value: ObjectNode,
    invalidField: () -> ScimException,
  ): Map<String, JsonNode> {
    val fields = linkedMapOf<String, JsonNode>()
    value.properties().forEach { (field, fieldValue) ->
      val normalized = field.asciiLowercaseOrNull() ?: throw invalidField()
      if (fields.put(normalized, fieldValue) != null) {
        throw ScimErrors.invalidValue("A SCIM object contains an ambiguous duplicate field")
      }
    }
    return fields
  }

  private fun activeValue(working: ObjectNode): Boolean? = working.get("active")?.takeIf(JsonNode::isBoolean)?.asBoolean()

  private fun requireValuePathRoot(
    path: String,
    expectedRoot: String,
  ) {
    val root = path.substringBefore('[').trimEnd()
    if (!root.equalsAsciiIgnoreCase(expectedRoot)) throw ScimErrors.invalidPath()
  }

  private fun validateResourceSchema(
    value: JsonNode?,
    expectedSchema: String,
  ) {
    if (value == null || !value.isArray || value.size() != 1 || !value[0].isTextual || value[0].asText() != expectedSchema) {
      throw ScimErrors.invalidPath("PATCH supports only the advertised core resource schema")
    }
  }
}

private data class PatchOperation(
  val op: PatchVerb,
  val path: String?,
  val value: JsonNode?,
)

private enum class PatchVerb {
  ADD,
  REPLACE,
  REMOVE,
}
