/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

data class ScimProjectionAttribute(
  val name: String,
  val children: List<ScimProjectionAttribute> = emptyList(),
  val alwaysReturned: Boolean = false,
)

class ScimProjectionSchema(
  attributes: List<ScimProjectionAttribute>,
  internal val resourceSchema: String? = null,
) {
  internal val attributes = attributes.associateBy { it.name.lowercase() }
}

object ScimProjectionSchemas {
  private fun attribute(
    name: String,
    vararg children: ScimProjectionAttribute,
    alwaysReturned: Boolean = false,
  ) = ScimProjectionAttribute(name, children.toList(), alwaysReturned)

  private val META =
    attribute(
      "meta",
      attribute("resourceType"),
      attribute("created"),
      attribute("lastModified"),
      attribute("location"),
    )

  private val DISCOVERY_META =
    attribute(
      "meta",
      attribute("resourceType"),
      attribute("location"),
    )

  private fun schemaAttributeChildren(depth: Int): List<ScimProjectionAttribute> =
    listOf(
      attribute("name"),
      attribute("type"),
      attribute("multiValued"),
      attribute("description"),
      attribute("required"),
      attribute("canonicalValues"),
      attribute("caseExact"),
      attribute("mutability"),
      attribute("returned"),
      attribute("uniqueness"),
      attribute("referenceTypes"),
      if (depth > 0) {
        attribute("subAttributes", *schemaAttributeChildren(depth - 1).toTypedArray())
      } else {
        attribute("subAttributes")
      },
    )

  val SERVICE_PROVIDER_CONFIG =
    ScimProjectionSchema(
      listOf(
        attribute("schemas", alwaysReturned = true),
        attribute("documentationUri"),
        attribute("patch", attribute("supported")),
        attribute("bulk", attribute("supported"), attribute("maxOperations"), attribute("maxPayloadSize")),
        attribute("filter", attribute("supported"), attribute("maxResults")),
        attribute("changePassword", attribute("supported")),
        attribute("sort", attribute("supported")),
        attribute("etag", attribute("supported")),
        attribute(
          "authenticationSchemes",
          attribute("type"),
          attribute("name"),
          attribute("description"),
          attribute("specUri"),
          attribute("documentationUri"),
          attribute("primary"),
        ),
      ),
      resourceSchema = SCIM_SERVICE_PROVIDER_CONFIG_SCHEMA,
    )

  val RESOURCE_TYPE =
    ScimProjectionSchema(
      listOf(
        attribute("schemas", alwaysReturned = true),
        attribute("id", alwaysReturned = true),
        attribute("name"),
        attribute("endpoint"),
        attribute("description"),
        attribute("schema"),
      ),
      resourceSchema = SCIM_RESOURCE_TYPE_SCHEMA,
    )

  val SCHEMA =
    ScimProjectionSchema(
      listOf(
        attribute("schemas", alwaysReturned = true),
        attribute("id", alwaysReturned = true),
        attribute("name"),
        attribute("description"),
        DISCOVERY_META,
        attribute("attributes", *schemaAttributeChildren(2).toTypedArray()),
      ),
      resourceSchema = SCIM_SCHEMA_SCHEMA,
    )

  val USER =
    ScimProjectionSchema(
      listOf(
        attribute("schemas", alwaysReturned = true),
        attribute("id", alwaysReturned = true),
        attribute("externalId"),
        attribute("userName"),
        attribute(
          "name",
          attribute("formatted"),
          attribute("familyName"),
          attribute("givenName"),
          attribute("middleName"),
          attribute("honorificPrefix"),
          attribute("honorificSuffix"),
        ),
        attribute("displayName"),
        attribute("nickName"),
        attribute("profileUrl"),
        attribute("title"),
        attribute("userType"),
        attribute("preferredLanguage"),
        attribute("locale"),
        attribute("timezone"),
        attribute("active"),
        attribute("emails", attribute("value"), attribute("type"), attribute("primary")),
        attribute(
          "groups",
          attribute("value", alwaysReturned = true),
          attribute("\$ref"),
          attribute("display"),
        ),
        META,
      ),
      resourceSchema = SCIM_USER_SCHEMA,
    )

  val GROUP =
    ScimProjectionSchema(
      listOf(
        attribute("schemas", alwaysReturned = true),
        attribute("id", alwaysReturned = true),
        attribute("externalId"),
        attribute("displayName"),
        attribute(
          "members",
          attribute("value", alwaysReturned = true),
          attribute("\$ref"),
          attribute("display"),
        ),
        META,
      ),
      resourceSchema = SCIM_GROUP_SCHEMA,
    )
}

class ScimProjection private constructor(
  private val included: List<List<String>>?,
  private val excluded: List<List<String>>,
  private val alwaysReturned: List<List<String>>,
) {
  fun apply(resource: ObjectNode): ObjectNode {
    val topLevelAlwaysReturned = alwaysReturned.filter { it.size == 1 }
    val projected =
      if (included == null) {
        resource.deepCopy()
      } else {
        resource.objectNode().also { target ->
          (included + topLevelAlwaysReturned).forEach { copyPath(resource, target, it) }
        }
      }
    excluded.forEach { removePath(projected, it, alwaysReturned) }
    alwaysReturned.forEach { path ->
      if (path.size == 1 || projected.has(path.first())) {
        copyPath(resource, projected, path)
      }
    }
    return projected
  }

  companion object {
    fun parse(
      attributes: String?,
      excludedAttributes: String?,
      schema: ScimProjectionSchema,
    ): ScimProjection {
      if (attributes != null && excludedAttributes != null) {
        invalidProjection()
      }
      val included = attributes?.let { parsePaths(it, schema) }
      val excluded = excludedAttributes?.let { parsePaths(it, schema) }.orEmpty()
      return ScimProjection(included, excluded, alwaysPaths(schema.attributes.values))
    }

    private fun parsePaths(
      parameter: String,
      schema: ScimProjectionSchema,
    ): List<List<String>> {
      val rawPaths = parameter.split(',')
      if (rawPaths.isEmpty() || rawPaths.any { it.isBlank() }) {
        invalidProjection()
      }
      return rawPaths.map { rawPath ->
        val normalizedPath = schema.resourceSchema?.let { normalizeScimAttributePath(rawPath, it) } ?: rawPath
        val segments = normalizedPath.split('.')
        if (segments.any { it.isBlank() }) {
          invalidProjection()
        }
        canonicalPath(segments, schema.attributes)
      }
    }

    private fun canonicalPath(
      segments: List<String>,
      definitions: Map<String, ScimProjectionAttribute>,
    ): List<String> {
      val segment = segments.first().asciiLowercaseOrNull() ?: invalidProjection()
      val definition = definitions[segment] ?: invalidProjection()
      if (segments.size == 1) {
        return listOf(definition.name)
      }
      if (definition.children.isEmpty()) {
        invalidProjection()
      }
      return listOf(definition.name) +
        canonicalPath(segments.drop(1), definition.children.associateBy { it.name.lowercase() })
    }

    private fun alwaysPaths(
      attributes: Collection<ScimProjectionAttribute>,
      prefix: List<String> = emptyList(),
    ): List<List<String>> =
      attributes.flatMap { attribute ->
        val path = prefix + attribute.name
        (if (attribute.alwaysReturned) listOf(path) else emptyList()) + alwaysPaths(attribute.children, path)
      }
  }
}

private fun copyPath(
  source: ObjectNode,
  target: ObjectNode,
  path: List<String>,
) {
  val value = source.get(path.first()) ?: return
  if (path.size == 1) {
    target.replace(path.first(), value.deepCopy())
    return
  }
  when {
    value.isObject -> {
      val childTarget = (target.get(path.first()) as? ObjectNode) ?: target.objectNode().also { target.replace(path.first(), it) }
      copyPath(value as ObjectNode, childTarget, path.drop(1))
      if (childTarget.isEmpty) {
        target.remove(path.first())
      }
    }
    value.isArray -> {
      val childTarget = (target.get(path.first()) as? ArrayNode) ?: target.arrayNode().also { target.replace(path.first(), it) }
      while (childTarget.size() < value.size()) {
        childTarget.addObject()
      }
      value.forEachIndexed { index, element ->
        if (element is ObjectNode) {
          copyPath(element, childTarget[index] as ObjectNode, path.drop(1))
        }
      }
    }
  }
}

private fun removePath(
  resource: ObjectNode,
  path: List<String>,
  alwaysReturned: List<List<String>>,
) {
  if (alwaysReturned.any { it == path }) {
    return
  }
  if (path.size == 1) {
    resource.remove(path.first())
    return
  }
  when (val value = resource.get(path.first())) {
    is ObjectNode -> removePath(value, path.drop(1), alwaysReturned.mapNotNull { it.dropPrefix(path.first()) })
    is ArrayNode ->
      value.filterIsInstance<ObjectNode>().forEach {
        removePath(
          it,
          path.drop(1),
          alwaysReturned.mapNotNull { p ->
            p.dropPrefix(path.first())
          },
        )
      }
  }
}

private fun List<String>.dropPrefix(prefix: String): List<String>? = if (firstOrNull() == prefix) drop(1) else null

private fun invalidProjection(): Nothing = throw ScimErrors.invalidValue("The SCIM projection is malformed or unsupported")
