/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.scim.generated.models.ScimUser
import io.airbyte.domain.models.scim.ScimUserRead
import jakarta.inject.Singleton
import java.net.URI

@Singleton
open class ScimUserResourceService(
  private val objectMapper: ObjectMapper,
) {
  open fun render(
    user: ScimUserRead,
    baseUri: URI,
    attributes: String?,
    excludedAttributes: String?,
  ): ScimUser = render(user, baseUri, compileProjection(attributes, excludedAttributes))

  open fun compileProjection(
    attributes: String?,
    excludedAttributes: String?,
  ): ScimProjection = ScimProjection.parse(attributes, excludedAttributes, ScimProjectionSchemas.USER)

  open fun render(
    user: ScimUserRead,
    baseUri: URI,
    projection: ScimProjection,
  ): ScimUser {
    val complete = completeResource(user, baseUri)
    val projected = projection.apply(complete)
    return objectMapper.treeToValue(projected, ScimUser::class.java)
  }

  open fun completeResource(
    user: ScimUserRead,
    baseUri: URI,
  ): ObjectNode =
    user.attributes.deepCopy().also { resource ->
      resource.set<ArrayNode>("schemas", resource.arrayNode().add(SCIM_USER_SCHEMA))
      resource.put("id", user.id.toString())
      if (user.externalId == null) resource.remove("externalId") else resource.put("externalId", user.externalId)
      resource.put("userName", user.userName)
      resource.put("active", user.active)
      resource.set<ArrayNode>(
        "groups",
        resource.arrayNode().also { groups ->
          user.groups.forEach { group ->
            groups.addObject().also {
              it.put("value", group.id.toString())
              it.put("${'$'}ref", canonicalGroupLocation(baseUri, group.id.toString()).toString())
              it.put("display", group.displayName)
            }
          }
        },
      )
      resource.set<ObjectNode>(
        "meta",
        resource.objectNode().also {
          it.put("resourceType", "User")
          it.put("created", user.createdAt.toString())
          it.put("lastModified", user.updatedAt.toString())
          it.put("location", canonicalUserLocation(baseUri, user.id.toString()).toString())
        },
      )
    }

  fun canonicalUserLocation(
    baseUri: URI,
    id: String,
  ): URI = baseUri.resolve("scim/v2/Users/$id")

  private fun canonicalGroupLocation(
    baseUri: URI,
    id: String,
  ): URI = baseUri.resolve("scim/v2/Groups/$id")
}
