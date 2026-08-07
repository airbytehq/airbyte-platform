/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.scim.generated.models.ScimGroup
import io.airbyte.domain.models.scim.ScimGroupRead
import jakarta.inject.Singleton
import java.net.URI

@Singleton
open class ScimGroupResourceService(
  private val objectMapper: ObjectMapper,
) {
  open fun render(
    group: ScimGroupRead,
    baseUri: URI,
    attributes: String?,
    excludedAttributes: String?,
  ): ScimGroup = render(group, baseUri, compileProjection(attributes, excludedAttributes))

  open fun compileProjection(
    attributes: String?,
    excludedAttributes: String?,
  ): ScimProjection = ScimProjection.parse(attributes, excludedAttributes, ScimProjectionSchemas.GROUP)

  open fun render(
    group: ScimGroupRead,
    baseUri: URI,
    projection: ScimProjection,
  ): ScimGroup = objectMapper.treeToValue(projection.apply(completeResource(group, baseUri)), ScimGroup::class.java)

  open fun completeResource(
    group: ScimGroupRead,
    baseUri: URI,
  ): ObjectNode =
    objectMapper.createObjectNode().also { resource ->
      resource.set<ArrayNode>("schemas", resource.arrayNode().add(SCIM_GROUP_SCHEMA))
      resource.put("id", group.id.toString())
      group.externalId?.let { resource.put("externalId", it) }
      resource.put("displayName", group.displayName)
      resource.set<ArrayNode>(
        "members",
        resource.arrayNode().also { members ->
          group.members.forEach { member ->
            members.addObject().also {
              it.put("value", member.id.toString())
              it.put("${'$'}ref", canonicalUserLocation(baseUri, member.id.toString()).toString())
              it.put("display", member.display)
            }
          }
        },
      )
      resource.set<ObjectNode>(
        "meta",
        resource.objectNode().also {
          it.put("resourceType", "Group")
          it.put("created", group.createdAt.toString())
          it.put("lastModified", group.updatedAt.toString())
          it.put("location", canonicalGroupLocation(baseUri, group.id.toString()).toString())
        },
      )
    }

  fun canonicalGroupLocation(
    baseUri: URI,
    id: String,
  ): URI = baseUri.resolve("scim/v2/Groups/$id")

  private fun canonicalUserLocation(
    baseUri: URI,
    id: String,
  ): URI = baseUri.resolve("scim/v2/Users/$id")
}
