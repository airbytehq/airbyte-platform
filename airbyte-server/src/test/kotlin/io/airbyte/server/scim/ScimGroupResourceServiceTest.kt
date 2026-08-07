/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.domain.models.scim.ScimGroupMember
import io.airbyte.domain.models.scim.ScimGroupRead
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

class ScimGroupResourceServiceTest {
  private val objectMapper =
    jacksonObjectMapper()
      .findAndRegisterModules()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
  private val service = ScimGroupResourceService(objectMapper)

  @Test
  fun `renders canonical Group fields exact member representations and complete meta`() {
    val group = group()

    val resource = service.render(group, URI.create("https://airbyte.example.com/"), null, null)

    assertThat(objectMapper.valueToTree<JsonNode>(resource))
      .isEqualTo(
        objectMapper.readTree(
          """
          {
            "schemas": ["$SCIM_GROUP_SCHEMA"],
            "id": "${group.id}",
            "externalId": "Case-Exact",
            "displayName": "Engineering",
            "members": [{
              "value": "${group.members.single().id}",
              "${'$'}ref": "https://airbyte.example.com/scim/v2/Users/${group.members.single().id}",
              "display": "Alice Example"
            }],
            "meta": {
              "resourceType": "Group",
              "created": "2026-07-16T01:02:03Z",
              "lastModified": "2026-07-17T04:05:06Z",
              "location": "https://airbyte.example.com/scim/v2/Groups/${group.id}"
            }
          }
          """.trimIndent(),
        ),
      )
  }

  @Test
  fun `preserves a base URL path in Group and User locations`() {
    val group = group()

    val resource = service.completeResource(group, URI.create("https://airbyte.example.com/airbyte/"))

    assertThat(resource.path("meta").path("location").asText())
      .isEqualTo("https://airbyte.example.com/airbyte/scim/v2/Groups/${group.id}")
    assertThat(
      resource
        .path("members")
        .single()
        .path("${'$'}ref")
        .asText(),
    ).isEqualTo("https://airbyte.example.com/airbyte/scim/v2/Users/${group.members.single().id}")
  }

  @Test
  fun `applies Group projection after building the complete representation`() {
    val group = group()

    val resource = service.render(group, URI.create("https://airbyte.example.com/"), "displayName,members.value", null)

    assertThat(objectMapper.valueToTree<JsonNode>(resource))
      .isEqualTo(
        objectMapper.readTree(
          """
          {
            "schemas": ["$SCIM_GROUP_SCHEMA"],
            "id": "${group.id}",
            "displayName": "Engineering",
            "members": [{"value": "${group.members.single().id}"}]
          }
          """.trimIndent(),
        ),
      )
  }

  @Test
  fun `omits null externalId and returns an empty member collection`() {
    val group = group().copy(externalId = null, members = emptyList())

    val resource = service.completeResource(group, URI.create("https://airbyte.example.com/"))

    assertThat(resource.has("externalId")).isFalse
    assertThat(resource.path("members").isArray).isTrue
    assertThat(resource.path("members")).isEmpty()
  }

  private fun group(): ScimGroupRead =
    ScimGroupRead(
      id = UUID.fromString("11111111-1111-1111-1111-111111111111"),
      configurationId = UUID.fromString("22222222-2222-2222-2222-222222222222"),
      organizationId = UUID.fromString("33333333-3333-3333-3333-333333333333"),
      groupId = UUID.fromString("44444444-4444-4444-4444-444444444444"),
      externalId = "Case-Exact",
      displayName = "Engineering",
      createdAt = OffsetDateTime.parse("2026-07-16T01:02:03Z"),
      updatedAt = OffsetDateTime.parse("2026-07-17T04:05:06Z"),
      members =
        listOf(
          ScimGroupMember(
            id = UUID.fromString("55555555-5555-5555-5555-555555555555"),
            userId = UUID.fromString("66666666-6666-6666-6666-666666666666"),
            display = "Alice Example",
          ),
        ),
    )
}
