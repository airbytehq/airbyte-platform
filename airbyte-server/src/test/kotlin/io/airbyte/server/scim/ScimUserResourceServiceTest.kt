/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.domain.models.scim.ScimUserGroup
import io.airbyte.domain.models.scim.ScimUserRead
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

class ScimUserResourceServiceTest {
  private val objectMapper =
    jacksonObjectMapper()
      .findAndRegisterModules()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
  private val service = ScimUserResourceService(objectMapper)

  @Test
  fun `renders the persisted allowlist and server attributes with canonical group references`() {
    val user = user()

    val resource = service.render(user, URI.create("https://airbyte.example.com"), null, null)

    assertThat(objectMapper.valueToTree<JsonNode>(resource))
      .isEqualTo(
        objectMapper.readTree(
          """
          {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "id": "${user.id}",
            "externalId": "entra-123",
            "userName": "SCIM.User@example.com",
            "name": {"givenName": "SCIM", "familyName": "User"},
            "displayName": "SCIM User",
            "active": true,
            "emails": [{"value": "scim.user@example.com", "type": "work", "primary": true}],
            "groups": [{"value": "${user.groups.single().id}", "${'$'}ref": "https://airbyte.example.com/scim/v2/Groups/${user.groups.single().id}", "display": "Engineering"}],
            "meta": {
              "resourceType": "User",
              "created": "2026-07-16T01:02:03Z",
              "lastModified": "2026-07-17T04:05:06Z",
              "location": "https://airbyte.example.com/scim/v2/Users/${user.id}"
            }
          }
          """.trimIndent(),
        ),
      )
  }

  @Test
  fun `preserves a base URL path in User locations and Group references`() {
    val user = user()

    val resource = service.completeResource(user, URI.create("https://airbyte.example.com/airbyte/"))

    assertThat(resource.path("meta").path("location").asText())
      .isEqualTo("https://airbyte.example.com/airbyte/scim/v2/Users/${user.id}")
    assertThat(
      resource
        .path("groups")
        .single()
        .path("${'$'}ref")
        .asText(),
    ).isEqualTo("https://airbyte.example.com/airbyte/scim/v2/Groups/${user.groups.single().id}")
  }

  @Test
  fun `applies response projection after building the complete resource`() {
    val user = user()

    val resource = service.render(user, URI.create("https://airbyte.example.com"), "userName,groups.display", null)

    assertThat(objectMapper.valueToTree<JsonNode>(resource))
      .isEqualTo(
        objectMapper.readTree(
          """
          {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "id": "${user.id}",
            "userName": "SCIM.User@example.com",
            "groups": [{"value": "${user.groups.single().id}", "display": "Engineering"}]
          }
          """.trimIndent(),
        ),
      )
  }

  private fun user(): ScimUserRead =
    ScimUserRead(
      id = UUID.fromString("11111111-1111-1111-1111-111111111111"),
      configurationId = UUID.fromString("22222222-2222-2222-2222-222222222222"),
      organizationId = UUID.fromString("33333333-3333-3333-3333-333333333333"),
      userId = UUID.fromString("44444444-4444-4444-4444-444444444444"),
      externalId = "entra-123",
      userName = "SCIM.User@example.com",
      primaryEmail = "scim.user@example.com",
      active = true,
      attributes =
        objectMapper.createObjectNode().also {
          it.set<JsonNode>("name", objectMapper.readTree("""{"givenName":"SCIM","familyName":"User"}"""))
          it.put("displayName", "SCIM User")
          it.set<JsonNode>("emails", objectMapper.readTree("""[{"value":"scim.user@example.com","type":"work","primary":true}]"""))
        },
      createdAt = OffsetDateTime.parse("2026-07-16T01:02:03Z"),
      updatedAt = OffsetDateTime.parse("2026-07-17T04:05:06Z"),
      groups = listOf(ScimUserGroup(UUID.fromString("55555555-5555-5555-5555-555555555555"), "Engineering")),
    )
}
