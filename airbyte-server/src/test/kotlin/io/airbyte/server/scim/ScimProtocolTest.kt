/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.api.scim.generated.models.ScimGroup
import io.airbyte.api.scim.generated.models.ScimMeta
import io.airbyte.api.scim.generated.models.ScimUser
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpStatus
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.net.URI
import java.time.OffsetDateTime

class ScimProtocolTest {
  @Test
  fun `created user response uses the canonical resource location`() {
    val user =
      ScimUser(
        schemas = listOf(SCIM_USER_SCHEMA),
        id = "user-id",
        userName = "user@example.com",
        active = true,
        meta = meta("User", "/scim/v2/Users/user-id"),
      )

    val response = scimCreated(user)

    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.body()).isSameAs(user)
    assertThat(response.contentType).contains(SCIM_MEDIA_TYPE)
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Users/user-id")
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Users/user-id")
  }

  @Test
  fun `created group response uses the canonical resource location`() {
    val group =
      ScimGroup(
        schemas = listOf(SCIM_GROUP_SCHEMA),
        id = "group-id",
        displayName = "Engineering",
        members = emptyList(),
        meta = meta("Group", "/scim/v2/Groups/group-id"),
      )

    val response = scimCreated(group)

    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.body()).isSameAs(group)
    assertThat(response.contentType).contains(SCIM_MEDIA_TYPE)
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Groups/group-id")
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Groups/group-id")
  }

  @Test
  fun `created response requires canonical meta location`() {
    val projectedUser = ScimUser(schemas = listOf(SCIM_USER_SCHEMA), id = "user-id")

    assertThatThrownBy { scimCreated(projectedUser) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Created SCIM User must include meta.location")
  }

  @Test
  fun `created user response keeps canonical headers when projection omits meta`() {
    val projectedUser = ScimUser(schemas = listOf(SCIM_USER_SCHEMA), id = "user-id")
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Users/user-id")

    val response = scimCreated(projectedUser, canonicalLocation)

    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.body()).isSameAs(projectedUser)
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(canonicalLocation.toString())
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(canonicalLocation.toString())
  }

  @Test
  fun `created group response keeps canonical headers when projection omits meta`() {
    val projectedGroup = ScimGroup(schemas = listOf(SCIM_GROUP_SCHEMA), id = "group-id")
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Groups/group-id")

    val response = scimCreated(projectedGroup, canonicalLocation)

    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.body()).isSameAs(projectedGroup)
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(canonicalLocation.toString())
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(canonicalLocation.toString())
  }

  @Test
  fun `created user response keeps canonical headers when projection omits meta location`() {
    val projectedUser =
      ScimUser(
        schemas = listOf(SCIM_USER_SCHEMA),
        id = "user-id",
        meta = ScimMeta(resourceType = "User"),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Users/user-id")

    val response = scimCreated(projectedUser, canonicalLocation)

    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(canonicalLocation.toString())
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(canonicalLocation.toString())
  }

  @Test
  fun `created group response keeps canonical headers when projection omits meta location`() {
    val projectedGroup =
      ScimGroup(
        schemas = listOf(SCIM_GROUP_SCHEMA),
        id = "group-id",
        meta = ScimMeta(resourceType = "Group"),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Groups/group-id")

    val response = scimCreated(projectedGroup, canonicalLocation)

    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(canonicalLocation.toString())
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(canonicalLocation.toString())
  }

  @Test
  fun `created user response accepts meta location matching canonical headers`() {
    val projectedUser =
      ScimUser(
        schemas = listOf(SCIM_USER_SCHEMA),
        id = "user-id",
        meta = meta("User", "/scim/v2/Users/user-id"),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Users/user-id")

    val response = scimCreated(projectedUser, canonicalLocation)

    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(projectedUser.meta?.location.toString())
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(projectedUser.meta?.location.toString())
  }

  @Test
  fun `created group response accepts meta location matching canonical headers`() {
    val projectedGroup =
      ScimGroup(
        schemas = listOf(SCIM_GROUP_SCHEMA),
        id = "group-id",
        meta = meta("Group", "/scim/v2/Groups/group-id"),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Groups/group-id")

    val response = scimCreated(projectedGroup, canonicalLocation)

    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(projectedGroup.meta?.location.toString())
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(projectedGroup.meta?.location.toString())
  }

  @Test
  fun `created user response rejects textually different meta location`() {
    val projectedUser =
      ScimUser(
        schemas = listOf(SCIM_USER_SCHEMA),
        id = "user-id",
        meta =
          ScimMeta(
            resourceType = "User",
            location = URI.create("HTTPS://AIRBYTE.EXAMPLE.COM/scim/v2/Users/user-id"),
          ),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Users/user-id")

    assertThatThrownBy { scimCreated(projectedUser, canonicalLocation) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Created SCIM User meta.location must match the canonical location")
  }

  @Test
  fun `created group response rejects textually different meta location`() {
    val projectedGroup =
      ScimGroup(
        schemas = listOf(SCIM_GROUP_SCHEMA),
        id = "group-id",
        meta =
          ScimMeta(
            resourceType = "Group",
            location = URI.create("HTTPS://AIRBYTE.EXAMPLE.COM/scim/v2/Groups/group-id"),
          ),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Groups/group-id")

    assertThatThrownBy { scimCreated(projectedGroup, canonicalLocation) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Created SCIM Group meta.location must match the canonical location")
  }

  @Test
  fun `created user response rejects meta location that differs from canonical headers`() {
    val projectedUser =
      ScimUser(
        schemas = listOf(SCIM_USER_SCHEMA),
        id = "user-id",
        meta = meta("User", "/scim/v2/Users/different-id"),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Users/user-id")

    assertThatThrownBy { scimCreated(projectedUser, canonicalLocation) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Created SCIM User meta.location must match the canonical location")
  }

  @Test
  fun `created group response rejects meta location that differs from canonical headers`() {
    val projectedGroup =
      ScimGroup(
        schemas = listOf(SCIM_GROUP_SCHEMA),
        id = "group-id",
        meta = meta("Group", "/scim/v2/Groups/different-id"),
      )
    val canonicalLocation = URI.create("https://airbyte.example.com/scim/v2/Groups/group-id")

    assertThatThrownBy { scimCreated(projectedGroup, canonicalLocation) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Created SCIM Group meta.location must match the canonical location")
  }

  @Test
  fun `updated response returns the full resource as SCIM JSON`() {
    val user =
      ScimUser(
        schemas = listOf(SCIM_USER_SCHEMA),
        id = "user-id",
        userName = "renamed@example.com",
        active = true,
        meta = meta("User", "/scim/v2/Users/user-id"),
      )

    val response = scimUpdated(user)

    assertThat(response.status).isEqualTo(HttpStatus.OK)
    assertThat(response.body()).isSameAs(user)
    assertThat(response.contentType).contains(SCIM_MEDIA_TYPE)
  }

  @Test
  fun `group PATCH without projection returns bodyless no content`() {
    val group = ScimGroup(schemas = listOf(SCIM_GROUP_SCHEMA), id = "group-id")

    val response = scimGroupPatched(group, attributes = null, excludedAttributes = null)

    assertThat(response.status).isEqualTo(HttpStatus.NO_CONTENT)
    assertThat(response.body()).isNull()
    assertThat(response.contentType).isEmpty()
  }

  @Test
  fun `group PATCH with either projection returns the Group as SCIM JSON`() {
    val group = ScimGroup(schemas = listOf(SCIM_GROUP_SCHEMA), id = "group-id")

    listOf(
      "displayName" to null,
      null to "members",
    ).forEach { (attributes, excludedAttributes) ->
      val response = scimGroupPatched(group, attributes, excludedAttributes)

      assertThat(response.status).isEqualTo(HttpStatus.OK)
      assertThat(response.body()).isSameAs(group)
      assertThat(response.contentType).contains(SCIM_MEDIA_TYPE)
    }
  }

  @Test
  fun `deleted response is bodyless`() {
    val response = scimDeleted()

    assertThat(response.status).isEqualTo(HttpStatus.NO_CONTENT)
    assertThat(response.body()).isNull()
    assertThat(response.contentType).isEmpty()
  }

  private fun meta(
    resourceType: String,
    path: String,
  ): ScimMeta =
    ScimMeta(
      resourceType = resourceType,
      created = TIMESTAMP,
      lastModified = TIMESTAMP,
      location = URI.create("https://airbyte.example.com$path"),
    )

  companion object {
    private val TIMESTAMP = OffsetDateTime.parse("2026-07-15T12:00:00Z")
  }
}
