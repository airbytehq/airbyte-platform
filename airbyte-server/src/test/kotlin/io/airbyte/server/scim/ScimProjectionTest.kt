/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micronaut.http.HttpStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ScimProjectionTest {
  private val objectMapper = jacksonObjectMapper()
  private val user =
    objectMapper.readTree(
      """
      {
        "schemas":["$SCIM_USER_SCHEMA"],
        "id":"user-id",
        "userName":"alice@example.com",
        "displayName":"Alice",
        "name":{"givenName":"Alice","familyName":"Example"},
        "emails":[{"value":"alice@example.com","type":"work","primary":true}],
        "meta":{"resourceType":"User","created":"2026-07-16T00:00:00Z","location":"https://example/scim/v2/Users/user-id"}
      }
      """.trimIndent(),
    ) as ObjectNode

  @Test
  fun `attributes retains requested fields subattributes and always-returned attributes`() {
    val projection = ScimProjection.parse("displayName,name.givenName,meta.location,emails.value", null, ScimProjectionSchemas.USER)

    assertThat(projection.apply(user)).isEqualTo(
      objectMapper.readTree(
        """
        {
          "schemas":["$SCIM_USER_SCHEMA"],
          "id":"user-id",
          "displayName":"Alice",
          "name":{"givenName":"Alice"},
          "emails":[{"value":"alice@example.com"}],
          "meta":{"location":"https://example/scim/v2/Users/user-id"}
        }
        """.trimIndent(),
      ),
    )
  }

  @Test
  fun `excludedAttributes removes fields and subattributes but not always-returned attributes`() {
    val projection = ScimProjection.parse(null, "displayName,meta.location,schemas,id,emails.value", ScimProjectionSchemas.USER)
    val projected = projection.apply(user)

    assertThat(projected.has("displayName")).isFalse()
    assertThat(projected["meta"].has("location")).isFalse()
    assertThat(projected["schemas"]).isEqualTo(user["schemas"])
    assertThat(projected["id"]).isEqualTo(user["id"])
    assertThat(projected["emails"][0].has("value")).isFalse()
  }

  @Test
  fun `excluding Group members removes the parent collection despite always-returned member values`() {
    val group =
      objectMapper.readTree(
        """
        {
          "schemas":["$SCIM_GROUP_SCHEMA"],
          "id":"group-id",
          "displayName":"Engineering",
          "members":[{"value":"user-id","${'$'}ref":"https://example/scim/v2/Users/user-id","display":"Alice"}]
        }
        """.trimIndent(),
      ) as ObjectNode

    val projected = ScimProjection.parse(null, "members", ScimProjectionSchemas.GROUP).apply(group)

    assertThat(projected.has("members")).isFalse()
  }

  @Test
  fun `excluding User groups removes the parent collection despite always-returned group values`() {
    val resource = user.deepCopy()
    resource.replace(
      "groups",
      objectMapper.readTree("""[{"value":"group-id","${'$'}ref":"https://example/scim/v2/Groups/group-id","display":"Engineering"}]"""),
    )

    val projected = ScimProjection.parse(null, "groups", ScimProjectionSchemas.USER).apply(resource)

    assertThat(projected.has("groups")).isFalse()
  }

  @Test
  fun `nested Group member projection retains value only while members is requested`() {
    val group =
      objectMapper.readTree(
        """
        {
          "schemas":["$SCIM_GROUP_SCHEMA"],
          "id":"group-id",
          "displayName":"Engineering",
          "members":[{"value":"user-id","${'$'}ref":"https://example/scim/v2/Users/user-id","display":"Alice"}]
        }
        """.trimIndent(),
      ) as ObjectNode

    val nested = ScimProjection.parse("members.display", null, ScimProjectionSchemas.GROUP).apply(group)
    val unrelated = ScimProjection.parse("displayName", null, ScimProjectionSchemas.GROUP).apply(group)

    assertThat(nested["members"][0]).isEqualTo(objectMapper.readTree("""{"value":"user-id","display":"Alice"}"""))
    assertThat(unrelated.has("members")).isFalse()
  }

  @Test
  fun `nested User group projection retains value only while groups is requested`() {
    val resource = user.deepCopy()
    resource.replace(
      "groups",
      objectMapper.readTree("""[{"value":"group-id","${'$'}ref":"https://example/scim/v2/Groups/group-id","display":"Engineering"}]"""),
    )

    val nested = ScimProjection.parse("groups.display", null, ScimProjectionSchemas.USER).apply(resource)
    val unrelated = ScimProjection.parse("displayName", null, ScimProjectionSchemas.USER).apply(resource)

    assertThat(nested["groups"][0]).isEqualTo(objectMapper.readTree("""{"value":"group-id","display":"Engineering"}"""))
    assertThat(unrelated.has("groups")).isFalse()
  }

  @Test
  fun `default projection returns an independent full representation`() {
    val projected = ScimProjection.parse(null, null, ScimProjectionSchemas.USER).apply(user)

    assertThat(projected).isEqualTo(user)
    assertThat(projected).isNotSameAs(user)
  }

  @Test
  fun `projection paths reject Unicode lookalikes and accept mixed canonical ASCII case`() {
    listOf(
      "uſerName",
      "urn:ietf:paramſ:scim:schemas:core:2.0:User:userName",
    ).forEach { attributes ->
      val exception =
        assertThrows<ScimException>(attributes) {
          ScimProjection.parse(attributes, null, ScimProjectionSchemas.USER)
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
    }

    val projection =
      ScimProjection.parse(
        "URN:IETF:PARAMS:SCIM:SCHEMAS:CORE:2.0:USER:USERNAME",
        null,
        ScimProjectionSchemas.USER,
      )
    assertThat(projection.apply(user)["userName"].asText()).isEqualTo("alice@example.com")
  }

  @Test
  fun `projection paths reject whitespace without normalizing it`() {
    listOf(
      Pair(" userName", null),
      Pair("userName ", null),
      Pair("userName, meta.location", null),
      Pair("userName ,meta.location", null),
      Pair("userName,\tmeta.location", null),
      Pair("userName,\u00A0meta.location", null),
      Pair(null, "meta.location\u2003"),
    ).forEach { (attributes, excludedAttributes) ->
      val exception =
        assertThrows<ScimException>(attributes ?: excludedAttributes ?: error("Missing projection parameter")) {
          ScimProjection.parse(attributes, excludedAttributes, ScimProjectionSchemas.USER)
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
    }

    val projection = ScimProjection.parse("USERNAME,META.LOCATION", null, ScimProjectionSchemas.USER)

    assertThat(projection.apply(user)).isEqualTo(
      objectMapper.readTree(
        """
        {
          "schemas":["$SCIM_USER_SCHEMA"],
          "id":"user-id",
          "userName":"alice@example.com",
          "meta":{"location":"https://example/scim/v2/Users/user-id"}
        }
        """.trimIndent(),
      ),
    )
  }

  @Test
  fun `Schema discovery meta projection exposes only resourceType and location`() {
    val schema =
      objectMapper.readTree(
        """
        {
          "schemas":["$SCIM_SCHEMA_SCHEMA"],
          "id":"$SCIM_USER_SCHEMA",
          "meta":{
            "resourceType":"Schema",
            "location":"https://example/scim/v2/Schemas/$SCIM_USER_SCHEMA"
          }
        }
        """.trimIndent(),
      ) as ObjectNode

    val included = ScimProjection.parse("meta.resourceType", null, ScimProjectionSchemas.SCHEMA).apply(schema)
    val excluded = ScimProjection.parse(null, "meta.location", ScimProjectionSchemas.SCHEMA).apply(schema)

    assertThat(included["meta"]["resourceType"].asText()).isEqualTo("Schema")
    assertThat(included["meta"].has("location")).isFalse()
    assertThat(excluded["meta"].has("resourceType")).isTrue()
    assertThat(excluded["meta"].has("location")).isFalse()

    listOf(
      Pair("meta.created", null),
      Pair(null, "meta.lastModified"),
    ).forEach { (attributes, excludedAttributes) ->
      val exception =
        assertThrows<ScimException> {
          ScimProjection.parse(attributes, excludedAttributes, ScimProjectionSchemas.SCHEMA)
        }
      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
    }
  }

  @Test
  fun `mutually exclusive malformed empty and unknown projections are invalidValue`() {
    listOf(
      Triple("id", "meta", ScimProjectionSchemas.USER),
      Triple("", null, ScimProjectionSchemas.USER),
      Triple("displayName,", null, ScimProjectionSchemas.USER),
      Triple("unknown", null, ScimProjectionSchemas.USER),
      Triple("name.unknown", null, ScimProjectionSchemas.USER),
      Triple("displayName.value", null, ScimProjectionSchemas.USER),
    ).forEach { (attributes, excluded, schema) ->
      val exception = assertThrows<ScimException> { ScimProjection.parse(attributes, excluded, schema) }
      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
    }
  }
}
