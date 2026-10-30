/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ScimUserRequestParserTest {
  private val objectMapper = jacksonObjectMapper()

  @Test
  fun `normalizes the full allowlist and selects the primary email`() {
    val result =
      ScimUserRequestParser.parse(
        json(
          """
          {
            "schemas":["$SCIM_USER_SCHEMA"],
            "externalId":"external-1",
            "userName":" alice ",
            "name":{
              "formatted":"Alice Example",
              "familyName":"Example",
              "givenName":"Alice",
              "middleName":"Q",
              "honorificPrefix":"Dr",
              "honorificSuffix":"III"
            },
            "displayName":"Alice",
            "nickName":"Al",
            "profileUrl":"https://example.com/alice",
            "title":"Engineer",
            "userType":"employee",
            "preferredLanguage":"en-US",
            "locale":"en-US",
            "timezone":"Asia/Kolkata",
            "active":false,
            "emails":[
              {"value":" alias@example.com ","type":"home"},
              {"value":" Alice@Example.com ","type":"other","primary":true}
            ],
            "id":"ignored",
            "meta":{
              "resourceType":"User",
              "created":"2026-07-17T00:00:00Z",
              "lastModified":"2026-07-17T01:00:00Z",
              "location":"https://example.com/scim/v2/Users/ignored"
            },
            "groups":[{
              "value":"ignored",
              "${'$'}ref":"https://example.com/scim/v2/Groups/ignored",
              "display":"Ignored group"
            }],
            "password":"never-store"
          }
          """.trimIndent(),
        ),
      )

    assertThat(result.userName).isEqualTo("alice")
    assertThat(result.externalId).isEqualTo("external-1")
    assertThat(result.primaryEmail).isEqualTo("Alice@Example.com")
    assertThat(result.active).isFalse()
    assertThat(result.attributes)
      .isEqualTo(
        json(
          """
          {
            "name":{
              "formatted":"Alice Example",
              "familyName":"Example",
              "givenName":"Alice",
              "middleName":"Q",
              "honorificPrefix":"Dr",
              "honorificSuffix":"III"
            },
            "displayName":"Alice",
            "nickName":"Al",
            "profileUrl":"https://example.com/alice",
            "title":"Engineer",
            "userType":"employee",
            "preferredLanguage":"en-US",
            "locale":"en-US",
            "timezone":"Asia/Kolkata",
            "emails":[
              {"value":"alias@example.com","type":"home"},
              {"value":"Alice@Example.com","type":"other","primary":true}
            ]
          }
          """.trimIndent(),
        ),
      )
  }

  @Test
  fun `defaults omitted active to true and selects a single work email case insensitively`() {
    val result =
      ScimUserRequestParser.parse(
        json(
          """
          {
            "SCHEMAS":["$SCIM_USER_SCHEMA"],
            "USERNAME":"alice",
            "EMAILS":[{"VALUE":"alice@example.com","TYPE":"WoRk"}]
          }
          """.trimIndent(),
        ),
      )

    assertThat(result.active).isTrue()
    assertThat(result.primaryEmail).isEqualTo("alice@example.com")
    assertThat(result.attributes).isEqualTo(json("""{"emails":[{"value":"alice@example.com","type":"WoRk"}]}"""))
  }

  @Test
  fun `does not fall back to userName for email selection`() {
    assertInvalidValue(
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice@example.com","emails":[{"value":"home@example.com","type":"home"}]}""",
    )
  }

  @Test
  fun `rejects missing null empty and malformed required values`() {
    listOf(
      "{}",
      """{"schemas":["$SCIM_USER_SCHEMA"],"emails":[{"value":"a@example.com","type":"work"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":null,"emails":[{"value":"a@example.com","type":"work"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":" ","emails":[{"value":"a@example.com","type":"work"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":null}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[null]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":" "}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"not-an-email","type":"work"}]}""",
    ).forEach(::assertInvalidValue)
  }

  @Test
  fun `accepts the core schema alone or with the enterprise user extension declaration`() {
    listOf(
      """["$SCIM_USER_SCHEMA"]""",
      """["$SCIM_USER_SCHEMA","$SCIM_ENTERPRISE_USER_SCHEMA"]""",
      """["$SCIM_ENTERPRISE_USER_SCHEMA","$SCIM_USER_SCHEMA"]""",
    ).forEach { schemas ->
      val result = ScimUserRequestParser.parse(json(userWithSchemas(schemas)))

      assertThat(result.userName).isEqualTo("alice")
      assertThat(result.primaryEmail).isEqualTo("a@example.com")
    }
  }

  @Test
  fun `accepts the Microsoft Entra ID create user request`() {
    val result =
      ScimUserRequestParser.parse(
        json(
          """
          {
            "schemas": ["$SCIM_USER_SCHEMA", "$SCIM_ENTERPRISE_USER_SCHEMA"],
            "externalId": "0a21f0f2-8d2a-4f8e-bf98-7363c4aed4ef",
            "userName": "Test_User_ab6490ee-1e48-479e-a20b-2d77186b5dd1",
            "active": true,
            "emails": [{"primary": true, "type": "work", "value": "Test_User_fd0ea19b-0777-472c-9f96-4f70d2226f2e@testuser.com"}],
            "meta": {"resourceType": "User"},
            "name": {"formatted": "givenName familyName", "familyName": "familyName", "givenName": "givenName"}
          }
          """.trimIndent(),
        ),
      )

    assertThat(result.userName).isEqualTo("Test_User_ab6490ee-1e48-479e-a20b-2d77186b5dd1")
    assertThat(result.externalId).isEqualTo("0a21f0f2-8d2a-4f8e-bf98-7363c4aed4ef")
    assertThat(result.primaryEmail).isEqualTo("Test_User_fd0ea19b-0777-472c-9f96-4f70d2226f2e@testuser.com")
    assertThat(result.active).isTrue()
  }

  @Test
  fun `rejects a missing or malformed schemas array`() {
    assertInvalidValue(
      """{"userName":"alice","emails":[{"value":"a@example.com","type":"work"}]}""",
      "User schemas must be a non-empty array of schema URIs",
    )
    listOf(
      "null",
      "\"$SCIM_USER_SCHEMA\"",
      "[]",
      """["$SCIM_USER_SCHEMA",1]""",
      """["$SCIM_USER_SCHEMA",null]""",
      """["$SCIM_USER_SCHEMA",{}]""",
    ).forEach { assertInvalidValue(userWithSchemas(it), "User schemas must be a non-empty array of schema URIs") }
  }

  @Test
  fun `rejects schemas without the core User schema`() {
    listOf(
      """["wrong"]""",
      """["$SCIM_ENTERPRISE_USER_SCHEMA"]""",
    ).forEach { assertInvalidValue(userWithSchemas(it), "User requires the core User schema") }
  }

  @Test
  fun `matches schema URIs case-sensitively`() {
    assertInvalidValue(userWithSchemas("""["${SCIM_USER_SCHEMA.uppercase()}"]"""), "User requires the core User schema")
    assertInvalidValue(
      userWithSchemas("""["$SCIM_USER_SCHEMA","${SCIM_ENTERPRISE_USER_SCHEMA.uppercase()}"]"""),
      "User schemas contains an unsupported schema URI",
    )
  }

  @Test
  fun `rejects unsupported schema URIs alongside the core User schema`() {
    listOf(
      """["$SCIM_USER_SCHEMA","urn:example:extension"]""",
      """["$SCIM_USER_SCHEMA","$SCIM_ENTERPRISE_USER_SCHEMA","urn:example:extension"]""",
    ).forEach { assertInvalidValue(userWithSchemas(it), "User schemas contains an unsupported schema URI") }
  }

  @Test
  fun `rejects duplicate schema URIs`() {
    listOf(
      """["$SCIM_USER_SCHEMA","$SCIM_USER_SCHEMA"]""",
      """["$SCIM_USER_SCHEMA","$SCIM_ENTERPRISE_USER_SCHEMA","$SCIM_ENTERPRISE_USER_SCHEMA"]""",
    ).forEach { assertInvalidValue(userWithSchemas(it), "User schemas contains a duplicate schema URI") }
  }

  @Test
  fun `rejects an enterprise user extension object even when its schema is declared`() {
    listOf(
      """{"schemas":["$SCIM_USER_SCHEMA","$SCIM_ENTERPRISE_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work"}],"$SCIM_ENTERPRISE_USER_SCHEMA":{"department":"Platform"}}""",
      """{"schemas":["$SCIM_USER_SCHEMA","$SCIM_ENTERPRISE_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work"}],"$SCIM_ENTERPRISE_USER_SCHEMA":{}}""",
    ).forEach { assertInvalidValue(it, "User contains an unsupported field") }
  }

  @Test
  fun `rejects unsupported top level and name attributes`() {
    listOf(
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work"}],"phoneNumbers":[]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work"}],"urn:example:extension":{"value":true}}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","name":{"givenName":"Alice","unsupported":"x"},"emails":[{"value":"a@example.com","type":"work"}]}""",
    ).forEach(::assertInvalidValue)
  }

  @Test
  fun `rejects wrong-shaped ignored User attributes`() {
    val invalidValues =
      listOf(
        "id" to listOf("{}", "[]", "1", "true", "null"),
        "password" to listOf("{}", "[]", "1", "true", "null"),
        "meta" to listOf("[]", "\"meta\"", "1", "true", "null", "{\"unknown\":\"value\"}", "{\"created\":false}"),
        "groups" to
          listOf(
            "{}",
            "\"group\"",
            "1",
            "true",
            "null",
            "[null]",
            "[{}]",
            "[{\"value\":1}]",
            "[{\"value\":\"group-1\",\"unknown\":\"value\"}]",
            "[{\"value\":\"group-1\",\"display\":false}]",
          ),
      )

    invalidValues.forEach { (field, values) ->
      values.forEach { value ->
        assertInvalidValue(
          """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work"}],"$field":$value}""",
        )
      }
    }
  }

  @Test
  fun `accepts and discards emails display`() {
    val result =
      ScimUserRequestParser.parse(
        json(
          """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work","display":"Alice"}]}""",
        ),
      )

    assertThat(result.primaryEmail).isEqualTo("a@example.com")
    assertThat(result.attributes).isEqualTo(json("""{"emails":[{"value":"a@example.com","type":"work"}]}"""))
  }

  @Test
  fun `rejects non-string emails display values`() {
    listOf(
      """{"unvalidated":true}""",
      "[]",
      "1",
      "true",
      "null",
    ).forEach { display ->
      assertInvalidValue(
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work","display":$display}]}""",
      )
    }
  }

  @Test
  fun `rejects ambiguous email selection and invalid subattribute types`() {
    listOf(
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","primary":true},{"value":"b@example.com","primary":true}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work"},{"value":"b@example.com","type":"WORK"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":1,"primary":false}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work","primary":"true"}]}""",
    ).forEach(::assertInvalidValue)
  }

  @Test
  fun `rejects malformed profile URL and case-insensitive duplicate email values`() {
    listOf(
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","profileUrl":"https://example .com/alice","emails":[{"value":"a@example.com","type":"work"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"a@example.com","type":"work"},{"value":" A@EXAMPLE.COM ","type":"home"}]}""",
    ).forEach(::assertInvalidValue)
  }

  @Test
  fun `rejects malformed RFC 5321 email values`() {
    listOf(
      "alice@.example.com",
      "alice@example..com",
      ".alice@example.com",
      "alice.@example.com",
      "alice..smith@example.com",
      "alice@-example.com",
      "alice@example-.com",
    ).forEach { email ->
      assertInvalidValue(
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"valid@example.com","type":"work","primary":true},{"value":"$email","type":"home"}]}""",
      )
    }
  }

  @Test
  fun `accepts valid RFC 5321 email edge cases`() {
    listOf(
      "alice+tag@example.travel",
      "\"alice smith\"@example.com",
      "alice@[192.0.2.1]",
    ).forEach { email ->
      val escapedEmail = objectMapper.writeValueAsString(email)
      val result =
        ScimUserRequestParser.parse(
          json(
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":$escapedEmail,"type":"work"}]}""",
          ),
        )

      assertThat(result.primaryEmail).isEqualTo(email)
    }
  }

  @Test
  fun `enforces the RFC mailbox total length boundary`() {
    val maximumLengthEmail = "${"l".repeat(64)}@${"d".repeat(63)}.${"e".repeat(63)}.${"f".repeat(61)}"
    val oversizedEmail = maximumLengthEmail + "f"

    val result =
      ScimUserRequestParser.parse(
        json(
          """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"$maximumLengthEmail","type":"work"}]}""",
        ),
      )

    assertThat(result.primaryEmail).hasSize(254)
    assertInvalidValue(
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[{"value":"$oversizedEmail","type":"work"}]}""",
    )
  }

  @Test
  fun `profile URL requires a hierarchical HTTP or HTTPS URI with a host`() {
    listOf(
      "urn:example:user:alice",
      "mailto:alice@example.com",
      "file:///profiles/alice",
      "https:profile",
      "https:///profiles/alice",
      "//example.com/profiles/alice",
      "ftp://example.com/profiles/alice",
    ).forEach { profileUrl ->
      assertInvalidValue(
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","profileUrl":"$profileUrl","emails":[{"value":"alice@example.com","type":"work"}]}""",
      )
    }

    listOf(
      "http://example.com/profiles/alice",
      "https://profiles.example.com:8443/users/alice?view=full#bio",
    ).forEach { profileUrl ->
      val result =
        ScimUserRequestParser.parse(
          json(
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","profileUrl":"$profileUrl","emails":[{"value":"alice@example.com","type":"work"}]}""",
          ),
        )

      assertThat(result.attributes.path("profileUrl").asText()).isEqualTo(profileUrl)
    }
  }

  @Test
  fun `rejects blank externalId null optional strings duplicate canonical fields and invalid active`() {
    listOf(
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","externalId":" ","emails":[{"value":"a@example.com","type":"work"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","displayName":null,"emails":[{"value":"a@example.com","type":"work"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","active":0,"emails":[{"value":"a@example.com","type":"work"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","UserName":"other","emails":[{"value":"a@example.com","type":"work"}]}""",
    ).forEach(::assertInvalidValue)
  }

  @Test
  fun `explicit null externalId clears it`() {
    val result =
      ScimUserRequestParser.parse(
        json(
          """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","externalId":null,"emails":[{"value":"a@example.com","type":"work"}]}""",
        ),
      )

    assertThat(result.externalId).isNull()
  }

  @Test
  fun `input is not mutated when validation fails`() {
    val body = json("""{"schemas":["$SCIM_USER_SCHEMA"],"userName":"alice","emails":[]}""")
    val before = body.deepCopy()

    val thrown = assertThrows<ScimException> { ScimUserRequestParser.parse(body) }

    assertThat(body).isEqualTo(before)
    assertSame(body, body)
    assertThat(thrown.scimType).isEqualTo("invalidValue")
  }

  private fun assertInvalidValue(request: String) {
    val exception = assertThrows<ScimException> { ScimUserRequestParser.parse(json(request)) }
    assertThat(exception.scimType).isEqualTo("invalidValue")
  }

  private fun assertInvalidValue(
    request: String,
    detail: String,
  ) {
    val exception = assertThrows<ScimException> { ScimUserRequestParser.parse(json(request)) }
    assertThat(exception.scimType).isEqualTo("invalidValue")
    assertThat(exception.message).isEqualTo(detail)
  }

  private fun userWithSchemas(schemas: String) = """{"schemas":$schemas,"userName":"alice","emails":[{"value":"a@example.com","type":"work"}]}"""

  private fun json(value: String): ObjectNode = objectMapper.readTree(value) as ObjectNode
}
