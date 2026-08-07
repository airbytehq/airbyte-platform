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
import java.util.UUID

class ScimGroupRequestParserTest {
  private val objectMapper = jacksonObjectMapper()

  @Test
  fun `parses required displayName optional externalId and direct User mapping ids`() {
    val first = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val second = UUID.fromString("22222222-2222-2222-2222-222222222222")

    val parsed =
      ScimGroupRequestParser.parse(
        json(
          """
          {
            "schemas": ["$SCIM_GROUP_SCHEMA"],
            "displayName": "Engineering",
            "externalId": "Case-Exact",
            "members": [
              {"value": "$first"},
              {"value": " $second ", "${'$'}ref": "https://ignored.example/Users/$second", "display": "Ignored"},
              {"value": "$first"}
            ]
          }
          """.trimIndent(),
        ),
      )

    assertThat(parsed.displayName).isEqualTo("Engineering")
    assertThat(parsed.externalId).isEqualTo("Case-Exact")
    assertThat(parsed.memberIds).containsExactly(first, second)
  }

  @Test
  fun `ignores arbitrary read-only resource and member enrichment values`() {
    val memberId = UUID.fromString("11111111-1111-1111-1111-111111111111")

    val parsed =
      ScimGroupRequestParser.parse(
        json(
          """
          {
            "schemas": ["$SCIM_GROUP_SCHEMA"],
            "id": null,
            "displayName": "Engineering",
            "members": [
              {
                "value": "$memberId",
                "${'$'}ref": {"ignored": true},
                "display": ["Ignored"]
              }
            ],
            "meta": {"version": 17, "ignored": {"nested": true}}
          }
          """.trimIndent(),
        ),
      )

    assertThat(parsed.displayName).isEqualTo("Engineering")
    assertThat(parsed.memberIds).containsExactly(memberId)
  }

  @Test
  fun `omitted and empty members both produce no memberships`() {
    val omitted = ScimGroupRequestParser.parse(group(""))
    val empty = ScimGroupRequestParser.parse(group(""", "members": []"""))

    assertThat(omitted.memberIds).isEmpty()
    assertThat(empty.memberIds).isEmpty()
  }

  @Test
  fun `explicit null externalId clears it`() {
    val parsed = ScimGroupRequestParser.parse(group(""", "externalId": null"""))

    assertThat(parsed.externalId).isNull()
  }

  @Test
  fun `accepts displayName containing 256 PostgreSQL characters`() {
    val displayName = "😀".repeat(256)

    val parsed = ScimGroupRequestParser.parse(json("""{"schemas":["$SCIM_GROUP_SCHEMA"],"displayName":"$displayName"}"""))

    assertThat(parsed.displayName).isEqualTo(displayName)
  }

  @Test
  fun `rejects displayName containing 257 PostgreSQL characters`() {
    val displayName = "😀".repeat(257)

    val error =
      assertThrows<ScimException> {
        ScimGroupRequestParser.parse(json("""{"schemas":["$SCIM_GROUP_SCHEMA"],"displayName":"$displayName"}"""))
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(error.scimType).isEqualTo("invalidValue")
  }

  @Test
  fun `rejects additional schema declarations`() {
    listOf(
      "http://schemas.microsoft.com/2006/11/ResourceManagement/ADSCIM/2.0/Group",
      "urn:ietf:params:scim:schemas:extension:airbyte:2.0:Group",
      SCIM_GROUP_SCHEMA,
    ).forEach { additionalSchema ->
      val error =
        assertThrows<ScimException> {
          ScimGroupRequestParser.parse(
            json(
              """
              {
                "schemas": ["$SCIM_GROUP_SCHEMA", "$additionalSchema"],
                "displayName": "Engineering"
              }
              """.trimIndent(),
            ),
          )
        }

      assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(error.scimType).isEqualTo("invalidValue")
    }
  }

  @Test
  fun `rejects blank required and optional strings`() {
    listOf(
      json("""{"schemas":["$SCIM_GROUP_SCHEMA"],"displayName":" "}"""),
      group(", \"externalId\": \" \""),
    ).forEach { body ->
      val error = assertThrows<ScimException> { ScimGroupRequestParser.parse(body) }

      assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(error.scimType).isEqualTo("invalidValue")
    }
  }

  @Test
  fun `rejects missing core schema unsupported fields and malformed member ids`() {
    listOf(
      json("""{"schemas":["urn:example:Group"],"displayName":"Engineering"}"""),
      group(", \"description\": \"unsupported\""),
      group(", \"members\": [{\"value\":\"not-a-uuid\"}]"),
      group(", \"members\": null"),
      group(", \"members\": [{\"value\":\"11111111-1111-1111-1111-111111111111\",\"type\":\"Group\"}]"),
    ).forEach { body ->
      val error = assertThrows<ScimException> { ScimGroupRequestParser.parse(body) }

      assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(error.scimType).isEqualTo("invalidValue")
    }
  }

  private fun group(suffix: String): ObjectNode =
    json(
      """
      {
        "schemas": ["$SCIM_GROUP_SCHEMA"],
        "displayName": "Engineering"
        $suffix
      }
      """.trimIndent(),
    )

  private fun json(value: String): ObjectNode = objectMapper.readTree(value) as ObjectNode
}
