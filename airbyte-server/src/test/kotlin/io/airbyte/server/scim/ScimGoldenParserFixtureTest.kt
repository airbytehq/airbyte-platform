/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ScimGoldenParserFixtureTest {
  private val objectMapper = jacksonObjectMapper()

  @Test
  fun `golden filters cover all normative User and Group grammar shapes`() {
    val fixtures = fixture("filter-golden.json")

    fixtures["supported"].forEach { fixture ->
      val parsed = parseFilter(fixture["resourceType"].asText(), fixture["filter"].asText())
      val actual =
        when (parsed) {
          is ScimFilter.And -> parsed.clauses
          is ScimFilter.Equal -> listOf(parsed)
        }
      val expected =
        fixture["expected"].map {
          ScimFilter.Equal(
            ScimFilter.Attribute.valueOf(it["attribute"].asText()),
            it["value"].asText(),
            it["caseExact"].asBoolean(),
          )
        }

      assertThat(actual).describedAs(fixture["filter"].asText()).isEqualTo(expected)
    }

    fixtures["unsupported"].forEach { fixture ->
      val exception =
        assertThrows<ScimException>(fixture["filter"].asText()) {
          parseFilter(fixture["resourceType"].asText(), fixture["filter"].asText())
        }
      assertThat(exception.scimType).isEqualTo("invalidFilter")
    }
  }

  @Test
  fun `golden projections cover supported core qualification and fail-closed schemas`() {
    val fixtures = fixture("projection-golden.json")

    fixtures["supported"].forEach { fixture ->
      val projection =
        ScimProjection.parse(
          fixture["attributes"].takeUnless(JsonNode::isNull)?.asText(),
          fixture["excludedAttributes"].takeUnless(JsonNode::isNull)?.asText(),
          projectionSchema(fixture["resourceType"].asText()),
        )

      assertThat(projection.apply(fixture["original"] as ObjectNode))
        .describedAs(fixture["name"].asText())
        .isEqualTo(fixture["expected"])
    }

    fixtures["unsupported"].forEach { fixture ->
      val exception =
        assertThrows<ScimException>(fixture["name"].asText()) {
          ScimProjection.parse(
            fixture["attributes"].takeUnless(JsonNode::isNull)?.asText(),
            fixture["excludedAttributes"].takeUnless(JsonNode::isNull)?.asText(),
            projectionSchema(fixture["resourceType"].asText()),
          )
        }

      assertThat(exception.scimType).describedAs(fixture["name"].asText()).isEqualTo("invalidValue")
    }
  }

  @Test
  fun `golden PATCH fixtures cover normative supported and fail-closed grammar shapes`() {
    val fixtures = fixture("patch-golden.json")

    fixtures["supported"].forEach { fixture ->
      val original = fixture["original"] as ObjectNode
      val request = fixture["request"] as ObjectNode
      val actual =
        when (fixture["resourceType"].asText()) {
          "User" -> ScimPatchProcessor.applyUser(original, request).resource
          "Group" -> ScimPatchProcessor.applyGroup(original, request, TENANT) { _, _, _ -> true }
          else -> error("Unknown fixture resource type")
        }

      assertThat(actual).describedAs(fixture["name"].asText()).isEqualTo(fixture["expected"])
    }

    fixtures["unsupported"].forEach { fixture ->
      val original = fixture["original"] as ObjectNode
      val unchanged = original.deepCopy()
      val request = fixture["request"] as ObjectNode
      val exception =
        assertThrows<ScimException>(fixture["name"].asText()) {
          when (fixture["resourceType"].asText()) {
            "User" -> ScimPatchProcessor.applyUser(original, request)
            "Group" -> ScimPatchProcessor.applyGroup(original, request, TENANT) { _, _, _ -> true }
            else -> error("Unknown fixture resource type")
          }
        }

      assertThat(exception.scimType).describedAs(fixture["name"].asText()).isEqualTo(fixture["expectedScimType"].asText())
      assertThat(original).describedAs("${fixture["name"].asText()} atomicity").isEqualTo(unchanged)
    }
  }

  private fun parseFilter(
    resourceType: String,
    filter: String,
  ): ScimFilter =
    when (resourceType) {
      "User" -> ScimFilterParser.parseUser(filter)
      "Group" -> ScimFilterParser.parseGroup(filter)
      else -> error("Unknown fixture resource type")
    }

  private fun projectionSchema(resourceType: String): ScimProjectionSchema =
    when (resourceType) {
      "ServiceProviderConfig" -> ScimProjectionSchemas.SERVICE_PROVIDER_CONFIG
      "ResourceType" -> ScimProjectionSchemas.RESOURCE_TYPE
      "Schema" -> ScimProjectionSchemas.SCHEMA
      "User" -> ScimProjectionSchemas.USER
      "Group" -> ScimProjectionSchemas.GROUP
      else -> error("Unknown fixture resource type")
    }

  private fun fixture(name: String): JsonNode = requireNotNull(javaClass.getResourceAsStream("/scim/parsing/$name")).use(objectMapper::readTree)

  companion object {
    private val TENANT =
      ScimTenant(
        configurationId = UUID.fromString("11111111-1111-1111-1111-111111111111"),
        organizationId = UUID.fromString("22222222-2222-2222-2222-222222222222"),
      )
  }
}
