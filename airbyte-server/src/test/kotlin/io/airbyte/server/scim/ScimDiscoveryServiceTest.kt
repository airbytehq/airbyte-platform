/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micronaut.http.HttpStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ScimDiscoveryServiceTest {
  private val objectMapper = jacksonObjectMapper()
  private val service = ScimDiscoveryService()

  @Test
  fun `service provider configuration matches the approved payload`() {
    assertGolden(service.serviceProviderConfig(), "service-provider-config.json")
  }

  @Test
  fun `resource types match the approved payloads in stable order`() {
    assertGolden(service.listResourceTypes(), "resource-types.json")
    assertGolden(service.getResourceType("User"), "resource-type-user.json")
    assertGolden(service.getResourceType("Group"), "resource-type-group.json")
  }

  @Test
  fun `schemas match the approved supported subset in stable order`() {
    val expectedList =
      objectMapper.valueToTree<JsonNode>(
        mapOf(
          "schemas" to listOf("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
          "totalResults" to 2,
          "Resources" to listOf(readFixture("schema-user.json"), readFixture("schema-group.json")),
          "startIndex" to 1,
          "itemsPerPage" to 2,
        ),
      )

    assertThat(objectMapper.valueToTree<JsonNode>(service.listSchemas())).isEqualTo(expectedList)
    assertGolden(service.getSchema(SCIM_USER_SCHEMA), "schema-user.json")
    assertGolden(service.getSchema(SCIM_GROUP_SCHEMA), "schema-group.json")
  }

  @Test
  fun `discovery resources support requested and excluded projections`() {
    assertThat(objectMapper.valueToTree<JsonNode>(service.serviceProviderConfig("patch.supported", null)))
      .isEqualTo(
        objectMapper.readTree(
          """{"schemas":["$SCIM_SERVICE_PROVIDER_CONFIG_SCHEMA"],"patch":{"supported":true}}""",
        ),
      )

    val projectedSchemas = objectMapper.valueToTree<JsonNode>(service.listSchemas("name,meta.location", null))
    assertThat(projectedSchemas["Resources"]).allSatisfy { schema ->
      assertThat(schema.fieldNames().asSequence().toList()).containsExactlyInAnyOrder("schemas", "id", "name", "meta")
      assertThat(schema["meta"].fieldNames().asSequence().toList()).containsExactly("location")
    }

    val projectedResourceType = objectMapper.valueToTree<JsonNode>(service.getResourceType("User", null, "description,endpoint"))
    assertThat(projectedResourceType.fieldNames().asSequence().toList())
      .containsExactlyInAnyOrder("schemas", "id", "name", "schema")
  }

  @Test
  fun `discovery resources support case-insensitive fully qualified core projections`() {
    val serviceProviderConfig =
      objectMapper.valueToTree<JsonNode>(
        service.serviceProviderConfig(
          "UrN:iEtF:pArAmS:sCiM:sChEmAs:cOrE:2.0:sErViCePrOvIdErCoNfIg:PaTcH.sUpPoRtEd",
          null,
        ),
      )
    assertThat(serviceProviderConfig)
      .isEqualTo(
        objectMapper.readTree(
          """{"schemas":["$SCIM_SERVICE_PROVIDER_CONFIG_SCHEMA"],"patch":{"supported":true}}""",
        ),
      )

    val resourceTypes =
      objectMapper.valueToTree<JsonNode>(
        service.listResourceTypes("$SCIM_RESOURCE_TYPE_SCHEMA:endpoint", null),
      )
    assertThat(resourceTypes["Resources"]).allSatisfy { resourceType ->
      assertThat(resourceType.fieldNames().asSequence().toList()).containsExactlyInAnyOrder("schemas", "id", "endpoint")
    }

    val schemas =
      objectMapper.valueToTree<JsonNode>(
        service.listSchemas("$SCIM_SCHEMA_SCHEMA:name", null),
      )
    assertThat(schemas["Resources"]).allSatisfy { schema ->
      assertThat(schema.fieldNames().asSequence().toList()).containsExactlyInAnyOrder("schemas", "id", "name")
    }
  }

  @Test
  fun `invalid discovery projections use invalidValue`() {
    listOf(
      { service.serviceProviderConfig("patch", "bulk") },
      { service.listResourceTypes("unknown", null) },
      { service.getSchema(SCIM_USER_SCHEMA, "meta.unknown", null) },
      { service.getSchema(SCIM_USER_SCHEMA, "meta.created", null) },
      { service.getSchema(SCIM_USER_SCHEMA, null, "meta.lastModified") },
      { service.serviceProviderConfig("$SCIM_RESOURCE_TYPE_SCHEMA:patch.supported", null) },
      { service.getResourceType("User", "$SCIM_SCHEMA_SCHEMA:endpoint", null) },
      { service.getSchema(SCIM_USER_SCHEMA, "$SCIM_USER_SCHEMA:name", null) },
      { service.getSchema(SCIM_USER_SCHEMA, "urn:example:extension:Schema:name", null) },
    ).forEach { request ->
      val exception = assertThrows<ScimException> { request() }
      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
    }
  }

  @Test
  fun `membership mapping identifiers are always returned`() {
    val userGroups = requireNotNull(service.getSchema(SCIM_USER_SCHEMA).attributes).single { it.name == "groups" }
    val groupMembers = requireNotNull(service.getSchema(SCIM_GROUP_SCHEMA).attributes).single { it.name == "members" }

    assertThat(requireNotNull(userGroups.subAttributes).single { it.name == "value" }.returned)
      .isEqualTo(io.airbyte.api.scim.generated.models.ScimSchemaAttribute.Returned.ALWAYS)
    assertThat(requireNotNull(groupMembers.subAttributes).single { it.name == "value" }.returned)
      .isEqualTo(io.airbyte.api.scim.generated.models.ScimSchemaAttribute.Returned.ALWAYS)
  }

  @Test
  fun `User emails advertise mutation requirements`() {
    val emails = requireNotNull(service.getSchema(SCIM_USER_SCHEMA).attributes).single { it.name == "emails" }
    val emailValue = requireNotNull(emails.subAttributes).single { it.name == "value" }

    assertThat(emails.required).isTrue()
    assertThat(emailValue.required).isTrue()
  }

  @Test
  fun `group member identifiers are immutable`() {
    val groupMembers =
      requireNotNull(service.getSchema(SCIM_GROUP_SCHEMA).attributes)
        .single { it.name == "members" }
    val memberValue = requireNotNull(groupMembers.subAttributes).single { it.name == "value" }

    assertThat(groupMembers.mutability)
      .isEqualTo(io.airbyte.api.scim.generated.models.ScimSchemaAttribute.Mutability.READ_WRITE)
    assertThat(memberValue.mutability)
      .isEqualTo(io.airbyte.api.scim.generated.models.ScimSchemaAttribute.Mutability.IMMUTABLE)
  }

  @Test
  fun `unknown resource type returns a safe SCIM not found error`() {
    val exception = assertThrows<ScimException> { service.getResourceType("unknown") }

    assertThat(exception.status).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(exception.scimType).isNull()
    assertThat(exception.message).isEqualTo("ResourceType not found")
  }

  @Test
  fun `unknown schema returns a safe SCIM not found error`() {
    val exception = assertThrows<ScimException> { service.getSchema("urn:example:unknown") }

    assertThat(exception.status).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(exception.scimType).isNull()
    assertThat(exception.message).isEqualTo("Schema not found")
  }

  private fun assertGolden(
    actual: Any,
    fixtureName: String,
  ) {
    assertThat(objectMapper.valueToTree<JsonNode>(actual)).isEqualTo(readFixture(fixtureName))
  }

  private fun readFixture(fixtureName: String): JsonNode =
    requireNotNull(javaClass.getResourceAsStream("/scim/discovery/$fixtureName")).use {
      objectMapper.readTree(it)
    }
}
