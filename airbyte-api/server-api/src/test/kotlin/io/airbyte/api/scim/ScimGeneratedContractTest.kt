/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.scim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.scim.generated.apis.ScimDiscoveryApi
import io.airbyte.api.scim.generated.apis.ScimUsersApi
import io.airbyte.api.scim.generated.models.ScimAuthenticationScheme
import io.airbyte.api.scim.generated.models.ScimBulkConfiguration
import io.airbyte.api.scim.generated.models.ScimDiscoveryMeta
import io.airbyte.api.scim.generated.models.ScimEmail
import io.airbyte.api.scim.generated.models.ScimError
import io.airbyte.api.scim.generated.models.ScimFilterConfiguration
import io.airbyte.api.scim.generated.models.ScimGroup
import io.airbyte.api.scim.generated.models.ScimGroupListResponse
import io.airbyte.api.scim.generated.models.ScimGroupReference
import io.airbyte.api.scim.generated.models.ScimMeta
import io.airbyte.api.scim.generated.models.ScimPatchOperation
import io.airbyte.api.scim.generated.models.ScimResourceType
import io.airbyte.api.scim.generated.models.ScimResourceTypeListResponse
import io.airbyte.api.scim.generated.models.ScimSchema
import io.airbyte.api.scim.generated.models.ScimSchemaAttribute
import io.airbyte.api.scim.generated.models.ScimSchemaListResponse
import io.airbyte.api.scim.generated.models.ScimServiceProviderConfig
import io.airbyte.api.scim.generated.models.ScimUser
import io.airbyte.api.scim.generated.models.ScimUserListResponse
import io.airbyte.api.scim.generated.models.ScimUserReference
import io.airbyte.api.scim.generated.models.ScimUserReferenceRequest
import io.airbyte.commons.jackson.MoreMappers
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Body
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.PATCH
import jakarta.ws.rs.POST
import jakarta.ws.rs.PUT
import jakarta.ws.rs.Path
import jakarta.ws.rs.QueryParam
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths
import java.time.OffsetDateTime

class ScimGeneratedContractTest {
  private val runtimeObjectMapper = MoreMappers.initMapper()
  private val scimContract =
    requireNotNull(javaClass.classLoader.getResourceAsStream("scim.yaml")) { "scim.yaml must be available on the test classpath" }.use { input ->
      ObjectMapper(YAMLFactory()).readTree(input)
    }
  private val objectMapper =
    jacksonObjectMapper()
      .findAndRegisterModules()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

  @Test
  fun `SCIM generation emits only the Users, Groups, and discovery API interfaces`() {
    val generatedApiDirectory =
      Paths
        .get(
          ScimDiscoveryApi::class.java.protectionDomain.codeSource.location
            .toURI(),
        ).resolve(ScimDiscoveryApi::class.java.packageName.replace('.', '/'))
    val generatedApiClasses =
      Files.list(generatedApiDirectory).use { paths ->
        paths
          .map { it.fileName.toString() }
          .filter { it.endsWith("Api.class") && !it.contains('$') }
          .toList()
      }

    assertThat(generatedApiClasses)
      .containsExactlyInAnyOrder("ScimDiscoveryApi.class", "ScimUsersApi.class", "ScimGroupsApi.class")
  }

  @Test
  fun `discovery API routes are rooted at the SCIM base path`() {
    val apiClass = ScimDiscoveryApi::class.java
    val rootPath = apiClass.getAnnotation(Path::class.java).value
    val effectivePaths = apiClass.declaredMethods.map { rootPath + it.getAnnotation(Path::class.java).value }

    assertThat(effectivePaths)
      .containsExactlyInAnyOrder(
        "/scim/v2/ServiceProviderConfig",
        "/scim/v2/ResourceTypes",
        "/scim/v2/ResourceTypes/{id}",
        "/scim/v2/Schemas",
        "/scim/v2/Schemas/{schemaUri}",
      )
  }

  @Test
  fun `discovery API exposes exactly the five read operations`() {
    val methods = ScimDiscoveryApi::class.java.declaredMethods

    assertThat(methods).hasSize(5)
    assertThat(methods).allMatch { it.isAnnotationPresent(GET::class.java) }
    assertThat(methods.map { it.name })
      .containsExactlyInAnyOrder(
        "getServiceProviderConfig",
        "listResourceTypes",
        "getResourceType",
        "listSchemas",
        "getSchema",
      )
  }

  @Test
  fun `Users API exposes the complete protocol surface at the SCIM base path`() {
    val apiClass = ScimUsersApi::class.java
    val rootPath = apiClass.getAnnotation(Path::class.java).value
    val methods = apiClass.declaredMethods.associateBy { it.name }

    assertThat(methods.keys)
      .containsExactlyInAnyOrder("listUsers", "createUser", "getUser", "replaceUser", "patchUser", "deleteUser")
    assertThat(rootPath + methods.getValue("listUsers").getAnnotation(Path::class.java).value).isEqualTo("/scim/v2/Users")
    assertThat(rootPath + methods.getValue("createUser").getAnnotation(Path::class.java).value).isEqualTo("/scim/v2/Users")
    listOf("getUser", "replaceUser", "patchUser", "deleteUser").forEach { operation ->
      assertThat(rootPath + methods.getValue(operation).getAnnotation(Path::class.java).value)
        .isEqualTo("/scim/v2/Users/{id}")
    }
    assertThat(methods.getValue("listUsers").isAnnotationPresent(GET::class.java)).isTrue
    assertThat(methods.getValue("createUser").isAnnotationPresent(POST::class.java)).isTrue
    assertThat(methods.getValue("getUser").isAnnotationPresent(GET::class.java)).isTrue
    assertThat(methods.getValue("replaceUser").isAnnotationPresent(PUT::class.java)).isTrue
    assertThat(methods.getValue("patchUser").isAnnotationPresent(PATCH::class.java)).isTrue
    assertThat(methods.getValue("deleteUser").isAnnotationPresent(DELETE::class.java)).isTrue
  }

  @Test
  fun `Groups API exposes the complete protocol surface at the SCIM base path`() {
    val apiClass = Class.forName("io.airbyte.api.scim.generated.apis.ScimGroupsApi")
    val rootPath = apiClass.getAnnotation(Path::class.java).value
    val methods = apiClass.declaredMethods.associateBy { it.name }

    assertThat(methods.keys)
      .containsExactlyInAnyOrder("listGroups", "createGroup", "getGroup", "replaceGroup", "patchGroup", "deleteGroup")
    assertThat(rootPath + methods.getValue("listGroups").getAnnotation(Path::class.java).value).isEqualTo("/scim/v2/Groups")
    assertThat(rootPath + methods.getValue("createGroup").getAnnotation(Path::class.java).value).isEqualTo("/scim/v2/Groups")
    listOf("getGroup", "replaceGroup", "patchGroup", "deleteGroup").forEach { operation ->
      assertThat(rootPath + methods.getValue(operation).getAnnotation(Path::class.java).value)
        .isEqualTo("/scim/v2/Groups/{id}")
    }
    assertThat(methods.getValue("listGroups").isAnnotationPresent(GET::class.java)).isTrue
    assertThat(methods.getValue("createGroup").isAnnotationPresent(POST::class.java)).isTrue
    assertThat(methods.getValue("getGroup").isAnnotationPresent(GET::class.java)).isTrue
    assertThat(methods.getValue("replaceGroup").isAnnotationPresent(PUT::class.java)).isTrue
    assertThat(methods.getValue("patchGroup").isAnnotationPresent(PATCH::class.java)).isTrue
    assertThat(methods.getValue("deleteGroup").isAnnotationPresent(DELETE::class.java)).isTrue
  }

  @Test
  fun `Users reads and mutations expose the normative query parameters`() {
    val methods = ScimUsersApi::class.java.declaredMethods.associateBy { it.name }

    assertThat(queryParameters(methods.getValue("listUsers")))
      .containsExactlyInAnyOrder("filter", "startIndex", "count", "attributes", "excludedAttributes")
    listOf("createUser", "getUser", "replaceUser", "patchUser").forEach { operation ->
      assertThat(queryParameters(methods.getValue(operation)))
        .describedAs(operation)
        .containsExactlyInAnyOrder("attributes", "excludedAttributes")
    }
    assertThat(queryParameters(methods.getValue("deleteUser"))).isEmpty()
  }

  @Test
  fun `Groups reads and mutations expose the normative query parameters`() {
    val methods = Class.forName("io.airbyte.api.scim.generated.apis.ScimGroupsApi").declaredMethods.associateBy { it.name }

    assertThat(queryParameters(methods.getValue("listGroups")))
      .containsExactlyInAnyOrder("filter", "startIndex", "count", "attributes", "excludedAttributes")
    listOf("createGroup", "getGroup", "replaceGroup", "patchGroup").forEach { operation ->
      assertThat(queryParameters(methods.getValue(operation)))
        .describedAs(operation)
        .containsExactlyInAnyOrder("attributes", "excludedAttributes")
    }
    assertThat(queryParameters(methods.getValue("deleteGroup"))).isEmpty()
  }

  @Test
  fun `User mutation payloads are bound as HTTP bodies`() {
    listOf("createUser", "replaceUser", "patchUser").forEach { operation ->
      val method = ScimUsersApi::class.java.declaredMethods.single { it.name == operation }
      assertThat(method.parameterAnnotations.flatten().map { it.annotationClass.java })
        .describedAs(operation)
        .contains(Body::class.java)
    }
  }

  @Test
  fun `Group mutation payloads are bound as HTTP bodies`() {
    val methods = Class.forName("io.airbyte.api.scim.generated.apis.ScimGroupsApi").declaredMethods.associateBy { it.name }

    listOf("createGroup", "replaceGroup", "patchGroup").forEach { operation ->
      assertThat(
        methods
          .getValue(operation)
          .parameterAnnotations
          .flatten()
          .map { it.annotationClass.java },
      ).describedAs(operation)
        .contains(Body::class.java)
    }
  }

  @Test
  fun `Group PATCH advertises bodyless success and projected representation responses`() {
    val responses = scimContract["paths"]["/Groups/{id}"]["patch"]["responses"]

    assertThat(responses.has("200")).isTrue
    assertThat(responses["200"]["content"]["application/scim+json"]["schema"]["\$ref"].asText())
      .isEqualTo("#/components/schemas/ScimGroup")
    assertThat(responses.has("204")).isTrue
    assertThat(responses["204"].has("content")).isFalse
  }

  @Test
  fun `every discovery read exposes both SCIM projection parameters`() {
    ScimDiscoveryApi::class.java.declaredMethods.forEach { method ->
      val queryParameters =
        method.parameterAnnotations
          .flatten()
          .filterIsInstance<QueryParam>()
          .map(QueryParam::value)

      assertThat(queryParameters)
        .describedAs(method.name)
        .containsExactlyInAnyOrder("attributes", "excludedAttributes")
    }
  }

  @Test
  fun `every discovery read documents SCIM invalid projection errors`() {
    val badRequestResponse = scimContract["components"]["responses"]["ScimBadRequest"]

    assertThat(badRequestResponse).isNotNull
    assertThat(badRequestResponse["content"]["application/scim+json"]["schema"]["\$ref"].asText())
      .isEqualTo("#/components/schemas/ScimError")
    listOf(
      "/ServiceProviderConfig",
      "/ResourceTypes",
      "/ResourceTypes/{id}",
      "/Schemas",
      "/Schemas/{schemaUri}",
    ).forEach { path ->
      assertThat(scimContract["paths"][path]["get"]["responses"]["400"]["\$ref"].asText())
        .describedAs("%s 400 response", path)
        .isEqualTo("#/components/responses/ScimBadRequest")
    }
  }

  @Test
  fun `SCIM API operations expose typed mutable HTTP responses`() {
    val expectedBodyTypes =
      mapOf(
        "getServiceProviderConfig" to ScimServiceProviderConfig::class.java,
        "listResourceTypes" to ScimResourceTypeListResponse::class.java,
        "getResourceType" to ScimResourceType::class.java,
        "listSchemas" to ScimSchemaListResponse::class.java,
        "getSchema" to ScimSchema::class.java,
      )

    ScimDiscoveryApi::class.java.declaredMethods.forEach { method ->
      assertThat(method.returnType).isEqualTo(MutableHttpResponse::class.java)
      assertThat(method.genericReturnType.typeName)
        .isEqualTo("${MutableHttpResponse::class.java.name}<${expectedBodyTypes.getValue(method.name).name}>")
    }
  }

  @Test
  fun `membership references serialize the SCIM reference field exactly`() {
    val reference =
      ScimGroupReference(
        value = "group-mapping-id",
        dollarRef = URI.create("https://example.com/scim/v2/Groups/group-mapping-id"),
        display = "Engineering",
      )

    assertThat(objectMapper.valueToTree<JsonNode>(reference))
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf(
            "value" to "group-mapping-id",
            "\$ref" to "https://example.com/scim/v2/Groups/group-mapping-id",
            "display" to "Engineering",
          ),
        ),
      )
  }

  @Test
  fun `mutation membership references accept the required client-supplied value`() {
    val reference = ScimUserReferenceRequest(value = "user-mapping-id")

    assertThat(objectMapper.valueToTree<JsonNode>(reference))
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf("value" to "user-mapping-id"),
        ),
      )
  }

  @Test
  fun `projected email elements may omit unrequested values`() {
    val projectedEmail = ScimEmail(type = "work")

    assertThat(objectMapper.valueToTree<JsonNode>(projectedEmail))
      .isEqualTo(objectMapper.valueToTree<JsonNode>(mapOf("type" to "work")))
  }

  @Test
  fun `mutation complex elements use request schemas with required values`() {
    assertThat(schema("ScimUserRequest")["properties"]["emails"]["items"]["\$ref"].asText())
      .isEqualTo("#/components/schemas/ScimEmailRequest")
    assertThat(schema("ScimGroupRequest")["properties"]["members"]["items"]["\$ref"].asText())
      .isEqualTo("#/components/schemas/ScimUserReferenceRequest")

    listOf("ScimEmailRequest", "ScimUserReferenceRequest").forEach { schemaName ->
      assertThat(schema(schemaName)["required"].map(JsonNode::asText)).containsExactly("value")
    }
    assertThat(schema("ScimEmail").path("required").map(JsonNode::asText)).doesNotContain("value")
    listOf("ScimUserReference", "ScimGroupReference").forEach { schemaName ->
      assertThat(schema(schemaName)["required"].map(JsonNode::asText)).containsExactly("value")
    }
  }

  @Test
  fun `User mutation schema requires a non-empty selectable email collection`() {
    val userRequestSchema = schema("ScimUserRequest")
    val emails = userRequestSchema["properties"]["emails"]

    assertThat(userRequestSchema["required"].map(JsonNode::asText)).contains("emails")
    assertThat(emails["minItems"].asInt()).isEqualTo(1)
    assertThat(emails["items"]["\$ref"].asText()).isEqualTo("#/components/schemas/ScimEmailRequest")
    assertThat(emails["description"].asText()).isEqualTo(USER_EMAILS_DESCRIPTION)
  }

  @Test
  fun `membership reference enrichment fields are read only`() {
    listOf("ScimUserReference", "ScimGroupReference").forEach { schemaName ->
      val properties = schema(schemaName)["properties"]

      assertThat(properties["\$ref"].path("readOnly").asBoolean()).isTrue
      assertThat(properties["display"].path("readOnly").asBoolean()).isTrue
      assertThat(properties["value"].has("readOnly")).isFalse
    }
  }

  @Test
  fun `minimal User request omits server-issued and defaulted fields`() {
    val emails = listOf(mapOf("value" to "alice@example.com", "type" to "work", "primary" to true))
    val request =
      objectMapper.readValue(
        """{"schemas":["$USER_SCHEMA"],"userName":"alice@example.com","emails":[{"value":"alice@example.com","type":"work","primary":true}]}""",
        ScimUserRequest::class.java,
      )

    assertThat(objectMapper.valueToTree<JsonNode>(request))
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf("schemas" to listOf(USER_SCHEMA), "userName" to "alice@example.com", "emails" to emails),
        ),
      )
  }

  @Test
  fun `minimal Group request omits server-issued and optional fields`() {
    val request =
      objectMapper.readValue(
        """{"schemas":["$GROUP_SCHEMA"],"displayName":"Engineering"}""",
        ScimGroupRequest::class.java,
      )

    assertThat(objectMapper.valueToTree<JsonNode>(request))
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf("schemas" to listOf(GROUP_SCHEMA), "displayName" to "Engineering"),
        ),
      )
  }

  @Test
  fun `Group request advertises the displayName storage boundary`() {
    assertThat(schema("ScimGroupRequest")["properties"]["displayName"].path("maxLength").asInt()).isEqualTo(256)
  }

  @Test
  fun `User and Group mutation contracts accept explicit null externalId`() {
    val userRequestJson =
      """{"schemas":["$USER_SCHEMA"],"userName":"alice@example.com","emails":[{"value":"alice@example.com","primary":true}],"externalId":null}"""
    val groupRequestJson = """{"schemas":["$GROUP_SCHEMA"],"displayName":"Engineering","externalId":null}"""

    val userRequest = runtimeObjectMapper.readValue(userRequestJson, ScimUserRequest::class.java)
    val groupRequest = runtimeObjectMapper.readValue(groupRequestJson, ScimGroupRequest::class.java)

    assertThat(userRequest.body["externalId"].isNull).isTrue
    assertThat(groupRequest.body["externalId"].isNull).isTrue
    assertThat(schema("ScimUserRequest")["properties"]["externalId"].path("nullable").asBoolean()).isTrue
    assertThat(schema("ScimGroupRequest")["properties"]["externalId"].path("nullable").asBoolean()).isTrue
    assertThat(schema("ScimUser")["properties"]["externalId"].has("nullable")).isFalse
    assertThat(schema("ScimGroup")["properties"]["externalId"].has("nullable")).isFalse
  }

  @Test
  fun `User request preserves ignored unsupported and nested fields until validation`() {
    val requestJson =
      """
      {
        "schemas": ["$USER_SCHEMA"],
        "userName": "alice@example.com",
        "emails": [{"value": "alice@example.com", "type": "work"}],
        "id": "client-supplied-id",
        "meta": {"resourceType": "User"},
        "groups": [],
        "password": null,
        "urn:example:params:scim:schemas:extension:custom:2.0:User": {"department": "Engineering"},
        "name": {"givenName": "Alice", "unsupportedNested": "preserve-me"}
      }
      """.trimIndent()

    val request = runtimeObjectMapper.readValue(requestJson, ScimUserRequest::class.java)

    assertThat(request.body).isEqualTo(runtimeObjectMapper.readTree(requestJson))
  }

  @Test
  fun `Group request preserves ignored unsupported and nested fields until validation`() {
    val requestJson =
      """
      {
        "schemas": ["$GROUP_SCHEMA"],
        "displayName": "Engineering",
        "id": "client-supplied-id",
        "meta": {"resourceType": "Group"},
        "urn:example:params:scim:schemas:extension:custom:2.0:Group": {"description": "Unsupported"},
        "members": [{"value": "user-mapping-id", "unsupportedNested": "preserve-me"}]
      }
      """.trimIndent()

    val request = runtimeObjectMapper.readValue(requestJson, ScimGroupRequest::class.java)

    assertThat(request.body).isEqualTo(runtimeObjectMapper.readTree(requestJson))
  }

  @Test
  fun `Group request accepts a member containing only value`() {
    val requestJson =
      """
      {
        "schemas": ["$GROUP_SCHEMA"],
        "displayName": "Engineering",
        "members": [{"value": "user-mapping-id"}]
      }
      """.trimIndent()

    val request = objectMapper.readValue(requestJson, ScimGroupRequest::class.java)

    assertThat(objectMapper.valueToTree<JsonNode>(request))
      .isEqualTo(objectMapper.readTree(requestJson))
  }

  @Test
  fun `Patch operation model preserves path and arbitrary JSON value`() {
    val operationJson =
      """
      {
        "op": "replace",
        "path": "name.givenName",
        "value": {
          "formatted": "Alice Example",
          "parts": ["Alice", null, 42, true]
        }
      }
      """.trimIndent()

    val operation = objectMapper.readValue(operationJson, ScimPatchOperation::class.java)

    assertThat(objectMapper.valueToTree<JsonNode>(operation))
      .isEqualTo(objectMapper.readTree(operationJson))
  }

  @Test
  fun `Patch operation contract accepts lowercase and Entra mixed-case names`() {
    listOf("add", "Add", "replace", "Replace", "remove", "Remove").forEach { operationName ->
      val operation = ScimPatchOperation(op = operationName)

      assertThat(objectMapper.valueToTree<JsonNode>(operation)["op"].asText()).isEqualTo(operationName)
    }

    val operationSchema = schema("ScimPatchOperation")["properties"]["op"]
    assertThat(operationSchema.has("enum")).isFalse
    assertThat(operationSchema["pattern"].asText()).isEqualTo(PATCH_OPERATION_PATTERN)
  }

  @Test
  fun `PatchOp request preserves unsupported and nested fields until validation`() {
    val requestJson =
      """
      {
        "schemas": ["$PATCH_OP_SCHEMA"],
        "Operations": [
          {
            "op": "add",
            "path": "members",
            "value": [
              {
                "value": "user-mapping-id",
                "unsupportedNested": "preserve-me"
              }
            ],
            "unsupportedOperation": true
          }
        ],
        "unsupportedRoot": {"preserve": "me"}
      }
      """.trimIndent()

    val request = runtimeObjectMapper.readValue(requestJson, ScimPatchRequest::class.java)

    assertThat(request.body).isEqualTo(runtimeObjectMapper.readTree(requestJson))
  }

  @Test
  fun `shared User and Group models serialize exact SCIM fields`() {
    val created = OffsetDateTime.parse("2026-07-15T10:00:00Z")
    val lastModified = OffsetDateTime.parse("2026-07-15T11:00:00Z")
    val user =
      ScimUser(
        schemas = listOf(USER_SCHEMA),
        id = "user-mapping-id",
        userName = "alice@example.com",
        active = true,
        meta =
          ScimMeta(
            resourceType = "User",
            created = created,
            lastModified = lastModified,
            location = URI.create("https://example.com/scim/v2/Users/user-mapping-id"),
          ),
        emails = emptyList(),
        groups = emptyList(),
      )
    val group =
      ScimGroup(
        schemas = listOf(GROUP_SCHEMA),
        id = "group-mapping-id",
        displayName = "Engineering",
        members =
          listOf(
            ScimUserReference(
              value = "user-mapping-id",
              dollarRef = URI.create("https://example.com/scim/v2/Users/user-mapping-id"),
              display = "alice@example.com",
            ),
          ),
        meta =
          ScimMeta(
            resourceType = "Group",
            created = created,
            lastModified = lastModified,
            location = URI.create("https://example.com/scim/v2/Groups/group-mapping-id"),
          ),
      )

    val userJson = objectMapper.valueToTree<JsonNode>(user)
    val groupJson = objectMapper.valueToTree<JsonNode>(group)

    assertThat(userJson["schemas"].map(JsonNode::asText)).containsExactly(USER_SCHEMA)
    assertThat(userJson.has("externalId")).isFalse
    assertThat(userJson["emails"].isArray).isTrue
    assertThat(userJson["emails"]).hasSize(0)
    assertThat(userJson["groups"].isArray).isTrue
    assertThat(userJson["groups"]).hasSize(0)
    assertThat(userJson["meta"]["created"].asText()).isEqualTo("2026-07-15T10:00:00Z")
    assertThat(userJson["meta"].has("version")).isFalse

    assertThat(groupJson["schemas"].map(JsonNode::asText)).containsExactly(GROUP_SCHEMA)
    assertThat(groupJson.has("externalId")).isFalse
    assertThat(groupJson["members"][0])
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf(
            "value" to "user-mapping-id",
            "\$ref" to "https://example.com/scim/v2/Users/user-mapping-id",
            "display" to "alice@example.com",
          ),
        ),
      )
  }

  @Test
  fun `projected response models require only invariant representation fields`() {
    val projectedResponses =
      listOf(
        ScimUser(schemas = listOf(USER_SCHEMA), id = "user-mapping-id") to
          mapOf("schemas" to listOf(USER_SCHEMA), "id" to "user-mapping-id"),
        ScimGroup(schemas = listOf(GROUP_SCHEMA), id = "group-mapping-id") to
          mapOf("schemas" to listOf(GROUP_SCHEMA), "id" to "group-mapping-id"),
        ScimServiceProviderConfig(schemas = listOf(SERVICE_PROVIDER_CONFIG_SCHEMA)) to
          mapOf("schemas" to listOf(SERVICE_PROVIDER_CONFIG_SCHEMA)),
        ScimResourceType(schemas = listOf(RESOURCE_TYPE_SCHEMA), id = "User") to
          mapOf("schemas" to listOf(RESOURCE_TYPE_SCHEMA), "id" to "User"),
        ScimSchema(schemas = listOf(SCHEMA_SCHEMA), id = USER_SCHEMA) to
          mapOf("schemas" to listOf(SCHEMA_SCHEMA), "id" to USER_SCHEMA),
      )

    projectedResponses.forEach { (response, expected) ->
      assertThat(objectMapper.valueToTree<JsonNode>(response))
        .isEqualTo(objectMapper.valueToTree<JsonNode>(expected))
    }
  }

  @Test
  fun `projected complex response attributes may contain only selected subattributes`() {
    val projectedAttributes =
      listOf(
        ScimMeta(location = URI.create("/scim/v2/Users/user-mapping-id")) to
          mapOf("location" to "/scim/v2/Users/user-mapping-id"),
        ScimDiscoveryMeta(location = "/scim/v2/Schemas/$USER_SCHEMA") to
          mapOf("location" to "/scim/v2/Schemas/$USER_SCHEMA"),
        ScimBulkConfiguration(maxOperations = 100) to mapOf("maxOperations" to 100),
        ScimFilterConfiguration(maxResults = 100) to mapOf("maxResults" to 100),
        ScimAuthenticationScheme(type = "oauthbearertoken") to mapOf("type" to "oauthbearertoken"),
        ScimSchemaAttribute(name = "displayName") to mapOf("name" to "displayName"),
      )

    projectedAttributes.forEach { (attribute, expected) ->
      assertThat(objectMapper.valueToTree<JsonNode>(attribute))
        .isEqualTo(objectMapper.valueToTree<JsonNode>(expected))
    }
  }

  @Test
  fun `ResourceType id and relative Schema location survive projection`() {
    assertThat(schema("ScimResourceType")["required"].map(JsonNode::asText)).containsExactly("schemas", "id")
    assertThat(schema("ScimDiscoveryMeta")["properties"]["location"]["format"].asText()).isEqualTo("uri-reference")
    assertThat(schema("ScimMeta")["properties"]["location"]["format"].asText()).isEqualTo("uri")
  }

  @Test
  fun `schemas arrays require exactly the supported model URN`() {
    val expectedSchemas =
      linkedMapOf(
        "ScimServiceProviderConfig" to SERVICE_PROVIDER_CONFIG_SCHEMA,
        "ScimResourceType" to RESOURCE_TYPE_SCHEMA,
        "ScimResourceTypeListResponse" to LIST_RESPONSE_SCHEMA,
        "ScimSchema" to SCHEMA_SCHEMA,
        "ScimSchemaListResponse" to LIST_RESPONSE_SCHEMA,
        "ScimUserRequest" to USER_SCHEMA,
        "ScimUser" to USER_SCHEMA,
        "ScimGroupRequest" to GROUP_SCHEMA,
        "ScimGroup" to GROUP_SCHEMA,
        "ScimPatchRequest" to PATCH_OP_SCHEMA,
        "ScimUserListResponse" to LIST_RESPONSE_SCHEMA,
        "ScimGroupListResponse" to LIST_RESPONSE_SCHEMA,
        "ScimError" to ERROR_SCHEMA,
      )

    expectedSchemas.forEach { (schemaName, expectedUrn) ->
      val schemasProperty = schema(schemaName)["properties"]["schemas"]

      assertThat(schemasProperty.path("minItems").asInt()).describedAs("%s.schemas minItems", schemaName).isEqualTo(1)
      assertThat(schemasProperty.path("maxItems").asInt()).describedAs("%s.schemas maxItems", schemaName).isEqualTo(1)
      assertThat(schemasProperty.path("items").path("enum").map(JsonNode::asText))
        .describedAs("%s.schemas supported URN", schemaName)
        .containsExactly(expectedUrn)
    }
  }

  @Test
  fun `typed list and Error models preserve SCIM casing and omit null optionals`() {
    val meta =
      ScimMeta(
        resourceType = "User",
        created = OffsetDateTime.parse("2026-07-15T10:00:00Z"),
        lastModified = OffsetDateTime.parse("2026-07-15T10:00:00Z"),
        location = URI.create("https://example.com/scim/v2/Users/user-mapping-id"),
      )
    val user =
      ScimUser(
        schemas = listOf(USER_SCHEMA),
        id = "user-mapping-id",
        userName = "alice@example.com",
        active = true,
        meta = meta,
        groups = emptyList(),
      )
    val group =
      ScimGroup(
        schemas = listOf(GROUP_SCHEMA),
        id = "group-mapping-id",
        displayName = "Engineering",
        members = emptyList(),
        meta = meta.copy(resourceType = "Group", location = URI.create("https://example.com/scim/v2/Groups/group-mapping-id")),
      )
    val userList =
      ScimUserListResponse(
        schemas = listOf(LIST_RESPONSE_SCHEMA),
        totalResults = 1,
        resources = listOf(user),
        startIndex = 1,
        itemsPerPage = 1,
      )
    val groupList =
      ScimGroupListResponse(
        schemas = listOf(LIST_RESPONSE_SCHEMA),
        totalResults = 1,
        resources = listOf(group),
        startIndex = 1,
        itemsPerPage = 1,
      )
    val error = ScimError(schemas = listOf(ERROR_SCHEMA), status = "404")

    listOf(userList, groupList).forEach { response ->
      val json = objectMapper.valueToTree<JsonNode>(response)
      assertThat(json.has("Resources")).isTrue
      assertThat(json.has("resources")).isFalse
      assertThat(json["Resources"]).hasSize(1)
    }
    assertThat(objectMapper.valueToTree<JsonNode>(error))
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf(
            "schemas" to listOf(ERROR_SCHEMA),
            "status" to "404",
          ),
        ),
      )
  }

  @Test
  fun `Schema discovery metadata contains only its resource type and canonical location`() {
    val meta =
      ScimDiscoveryMeta(
        resourceType = "Schema",
        location = "/scim/v2/Schemas/$USER_SCHEMA",
      )

    assertThat(objectMapper.valueToTree<JsonNode>(meta))
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf(
            "resourceType" to "Schema",
            "location" to "/scim/v2/Schemas/$USER_SCHEMA",
          ),
        ),
      )
  }

  private companion object {
    const val USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"
    const val GROUP_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:Group"
    const val LIST_RESPONSE_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
    const val ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
    const val PATCH_OP_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
    const val SERVICE_PROVIDER_CONFIG_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"
    const val RESOURCE_TYPE_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:ResourceType"
    const val SCHEMA_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:Schema"
    const val PATCH_OPERATION_PATTERN = "^([Aa][Dd][Dd]|[Rr][Ee][Pp][Ll][Aa][Cc][Ee]|[Rr][Ee][Mm][Oo][Vv][Ee])$"
    const val USER_EMAILS_DESCRIPTION =
      "Email addresses for the User. At least one entry must have primary set to true or type set to work."
  }

  private fun schema(name: String): JsonNode = scimContract["components"]["schemas"][name]

  private fun queryParameters(method: java.lang.reflect.Method): List<String> =
    method.parameterAnnotations
      .flatten()
      .filterIsInstance<QueryParam>()
      .map(QueryParam::value)
}
