/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.execute

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion
import io.airbyte.api.generated.DestinationExecutionApi
import io.airbyte.api.generated.SourceApi
import io.airbyte.api.generated.SourceExecutionApi
import io.airbyte.api.model.generated.DestinationExecuteRequest
import io.airbyte.api.model.generated.SourceExecuteRequest
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.yaml.snakeyaml.LoaderOptions
import java.util.UUID
import io.airbyte.api.server.generated.apis.DestinationExecutionApi as KotlinDestinationExecutionApi
import io.airbyte.api.server.generated.apis.SourceApi as KotlinSourceApi
import io.airbyte.api.server.generated.apis.SourceExecutionApi as KotlinSourceExecutionApi
import io.airbyte.api.server.generated.models.DestinationExecuteRequest as KotlinDestinationExecuteRequest
import io.airbyte.api.server.generated.models.SourceExecuteRequest as KotlinSourceExecuteRequest

class SourceExecuteGeneratedContractTest {
  private val objectMapper = ObjectMapper().findAndRegisterModules()
  private val yamlMapper =
    ObjectMapper(
      YAMLFactory
        .builder()
        .loaderOptions(LoaderOptions().apply { codePointLimit = 20_000_000 })
        .build(),
    )
  private val contract =
    requireNotNull(javaClass.classLoader.getResourceAsStream("config.yaml")).use { input ->
      yamlMapper.readTree(input)
    }

  @Test
  fun `Java and Kotlin source APIs expose execute with a UUID path and opaque response`() {
    for ((api, sourceIdType) in listOf(
      SourceExecutionApi::class.java to UUID::class.java,
      KotlinSourceExecutionApi::class.java to String::class.java,
    )) {
      val method = api.declaredMethods.single { it.name == "executeSource" }
      assertEquals(
        "/api/v1/sources/{sourceId}/execute",
        api.getAnnotation(Path::class.java).value + method.getAnnotation(Path::class.java)?.value.orEmpty(),
      )
      assertTrue(method.isAnnotationPresent(POST::class.java))
      assertEquals(JsonNode::class.java, method.returnType)
      val sourceId = method.parameters.single { it.getAnnotation(PathParam::class.java)?.value == "sourceId" }
      assertEquals(sourceIdType, sourceId.type)
    }
    for (api in listOf(SourceApi::class.java, KotlinSourceApi::class.java)) {
      assertFalse(api.declaredMethods.any { it.name == "executeSource" })
    }
    val operation = contract.path("paths").path("/v1/sources/{sourceId}/execute").path("post")
    assertEquals("executeSource", operation.path("operationId").asText())
    assertEquals(listOf("source_execution"), operation.path("tags").map(JsonNode::asText))
  }

  @Test
  fun `destination execution exposes independent request and response schemas`() {
    for ((api, request) in listOf(
      DestinationExecutionApi::class.java to DestinationExecuteRequest::class.java,
      KotlinDestinationExecutionApi::class.java to KotlinDestinationExecuteRequest::class.java,
    )) {
      val method = api.declaredMethods.single { it.name == "executeDestination" }
      assertTrue(method.parameters.any { it.type == request })
      assertEquals(JsonNode::class.java, method.returnType)
      assertEquals(Any::class.java, request.superclass)
    }
    val operation = contract.path("paths").path("/v1/destinations/{destinationId}/execute").path("post")
    assertEquals(
      "#/components/schemas/DestinationExecuteRequest",
      operation.at("/requestBody/content/application~1json/schema/\$ref").asText(),
    )
    assertEquals(
      "#/components/schemas/DestinationExecuteResponse",
      operation.at("/responses/200/content/application~1json/schema/\$ref").asText(),
    )
    val schemas = contract.path("components").path("schemas")
    for (name in listOf("Request", "Response", "Params", "Data", "Meta")) {
      assertTrue(schemas.has("DestinationExecute$name"))
      assertFalse(schemas.path("DestinationExecute$name").toString().contains("SourceExecute"))
    }
  }

  @Test
  fun `generated server requests preserve opaque parameters and snake case controls`() {
    val json =
      """
      {"entity":"contacts","action":"api_search","params":{"filter":[null,{"active":false}],"cursor":null,"id":9223372036854775000},
      "select_fields":["name"],"exclude_fields":[],"skip_truncation":false,"intent":"lookup"}
      """.trimIndent()
    for (model in listOf(
      SourceExecuteRequest::class.java,
      KotlinSourceExecuteRequest::class.java,
      DestinationExecuteRequest::class.java,
      KotlinDestinationExecuteRequest::class.java,
    )) {
      val request = objectMapper.readValue(json, model)
      assertEquals(JsonNode::class.java, model.getMethod("getParams").returnType)
      assertEquals(objectMapper.readTree(json), objectMapper.readTree(objectMapper.writeValueAsString(request)))
      val omitted = objectMapper.readValue("""{"entity":"contacts","action":"list"}""", model)
      assertEquals(true, model.getMethod("getSkipTruncation").invoke(omitted))
    }
  }

  @Test
  fun `execute is excluded from every public API artifact`() {
    for (name in listOf("api.yaml", "api_sdk.yaml", "api_documentation_sources.yaml", "api_terraform.yaml")) {
      val spec =
        requireNotNull(javaClass.classLoader.getResourceAsStream(name)).use { input ->
          yamlMapper.readTree(input)
        }
      assertFalse(spec.path("paths").has("/sources/{sourceId}/execute"), name)
      assertFalse(spec.path("paths").has("/destinations/{destinationId}/execute"), name)
      assertFalse(spec.path("paths").has("/v1/destinations/{destinationId}/execute"), name)
      assertFalse(spec.path("paths").has("/workspaces/{workspaceId}/skills/docs"), name)
      assertFalse(spec.path("components").path("schemas").has("SkillDocsResponse"), name)
      assertFalse(spec.path("paths").has("/v1/sources/{sourceId}/execute"), name)
      for (schema in listOf(
        "SourceExecuteRequest",
        "SourceExecuteParams",
        "SourceExecuteMeta",
        "SourceExecuteResponse",
        "SourceExecuteData",
        "SourceExecuteProblem",
        "DestinationExecuteRequest",
        "DestinationExecuteResponse",
        "DestinationExecuteParams",
        "DestinationExecuteData",
        "DestinationExecuteMeta",
      )) {
        assertFalse(spec.path("components").path("schemas").has(schema), "$name $schema")
      }
    }
  }

  @Test
  fun `config execute schemas validate nullable options and opaque JSON semantics`() {
    val components = contract.path("components").deepCopy<ObjectNode>()
    val schemas = components.path("schemas")
    for (request in listOf("SourceExecuteRequest", "DestinationExecuteRequest")) {
      val properties = schemas.path(request).path("properties")
      for (field in listOf("select_fields", "exclude_fields", "intent")) {
        val property = properties.path(field) as ObjectNode
        assertTrue(property.path("nullable").asBoolean(), field)
        // OpenAPI 3.0 nullable is expressed as a type union for the JSON Schema validator.
        val type = property.path("type").asText()
        property.putArray("type").add(type).add("null")
        property.remove("nullable")
      }
    }
    val factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
    for (model in listOf("SourceExecuteRequest", "SourceExecuteResponse", "DestinationExecuteRequest", "DestinationExecuteResponse")) {
      val root = objectMapper.createObjectNode()
      root.put("\$ref", "#/components/schemas/$model")
      root.set<JsonNode>("components", components)
      val schema = factory.getSchema(root)
      val valid =
        if (model.endsWith("Request")) {
          listOf(
            """{"entity":"contacts","action":"list"}""",
            """{"entity":"contacts","action":"api_search","params":{"cursor":null},"select_fields":null,"exclude_fields":null,"intent":null}""",
            """{"entity":"issues","action":"list","params":{},"select_fields":[],"exclude_fields":[],"skip_truncation":false,"intent":""}""",
          )
        } else {
          listOf("null", "{}", "[]", "false", "0", "\"\"", "[null,{\"id\":9223372036854775000}]").flatMap { data ->
            listOf("{\"data\":$data}", "{\"data\":$data,\"meta\":{}}", "{\"data\":$data,\"meta\":{\"cursor\":null}}")
          }
        }
      for (json in valid) assertTrue(schema.validate(objectMapper.readTree(json)).isEmpty(), "$model $json")
      val invalid =
        if (model.endsWith("Request")) {
          listOf(
            "{}",
            """{"entity":"","action":"list"}""",
            """{"entity":"contacts","action":"list","params":null}""",
            """{"entity":"contacts","action":"list","skip_truncation":null}""",
            """{"entity":"contacts","action":"list","skip_truncation":"true"}""",
            """{"entity":"contacts","action":"list","select_fields":[null]}""",
            """{"entity":"contacts","action":"list","extra":true}""",
          )
        } else {
          listOf("{}", "null", "[]", """{"data":null,"meta":null}""", """{"data":[],"meta":[]}""")
        }
      for (json in invalid) assertFalse(schema.validate(objectMapper.readTree(json)).isEmpty(), "$model $json")
    }
  }
}
