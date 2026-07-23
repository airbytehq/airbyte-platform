/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimGroupFilterClause
import io.airbyte.domain.models.scim.ScimGroupListPage
import io.airbyte.domain.models.scim.ScimGroupMember
import io.airbyte.domain.models.scim.ScimGroupRead
import io.airbyte.domain.models.scim.ScimGroupWrite
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimAuthenticationService
import io.airbyte.domain.services.scim.ScimGroupLifecycleService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.server.scim.SCIM_GROUP_SCHEMA
import io.airbyte.server.scim.SCIM_LIST_RESPONSE_SCHEMA
import io.airbyte.server.scim.SCIM_MEDIA_TYPE
import io.airbyte.server.scim.SCIM_PATCH_OP_SCHEMA
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

private const val SCIM_GROUPS_SPEC = "ScimGroupsApiHttpTest"

@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SCIM_GROUPS_SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
class ScimGroupsApiHttpTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var objectMapper: ObjectMapper

  @Inject
  lateinit var authenticationService: ScimAuthenticationService

  @Inject
  lateinit var lifecycleService: ScimGroupLifecycleService

  private val rawToken = "airbyte_scim_${"a".repeat(64)}"
  private val configurationId = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val organizationId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val mappingId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val groupId = UUID.fromString("44444444-4444-4444-4444-444444444444")
  private val userMappingId = UUID.fromString("55555555-5555-5555-5555-555555555555")
  private val userId = UUID.fromString("66666666-6666-6666-6666-666666666666")
  private val context = ScimAuthenticationContext(configurationId, OrganizationId(organizationId), "b".repeat(64))

  @BeforeEach
  fun setUp() {
    clearMocks(authenticationService, lifecycleService)
    every { authenticationService.authenticate(rawToken) } returns context
  }

  @Test
  fun `POST Groups ignores arbitrary read-only inputs and returns the canonical server representation`() {
    val input = slot<ScimGroupWrite>()
    val requestBody =
      objectMapper.readTree(
        """
        {
          "schemas":["$SCIM_GROUP_SCHEMA"],
          "id":null,
          "externalId":"entra-group-1",
          "displayName":"Engineering",
          "members":[{"value":"$userMappingId","${'$'}ref":{"ignored":true},"display":["Ignored"]}],
          "meta":{"version":17,"ignored":{"nested":true}}
        }
        """.trimIndent(),
      )
    every { lifecycleService.create(configurationId, organizationId, capture(input)) } returns storedGroup()

    val response =
      client.toBlocking().exchange(
        HttpRequest
          .POST("/scim/v2/Groups", requestBody.toString())
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE)
          .header("X-Forwarded-Proto", "https")
          .header("X-Forwarded-Host", "airbyte.example.com"),
        JsonNode::class.java,
      )

    val expectedLocation = "https://airbyte.example.com/scim/v2/Groups/$mappingId"
    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.contentType.orElseThrow().name).isEqualTo("application/scim+json")
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(expectedLocation)
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(expectedLocation)
    assertThat(input.captured.displayName).isEqualTo("Engineering")
    assertThat(input.captured.externalId).isEqualTo("entra-group-1")
    assertThat(input.captured.memberIds).containsExactly(userMappingId)
    assertCompleteGroup(response.body(), expectedLocation)
  }

  @Test
  fun `POST and PUT Groups reject additional and substituted schema URNs before lifecycle writes`() {
    val extensionSchema = "urn:ietf:params:scim:schemas:extension:airbyte:2.0:Group"
    val invalidRequests =
      listOf(
        HttpRequest
          .POST(
            "/scim/v2/Groups",
            """{"schemas":["$SCIM_GROUP_SCHEMA","$extensionSchema"],"displayName":"Post additional"}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .POST(
            "/scim/v2/Groups",
            """{"schemas":["$extensionSchema"],"displayName":"Post substituted"}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PUT(
            "/scim/v2/Groups/$mappingId",
            """{"schemas":["$SCIM_GROUP_SCHEMA","$extensionSchema"],"displayName":"Put additional"}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PUT(
            "/scim/v2/Groups/$mappingId",
            """{"schemas":["$extensionSchema"],"displayName":"Put substituted"}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
      )

    invalidRequests.forEach { request ->
      assertHttpError(request, HttpStatus.BAD_REQUEST, "invalidValue")
    }

    verify(exactly = 0) { lifecycleService.create(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.replace(any(), any(), any(), any()) }
  }

  @Test
  fun `POST PUT and PATCH Groups enforce the displayName storage boundary before lifecycle writes`() {
    val displayNameAtLimit = "x".repeat(256)
    val displayNameOverLimit = "x".repeat(257)
    val current = storedGroup()
    val atLimit = current.copy(displayName = displayNameAtLimit)
    every { lifecycleService.create(configurationId, organizationId, any()) } returns atLimit
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns current
    every { lifecycleService.areActiveUsers(configurationId, organizationId, setOf(userMappingId)) } returns true
    every { lifecycleService.replace(configurationId, organizationId, mappingId, any()) } returns atLimit

    val postResponse =
      client.toBlocking().exchange(
        HttpRequest
          .POST("/scim/v2/Groups", groupRequestBody(displayNameAtLimit))
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        JsonNode::class.java,
      )
    val putResponse =
      client.toBlocking().exchange(
        HttpRequest
          .PUT("/scim/v2/Groups/$mappingId", groupRequestBody(displayNameAtLimit))
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        JsonNode::class.java,
      )
    val validPatchBody =
      objectMapper.writeValueAsString(
        mapOf(
          "schemas" to listOf(SCIM_PATCH_OP_SCHEMA),
          "Operations" to listOf(mapOf("op" to "replace", "path" to "displayName", "value" to displayNameAtLimit)),
        ),
      )
    val patchResponse: HttpResponse<Any> =
      client.toBlocking().exchange(
        HttpRequest
          .PATCH("/scim/v2/Groups/$mappingId", validPatchBody)
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
      )

    assertThat(postResponse.status).isEqualTo(HttpStatus.CREATED)
    assertThat(postResponse.body().path("displayName").asText()).isEqualTo(displayNameAtLimit)
    assertThat(putResponse.status).isEqualTo(HttpStatus.OK)
    assertThat(putResponse.body().path("displayName").asText()).isEqualTo(displayNameAtLimit)
    assertThat(patchResponse.status).isEqualTo(HttpStatus.NO_CONTENT)
    verify(exactly = 1) { lifecycleService.create(configurationId, organizationId, any()) }
    verify(exactly = 2) { lifecycleService.replace(configurationId, organizationId, mappingId, any()) }
    clearMocks(lifecycleService, answers = false)

    val invalidPatchBody =
      objectMapper.writeValueAsString(
        mapOf(
          "schemas" to listOf(SCIM_PATCH_OP_SCHEMA),
          "Operations" to listOf(mapOf("op" to "replace", "path" to "displayName", "value" to displayNameOverLimit)),
        ),
      )
    listOf(
      HttpRequest
        .POST("/scim/v2/Groups", groupRequestBody(displayNameOverLimit))
        .bearerAuth(rawToken)
        .contentType(SCIM_MEDIA_TYPE),
      HttpRequest
        .PUT("/scim/v2/Groups/$mappingId", groupRequestBody(displayNameOverLimit))
        .bearerAuth(rawToken)
        .contentType(SCIM_MEDIA_TYPE),
      HttpRequest
        .PATCH("/scim/v2/Groups/$mappingId", invalidPatchBody)
        .bearerAuth(rawToken)
        .contentType(SCIM_MEDIA_TYPE),
    ).forEach { request ->
      assertHttpError(request, HttpStatus.BAD_REQUEST, "invalidValue")
    }

    verify(exactly = 0) { lifecycleService.create(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.replace(any(), any(), any(), any()) }
  }

  @Test
  fun `GET and filtered list Groups return canonical resources over HTTP`() {
    val stored = storedGroup()
    val filter = slot<ScimGroupFilterClause?>()
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns stored
    every { lifecycleService.list(configurationId, organizationId, captureNullable(filter), 0, 1) } returns
      ScimGroupListPage(listOf(stored), 1)

    val getResponse = getGroup()
    val listResponse =
      getGroups("?filter=displayName%20eq%20%22Engineering%22&startIndex=1&count=1")

    assertCompleteGroup(getResponse, "https://airbyte.example.com/scim/v2/Groups/$mappingId")
    assertThat(filter.captured?.value).isEqualTo("Engineering")
    assertThat(listResponse.path("schemas").single().asText()).isEqualTo(SCIM_LIST_RESPONSE_SCHEMA)
    assertThat(listResponse.path("totalResults").asInt()).isEqualTo(1)
    assertThat(listResponse.path("startIndex").asInt()).isEqualTo(1)
    assertThat(listResponse.path("itemsPerPage").asInt()).isEqualTo(1)
    assertCompleteGroup(listResponse.path("Resources").single(), "https://airbyte.example.com/scim/v2/Groups/$mappingId")
  }

  @Test
  fun `GET Groups projects whole and nested attributes over HTTP`() {
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns storedGroup()

    val displayOnly = getGroup("?attributes=displayName")
    val memberValue = getGroup("?attributes=members.value")
    val metadataWithoutTimestamp = getGroup("?excludedAttributes=meta.lastModified")

    assertThat(displayOnly.fieldNames().asSequence().toList()).containsExactly("schemas", "id", "displayName")
    assertThat(memberValue.fieldNames().asSequence().toList()).containsExactly("schemas", "id", "members")
    assertThat(
      memberValue
        .path("members")
        .single()
        .fieldNames()
        .asSequence()
        .toList(),
    ).containsExactly("value")
    assertThat(metadataWithoutTimestamp.path("meta").has("lastModified")).isFalse()
    assertThat(metadataWithoutTimestamp.path("meta").path("resourceType").asText()).isEqualTo("Group")
  }

  @Test
  fun `PUT PATCH and DELETE Groups expose their protocol status and body semantics over HTTP`() {
    val current = storedGroup()
    val updated = current.copy(displayName = "Platform", externalId = null)
    val replaceInputs = mutableListOf<ScimGroupWrite>()
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns current
    every { lifecycleService.areActiveUsers(configurationId, organizationId, setOf(userMappingId)) } returns true
    every { lifecycleService.replace(configurationId, organizationId, mappingId, capture(replaceInputs)) } returnsMany
      listOf(updated, updated, updated)
    every { lifecycleService.delete(configurationId, organizationId, mappingId) } returns Unit
    val putBody =
      """
      {
        "schemas":["$SCIM_GROUP_SCHEMA"],
        "id":["ignored"],
        "displayName":"Platform",
        "members":[{"value":"$userMappingId","${'$'}ref":false,"display":{"ignored":true}}],
        "meta":{"version":{"ignored":true}}
      }
      """.trimIndent()
    val patchBody =
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","value":"Platform"}]}"""

    val putResponse =
      client.toBlocking().exchange(
        HttpRequest
          .PUT("/scim/v2/Groups/$mappingId", putBody)
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        JsonNode::class.java,
      )
    val bodylessPatch: HttpResponse<Any> =
      client.toBlocking().exchange(
        HttpRequest
          .PATCH("/scim/v2/Groups/$mappingId", patchBody)
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
      )
    val projectedPatch =
      client.toBlocking().exchange(
        HttpRequest
          .PATCH("/scim/v2/Groups/$mappingId?attributes=displayName", patchBody)
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        JsonNode::class.java,
      )
    val deleteResponse =
      client.toBlocking().exchange<Any, Any>(
        HttpRequest.DELETE<Any>("/scim/v2/Groups/$mappingId").bearerAuth(rawToken),
      )

    assertThat(putResponse.status).isEqualTo(HttpStatus.OK)
    assertThat(putResponse.body().path("displayName").asText()).isEqualTo("Platform")
    assertThat(putResponse.body().path("id").asText()).isEqualTo(mappingId.toString())
    assertThat(
      putResponse
        .body()
        .path("members")
        .single()
        .path("${'$'}ref")
        .isTextual,
    ).isTrue()
    assertThat(
      putResponse
        .body()
        .path("members")
        .single()
        .path("display")
        .asText(),
    ).isEqualTo("Alice Example")
    assertThat(
      putResponse
        .body()
        .path("meta")
        .path("version")
        .isMissingNode,
    ).isTrue()
    assertThat(
      OffsetDateTime.parse(
        putResponse
          .body()
          .path("meta")
          .path("created")
          .asText(),
      ),
    ).isEqualTo(current.createdAt)
    assertThat(replaceInputs.first().externalId).isNull()
    assertThat(replaceInputs.first().memberIds).containsExactly(userMappingId)
    assertThat(bodylessPatch.status).isEqualTo(HttpStatus.NO_CONTENT)
    assertThat(projectedPatch.status).isEqualTo(HttpStatus.OK)
    assertThat(
      projectedPatch
        .body()
        .fieldNames()
        .asSequence()
        .toList(),
    ).containsExactly("schemas", "id", "displayName")
    assertThat(deleteResponse.status).isEqualTo(HttpStatus.NO_CONTENT)
    verify(exactly = 3) { lifecycleService.replace(configurationId, organizationId, mappingId, any()) }
  }

  private fun getGroup(query: String = ""): JsonNode =
    client
      .toBlocking()
      .exchange(
        HttpRequest
          .GET<Any>("/scim/v2/Groups/$mappingId$query")
          .bearerAuth(rawToken)
          .header("X-Forwarded-Proto", "https")
          .header("X-Forwarded-Host", "airbyte.example.com"),
        JsonNode::class.java,
      ).body()

  private fun assertHttpError(
    request: HttpRequest<*>,
    status: HttpStatus,
    scimType: String,
  ) {
    val error = assertThrows<HttpClientResponseException> { client.toBlocking().exchange(request, JsonNode::class.java) }
    assertThat(error.status).isEqualTo(status)
    val body = error.response.getBody(JsonNode::class.java).orElseThrow()
    assertThat(body.path("schemas").single().asText()).isEqualTo("urn:ietf:params:scim:api:messages:2.0:Error")
    assertThat(body.path("scimType").asText()).isEqualTo(scimType)
  }

  private fun getGroups(query: String): JsonNode =
    client
      .toBlocking()
      .exchange(
        HttpRequest
          .GET<Any>("/scim/v2/Groups$query")
          .bearerAuth(rawToken)
          .header("X-Forwarded-Proto", "https")
          .header("X-Forwarded-Host", "airbyte.example.com"),
        JsonNode::class.java,
      ).body()

  private fun groupRequestBody(displayName: String): String =
    objectMapper
      .createObjectNode()
      .apply {
        putArray("schemas").add(SCIM_GROUP_SCHEMA)
        put("displayName", displayName)
      }.toString()

  private fun assertCompleteGroup(
    resource: JsonNode,
    expectedLocation: String,
  ) {
    val stored = storedGroup()
    assertThat(resource.path("schemas").single().asText()).isEqualTo(SCIM_GROUP_SCHEMA)
    assertThat(resource.path("id").asText()).isEqualTo(mappingId.toString())
    assertThat(resource.path("externalId").asText()).isEqualTo("entra-group-1")
    assertThat(resource.path("displayName").asText()).isEqualTo("Engineering")
    val member = resource.path("members").single()
    assertThat(member.path("value").asText()).isEqualTo(userMappingId.toString())
    assertThat(member.path("${'$'}ref").asText()).isEqualTo(expectedLocation.replace("/Groups/$mappingId", "/Users/$userMappingId"))
    assertThat(member.path("display").asText()).isEqualTo("Alice Example")
    assertThat(resource.path("meta").path("resourceType").asText()).isEqualTo("Group")
    assertThat(OffsetDateTime.parse(resource.path("meta").path("created").asText())).isEqualTo(stored.createdAt)
    assertThat(OffsetDateTime.parse(resource.path("meta").path("lastModified").asText())).isEqualTo(stored.updatedAt)
    assertThat(resource.path("meta").path("location").asText()).isEqualTo(expectedLocation)
  }

  private fun storedGroup(): ScimGroupRead =
    ScimGroupRead(
      id = mappingId,
      configurationId = configurationId,
      organizationId = organizationId,
      groupId = groupId,
      externalId = "entra-group-1",
      displayName = "Engineering",
      createdAt = OffsetDateTime.parse("2026-07-17T00:00:00Z"),
      updatedAt = OffsetDateTime.parse("2026-07-17T01:00:00Z"),
      members = listOf(ScimGroupMember(userMappingId, userId, "Alice Example")),
    )
}

@Requires(property = "spec.name", value = SCIM_GROUPS_SPEC)
@Factory
class ScimGroupsTestBeans {
  @Singleton
  @Primary
  fun authenticationService(): ScimAuthenticationService = mockk()

  @Singleton
  @Primary
  fun lifecycleService(): ScimGroupLifecycleService = mockk()

  @Singleton
  @Primary
  fun mutationService(): ScimMutationService =
    object : ScimMutationService(mockk(), mockk(), mockk()) {
      override fun <T> execute(
        context: ScimAuthenticationContext,
        mutation: () -> T,
      ): T = mutation()
    }
}
