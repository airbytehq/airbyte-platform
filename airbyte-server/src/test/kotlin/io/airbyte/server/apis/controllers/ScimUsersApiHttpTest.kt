/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimUserConflictException
import io.airbyte.domain.models.scim.ScimUserFilterClause
import io.airbyte.domain.models.scim.ScimUserGroup
import io.airbyte.domain.models.scim.ScimUserListPage
import io.airbyte.domain.models.scim.ScimUserNotFoundException
import io.airbyte.domain.models.scim.ScimUserRead
import io.airbyte.domain.models.scim.ScimUserWrite
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimAuthenticationService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimUserLifecycleService
import io.airbyte.server.scim.SCIM_MEDIA_TYPE
import io.airbyte.server.scim.SCIM_USER_SCHEMA
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
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

private const val SCIM_USERS_SPEC = "ScimUsersApiHttpTest"

@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SCIM_USERS_SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
class ScimUsersApiHttpTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var objectMapper: ObjectMapper

  @Inject
  lateinit var authenticationService: ScimAuthenticationService

  @Inject
  lateinit var lifecycleService: ScimUserLifecycleService

  private val rawToken = "airbyte_scim_${"a".repeat(64)}"
  private val configurationId = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val organizationId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val mappingId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val context = ScimAuthenticationContext(configurationId, OrganizationId(organizationId), "b".repeat(64))

  @BeforeEach
  fun setUp() {
    clearMocks(authenticationService, lifecycleService)
    every { authenticationService.authenticate(rawToken) } returns context
    every { lifecycleService.enrichGroups(configurationId, organizationId, any()) } answers { thirdArg<List<ScimUserRead>>() }
  }

  @Test
  fun `POST Users binds raw SCIM JSON and returns protocol headers and media type`() {
    val input = slot<ScimUserWrite>()
    val requestBody =
      objectMapper.readTree(
        """
        {"schemas":["$SCIM_USER_SCHEMA"],"id":"ignored","userName":"user@example.com","externalId":"entra-1","emails":[{"value":"user@example.com","type":"work","display":"discard me"}],"meta":{"resourceType":"User","created":"2026-07-17T00:00:00Z","lastModified":"2026-07-17T01:00:00Z","location":"https://example.com/scim/v2/Users/ignored"},"groups":[{"value":"ignored","${'$'}ref":"https://example.com/scim/v2/Groups/ignored","display":"Ignored group"}],"password":"discarded"}
        """.trimIndent(),
      )
    every { lifecycleService.create(configurationId, organizationId, capture(input)) } returns storedUser()

    val response =
      client.toBlocking().exchange(
        HttpRequest
          .POST("/scim/v2/Users?excludedAttributes=meta", requestBody.toString())
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE)
          .header("X-Forwarded-Proto", "https")
          .header("X-Forwarded-Host", "airbyte.example.com"),
        JsonNode::class.java,
      )

    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.contentType.orElseThrow().name).isEqualTo("application/scim+json")
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Users/$mappingId")
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Users/$mappingId")
    assertThat(
      response
        .body()
        .path("schemas")
        .single()
        .asText(),
    ).isEqualTo(SCIM_USER_SCHEMA)
    assertThat(response.body().path("id").asText()).isEqualTo(mappingId.toString())
    assertThat(response.body().path("userName").asText()).isEqualTo("user@example.com")
    assertThat(
      input.captured.attributes
        .path("emails")
        .single()
        .has("display"),
    ).isFalse()
    assertThat(
      response
        .body()
        .path("emails")
        .single()
        .has("display"),
    ).isFalse()
    assertThat(response.body().has("meta")).isFalse()
  }

  @Test
  fun `unprojected POST Users returns complete meta consistent with canonical headers`() {
    val stored = storedUser()
    val requestBody =
      objectMapper.readTree(
        """
        {"schemas":["$SCIM_USER_SCHEMA"],"userName":"user@example.com","emails":[{"value":"user@example.com","type":"work"}]}
        """.trimIndent(),
      )
    every { lifecycleService.create(configurationId, organizationId, any()) } returns stored

    val response =
      client.toBlocking().exchange(
        HttpRequest
          .POST("/scim/v2/Users", requestBody.toString())
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE)
          .header("X-Forwarded-Proto", "https")
          .header("X-Forwarded-Host", "airbyte.example.com"),
        JsonNode::class.java,
      )

    val expectedLocation = "https://airbyte.example.com/scim/v2/Users/$mappingId"
    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(expectedLocation)
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(expectedLocation)
    assertCompleteMeta(response.body(), stored, expectedLocation)
    assertThat(
      response
        .body()
        .path("meta")
        .path("location")
        .asText(),
    ).isEqualTo(response.header(HttpHeaders.LOCATION))
    assertThat(
      response
        .body()
        .path("meta")
        .path("location")
        .asText(),
    ).isEqualTo(response.header(HttpHeaders.CONTENT_LOCATION))
  }

  @Test
  fun `GET and filtered list Users return canonical resources`() {
    val stored = storedUser()
    val filters = slot<List<ScimUserFilterClause>>()
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns stored
    every { lifecycleService.list(configurationId, organizationId, capture(filters), 0, 1) } returns ScimUserListPage(listOf(stored), 1)

    val getResponse =
      client.toBlocking().exchange(
        HttpRequest
          .GET<Any>("/scim/v2/Users/$mappingId")
          .bearerAuth(rawToken)
          .header("X-Forwarded-Proto", "https")
          .header("X-Forwarded-Host", "airbyte.example.com"),
        JsonNode::class.java,
      )
    val listResponse =
      client.toBlocking().exchange(
        HttpRequest
          .GET<Any>("/scim/v2/Users?filter=userName%20eq%20%22user%40example.com%22&startIndex=1&count=1")
          .bearerAuth(rawToken)
          .header("X-Forwarded-Proto", "https")
          .header("X-Forwarded-Host", "airbyte.example.com"),
        JsonNode::class.java,
      )

    assertThat(getResponse.status).isEqualTo(HttpStatus.OK)
    assertThat(getResponse.body().path("id").asText()).isEqualTo(mappingId.toString())
    assertThat(getResponse.body().path("userName").asText()).isEqualTo("user@example.com")
    assertCompleteMeta(getResponse.body(), stored, "https://airbyte.example.com/scim/v2/Users/$mappingId")
    assertThat(filters.captured.single().value).isEqualTo("user@example.com")
    assertThat(listResponse.body().path("totalResults").asInt()).isEqualTo(1)
    val listedUser = listResponse.body().path("Resources").single()
    assertThat(listedUser.path("id").asText()).isEqualTo(mappingId.toString())
    assertCompleteMeta(listedUser, stored, "https://airbyte.example.com/scim/v2/Users/$mappingId")
  }

  @Test
  fun `GET User filter preserves repeated multi valued and scalar clauses`() {
    val filters = slot<List<ScimUserFilterClause>>()
    every { lifecycleService.list(configurationId, organizationId, capture(filters), 0, 100) } returns ScimUserListPage(emptyList(), 0)

    val response =
      getUsers(
        "?filter=emails.value%20eq%20%22primary%40example.com%22%20and%20emails.value%20eq%20%22alias%40example.com%22%20and%20emails%5Btype%20eq%20%22work%22%5D.value%20eq%20%22primary%40example.com%22%20and%20emails%5Btype%20eq%20%22work%22%5D.value%20eq%20%22backup%40example.com%22%20and%20userName%20eq%20%22first%40example.com%22%20and%20userName%20eq%20%22second%40example.com%22",
      )

    assertPage(response, totalResults = 0, startIndex = 1, itemsPerPage = 0)
    assertThat(filters.captured.map { it.value }).containsExactly(
      "primary@example.com",
      "alias@example.com",
      "primary@example.com",
      "backup@example.com",
      "first@example.com",
      "second@example.com",
    )
  }

  @Test
  fun `GET Users projects whole meta and individual meta subattributes`() {
    val stored = storedUser()
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns stored

    val wholeIncluded = getUser("?attributes=meta")
    val locationIncluded = getUser("?attributes=meta.location")
    val lastModifiedExcluded = getUser("?excludedAttributes=meta.lastModified")
    val wholeExcluded = getUser("?excludedAttributes=meta")

    assertThat(wholeIncluded.fieldNames().asSequence().toList()).containsExactly("schemas", "id", "meta")
    assertThat(
      wholeIncluded
        .path("meta")
        .fieldNames()
        .asSequence()
        .toList(),
    ).containsExactly("resourceType", "created", "lastModified", "location")
    assertThat(locationIncluded.fieldNames().asSequence().toList()).containsExactly("schemas", "id", "meta")
    assertThat(
      locationIncluded
        .path("meta")
        .fieldNames()
        .asSequence()
        .toList(),
    ).containsExactly("location")
    assertThat(
      lastModifiedExcluded
        .path("meta")
        .fieldNames()
        .asSequence()
        .toList(),
    ).containsExactly("resourceType", "created", "location")
    assertThat(wholeExcluded.has("meta")).isFalse()
  }

  @Test
  fun `GET Users returns empty enriched and projected groups`() {
    val groupId = UUID.fromString("55555555-5555-5555-5555-555555555555")
    val empty = storedUser()
    val grouped = storedUser().copy(groups = listOf(ScimUserGroup(groupId, "Engineering")))
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returnsMany listOf(empty, grouped, grouped, grouped)

    val emptyResponse = getUser()
    val groupedResponse = getUser()
    val projectedResponse = getUser("?attributes=groups.value")
    val excludedResponse = getUser("?excludedAttributes=groups")

    assertThat(emptyResponse.path("groups").isArray).isTrue()
    assertThat(emptyResponse.path("groups")).isEmpty()
    assertThat(
      groupedResponse
        .path("groups")
        .single()
        .path("value")
        .asText(),
    ).isEqualTo(groupId.toString())
    assertThat(
      groupedResponse
        .path("groups")
        .single()
        .path("display")
        .asText(),
    ).isEqualTo("Engineering")
    assertThat(
      groupedResponse
        .path("groups")
        .single()
        .path("${'$'}ref")
        .asText(),
    ).endsWith("/scim/v2/Groups/$groupId")
    assertThat(
      projectedResponse
        .path("groups")
        .single()
        .fieldNames()
        .asSequence()
        .toList(),
    ).containsExactly("value")
    assertThat(excludedResponse.has("groups")).isFalse()
  }

  @Test
  fun `list Users exposes empty zero-count negative-count and beyond-end pages over HTTP`() {
    every { lifecycleService.list(configurationId, organizationId, any(), 0, 100) } returns ScimUserListPage(emptyList(), 0)
    every { lifecycleService.list(configurationId, organizationId, any(), 0, 0) } returns ScimUserListPage(emptyList(), 1)
    every { lifecycleService.list(configurationId, organizationId, any(), 98, 10) } returns ScimUserListPage(emptyList(), 1)

    val noMatches = getUsers("?filter=userName%20eq%20%22missing%40example.com%22")
    val zeroCount = getUsers("?count=0")
    val negativeCount = getUsers("?count=-5")
    val beyondEnd = getUsers("?startIndex=99&count=10")

    assertPage(noMatches, totalResults = 0, startIndex = 1, itemsPerPage = 0)
    assertPage(zeroCount, totalResults = 1, startIndex = 1, itemsPerPage = 0)
    assertPage(negativeCount, totalResults = 1, startIndex = 1, itemsPerPage = 0)
    assertPage(beyondEnd, totalResults = 1, startIndex = 99, itemsPerPage = 0)
    verify(exactly = 0) { lifecycleService.enrichGroups(any(), any(), any()) }
  }

  @Test
  fun `PUT uses the path id and clears or defaults omitted attributes`() {
    val input = slot<ScimUserWrite>()
    val requestBody =
      objectMapper.readTree(
        """
        {"schemas":["$SCIM_USER_SCHEMA"],"id":"55555555-5555-5555-5555-555555555555","userName":"updated@example.com","emails":[{"value":"updated@example.com","type":"work","display":"discard me"}],"meta":{"resourceType":"User"},"groups":[],"password":"discarded"}
        """.trimIndent(),
      )
    every { lifecycleService.replace(configurationId, organizationId, mappingId, capture(input)) } returns
      storedUser().copy(
        externalId = null,
        userName = "updated@example.com",
        primaryEmail = "updated@example.com",
        attributes = objectMapper.createObjectNode().set("emails", inputEmailArray("updated@example.com")),
      )

    val response =
      client.toBlocking().exchange(
        HttpRequest
          .PUT("/scim/v2/Users/$mappingId", requestBody.toString())
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        JsonNode::class.java,
      )

    assertThat(response.status).isEqualTo(HttpStatus.OK)
    assertThat(input.captured.externalId).isNull()
    assertThat(input.captured.active).isTrue()
    assertThat(
      input.captured.attributes
        .fieldNames()
        .asSequence()
        .toList(),
    ).containsExactly("emails")
    assertThat(
      input.captured.attributes
        .path("emails")
        .single()
        .has("display"),
    ).isFalse()
    verify { lifecycleService.replace(configurationId, organizationId, mappingId, any()) }
  }

  @Test
  fun `PATCH and DELETE Users execute their HTTP lifecycle operations`() {
    val stored = storedUser()
    val transitions = slot<List<Boolean>>()
    val patchBody =
      objectMapper.readTree(
        """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"add","path":"groups","value":{"value":"ignored","${'$'}ref":"https://example.com/scim/v2/Groups/ignored","display":"Ignored group"}},{"op":"replace","path":"meta","value":{"resourceType":"User"}},{"op":"replace","path":"password","value":"discarded"},{"op":"replace","path":"active","value":false}]}""",
      )
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns stored
    every { lifecycleService.patch(configurationId, organizationId, mappingId, any(), capture(transitions)) } returns stored.copy(active = false)
    every { lifecycleService.delete(configurationId, organizationId, mappingId) } returns Unit

    val patchResponse =
      client.toBlocking().exchange(
        HttpRequest
          .PATCH("/scim/v2/Users/$mappingId", patchBody.toString())
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        JsonNode::class.java,
      )
    val deleteResponse =
      client.toBlocking().exchange<Any, Any>(
        HttpRequest.DELETE<Any>("/scim/v2/Users/$mappingId").bearerAuth(rawToken),
      )

    assertThat(patchResponse.status).isEqualTo(HttpStatus.OK)
    assertThat(patchResponse.body().path("active").asBoolean()).isFalse()
    assertThat(transitions.captured).containsExactly(false)
    assertThat(deleteResponse.status).isEqualTo(HttpStatus.NO_CONTENT)
  }

  @Test
  fun `PATCH direct and pathless email add and replace discard textual display before lifecycle writes`() {
    val inputs = mutableListOf<ScimUserWrite>()
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns storedUser()
    every { lifecycleService.patch(configurationId, organizationId, mappingId, capture(inputs), any()) } returns storedUser()
    val operations =
      listOf(
        """{"op":"add","path":"emails","value":{"value":"direct-add@example.com","type":"home","display":"Direct add"}}""",
        """{"op":"replace","path":"emails","value":[{"value":"direct-replace@example.com","type":"work","display":"Direct replace"}]}""",
        """{"op":"add","value":{"emails":[{"value":"pathless-add@example.com","type":"home","display":"Pathless add"}]}}""",
        """{"op":"replace","value":{"emails":[{"value":"pathless-replace@example.com","type":"work","display":"Pathless replace"}]}}""",
      )

    operations.forEach { operation ->
      val response =
        client.toBlocking().exchange(
          HttpRequest
            .PATCH(
              "/scim/v2/Users/$mappingId",
              """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[$operation]}""",
            ).bearerAuth(rawToken)
            .contentType(SCIM_MEDIA_TYPE),
          JsonNode::class.java,
        )

      assertThat(response.status).isEqualTo(HttpStatus.OK)
    }

    assertThat(inputs).hasSize(4)
    inputs.forEach { input ->
      assertThat(input.attributes.path("emails")).allSatisfy { email -> assertThat(email.has("display")).isFalse() }
    }
  }

  @Test
  fun `PATCH filtered email replace without a matching work entry returns noTarget without lifecycle writes`() {
    val stored =
      storedUser().copy(
        attributes =
          objectMapper.createObjectNode().also {
            it
              .putArray("emails")
              .addObject()
              .put("value", "user@example.com")
              .put("type", "home")
              .put("primary", true)
          },
      )
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns stored
    every { lifecycleService.patch(configurationId, organizationId, mappingId, any(), any()) } returns stored

    assertHttpError(
      HttpRequest
        .PATCH(
          "/scim/v2/Users/$mappingId",
          """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"emails[type eq \"work\"].value","value":"new@example.com"}]}""",
        ).bearerAuth(rawToken)
        .contentType(SCIM_MEDIA_TYPE),
      HttpStatus.BAD_REQUEST,
      "noTarget",
    )

    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH direct and pathless email add and replace reject non-string display without lifecycle writes`() {
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns storedUser()
    val operations =
      listOf(
        """{"op":"add","path":"emails","value":{"value":"direct-add@example.com","type":"home","display":false}}""",
        """{"op":"replace","path":"emails","value":[{"value":"direct-replace@example.com","type":"work","display":false}]}""",
        """{"op":"add","value":{"emails":[{"value":"pathless-add@example.com","type":"home","display":false}]}}""",
        """{"op":"replace","value":{"emails":[{"value":"pathless-replace@example.com","type":"work","display":false}]}}""",
      )

    operations.forEach { operation ->
      assertHttpError(
        HttpRequest
          .PATCH(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[$operation]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpStatus.BAD_REQUEST,
        "invalidValue",
      )
    }

    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH rejects duplicate case-insensitive pathless attributes and direct or pathless subattributes without lifecycle writes`() {
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns storedUser()
    val operations =
      listOf(
        """{"op":"add","value":{"userName":"first@example.com","USERNAME":"second@example.com"}}""",
        """{"op":"replace","value":{"displayName":"First","DISPLAYNAME":"Second"}}""",
        """{"op":"add","path":"name","value":{"givenName":"First","GIVENNAME":"Second"}}""",
        """{"op":"replace","value":{"name":{"familyName":"First","FAMILYNAME":"Second"}}}""",
        """{"op":"add","path":"emails","value":{"value":"first@example.com","VALUE":"second@example.com","type":"home"}}""",
        """{"op":"replace","value":{"emails":[{"value":"first@example.com","VALUE":"second@example.com","type":"work"}]}}""",
      )

    operations.forEach { operation ->
      assertHttpError(
        HttpRequest
          .PATCH(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[$operation]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpStatus.BAD_REQUEST,
        "invalidValue",
      )
    }

    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `GET and DELETE unknown mapping ids return SCIM 404`() {
    every { lifecycleService.get(configurationId, organizationId, mappingId) } throws ScimUserNotFoundException()
    every { lifecycleService.delete(configurationId, organizationId, mappingId) } throws ScimUserNotFoundException()

    assertHttpError(
      HttpRequest.GET<Any>("/scim/v2/Users/$mappingId").bearerAuth(rawToken),
      HttpStatus.NOT_FOUND,
      null,
    )
    assertHttpError(
      HttpRequest.DELETE<Any>("/scim/v2/Users/$mappingId").bearerAuth(rawToken),
      HttpStatus.NOT_FOUND,
      null,
    )
  }

  @Test
  fun `invalid filter PUT conflict and invalid PATCH return canonical SCIM errors`() {
    val requestBody =
      objectMapper.readTree(
        """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"user@example.com","emails":[{"value":"user@example.com","type":"work"}]}""",
      )
    val invalidPatch =
      objectMapper.readTree(
        """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"unsupported","value":"x"}]}""",
      )
    every { lifecycleService.replace(configurationId, organizationId, mappingId, any()) } throws ScimUserConflictException()
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns storedUser()

    assertHttpError(
      HttpRequest.GET<Any>("/scim/v2/Users?filter=unsupported%20eq%20%22x%22").bearerAuth(rawToken),
      HttpStatus.BAD_REQUEST,
      "invalidFilter",
    )
    assertHttpError(
      HttpRequest
        .PUT("/scim/v2/Users/$mappingId", requestBody.toString())
        .bearerAuth(rawToken)
        .contentType(SCIM_MEDIA_TYPE),
      HttpStatus.CONFLICT,
      "uniqueness",
    )
    assertHttpError(
      HttpRequest
        .PATCH("/scim/v2/Users/$mappingId", invalidPatch.toString())
        .bearerAuth(rawToken)
        .contentType(SCIM_MEDIA_TYPE),
      HttpStatus.BAD_REQUEST,
      "invalidPath",
    )
    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `invalid profile URLs and email values return invalidValue before writes`() {
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns storedUser()
    val oversizedEmail = "${"l".repeat(64)}@${"d".repeat(63)}.${"e".repeat(63)}.${"f".repeat(62)}"
    val invalidRequests =
      listOf(
        HttpRequest
          .POST(
            "/scim/v2/Users",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"post@example.com","profileUrl":"https://example .com/post","emails":[{"value":"post@example.com","type":"work"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PUT(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"put@example.com","profileUrl":"https://example .com/put","emails":[{"value":"put@example.com","type":"work"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PATCH(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"profileUrl","value":"https://example .com/patch"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .POST(
            "/scim/v2/Users",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"post@example.com","emails":[{"value":"post@example.com","type":"work"},{"value":" POST@EXAMPLE.COM ","type":"home"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PUT(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"put@example.com","emails":[{"value":"put@example.com","type":"work"},{"value":" PUT@EXAMPLE.COM ","type":"home"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PATCH(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"emails","value":[{"value":"patch@example.com","type":"work"},{"value":" PATCH@EXAMPLE.COM ","type":"home"}]}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .POST(
            "/scim/v2/Users",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"post@example.com","profileUrl":"urn:example:user:post","emails":[{"value":"post@example.com","type":"work"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PUT(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"put@example.com","profileUrl":"file:///profiles/put","emails":[{"value":"put@example.com","type":"work"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PATCH(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"profileUrl","value":"https:profile"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .POST(
            "/scim/v2/Users",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"post@example.com","emails":[{"value":"valid-post@example.com","type":"work","primary":true},{"value":"alice@.example.com","type":"home"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PUT(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"put@example.com","emails":[{"value":"valid-put@example.com","type":"work","primary":true},{"value":".alice@example.com","type":"home"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PATCH(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"emails","value":[{"value":"valid-patch@example.com","type":"work","primary":true},{"value":"alice@-example.com","type":"home"}]}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .POST(
            "/scim/v2/Users",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"oversized@example.com","emails":[{"value":"$oversizedEmail","type":"work"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
      )

    invalidRequests.forEach { request ->
      assertHttpError(request, HttpStatus.BAD_REQUEST, "invalidValue")
    }
    verify(exactly = 0) { lifecycleService.create(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.replace(any(), any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `POST and PUT reject non-string email display values before writes`() {
    listOf(
      """{"unvalidated":true}""",
      "[]",
      "1",
      "true",
      "null",
    ).forEach { display ->
      assertHttpError(
        HttpRequest
          .POST(
            "/scim/v2/Users",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"post@example.com","emails":[{"value":"post@example.com","type":"work","display":$display}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpStatus.BAD_REQUEST,
        "invalidValue",
      )
      assertHttpError(
        HttpRequest
          .PUT(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"put@example.com","emails":[{"value":"put@example.com","type":"work","display":$display}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpStatus.BAD_REQUEST,
        "invalidValue",
      )
    }

    verify(exactly = 0) { lifecycleService.create(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.replace(any(), any(), any(), any()) }
  }

  @Test
  fun `POST PUT and PATCH reject wrong-shaped ignored attributes without lifecycle writes`() {
    every { lifecycleService.get(configurationId, organizationId, mappingId) } returns storedUser()
    val invalidRequests =
      listOf(
        HttpRequest
          .POST(
            "/scim/v2/Users",
            """{"schemas":["$SCIM_USER_SCHEMA"],"id":{},"userName":"post@example.com","emails":[{"value":"post@example.com","type":"work"}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PUT(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["$SCIM_USER_SCHEMA"],"userName":"put@example.com","emails":[{"value":"put@example.com","type":"work"}],"meta":[]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpRequest
          .PATCH(
            "/scim/v2/Users/$mappingId",
            """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"groups","value":false}]}""",
          ).bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
      )

    invalidRequests.forEach { request ->
      assertHttpError(request, HttpStatus.BAD_REQUEST, "invalidValue")
    }

    verify(exactly = 0) { lifecycleService.create(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.replace(any(), any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `null array scalar and wrong-shaped User envelopes return canonical SCIM errors without writes`() {
    listOf("null", "[]", "\"not-an-object\"").forEach { body ->
      assertHttpError(
        HttpRequest
          .POST("/scim/v2/Users", body)
          .bearerAuth(rawToken)
          .contentType(SCIM_MEDIA_TYPE),
        HttpStatus.BAD_REQUEST,
        "invalidSyntax",
      )
    }
    assertHttpError(
      HttpRequest
        .POST("/scim/v2/Users", "{}")
        .bearerAuth(rawToken)
        .contentType(SCIM_MEDIA_TYPE),
      HttpStatus.BAD_REQUEST,
      "invalidValue",
    )
    verify(exactly = 0) { lifecycleService.create(any(), any(), any()) }
  }

  private fun assertHttpError(
    request: HttpRequest<*>,
    status: HttpStatus,
    scimType: String?,
  ) {
    val error = assertThrows<HttpClientResponseException> { client.toBlocking().exchange(request, JsonNode::class.java) }
    assertThat(error.status).isEqualTo(status)
    val body = error.response.getBody(JsonNode::class.java).orElseThrow()
    assertThat(body.path("schemas").single().asText()).isEqualTo("urn:ietf:params:scim:api:messages:2.0:Error")
    if (scimType == null) {
      assertThat(body.has("scimType")).isFalse()
    } else {
      assertThat(body.path("scimType").asText()).isEqualTo(scimType)
    }
  }

  private fun inputEmailArray(email: String) =
    objectMapper
      .createArrayNode()
      .addObject()
      .put("value", email)
      .put("type", "work")

  private fun getUser(query: String = ""): JsonNode =
    client
      .toBlocking()
      .exchange(
        HttpRequest.GET<Any>("/scim/v2/Users/$mappingId$query").bearerAuth(rawToken),
        JsonNode::class.java,
      ).body()

  private fun getUsers(query: String): JsonNode =
    client
      .toBlocking()
      .exchange(
        HttpRequest.GET<Any>("/scim/v2/Users$query").bearerAuth(rawToken),
        JsonNode::class.java,
      ).body()

  private fun assertPage(
    response: JsonNode,
    totalResults: Int,
    startIndex: Int,
    itemsPerPage: Int,
  ) {
    assertThat(response.path("totalResults").asInt()).isEqualTo(totalResults)
    assertThat(response.path("startIndex").asInt()).isEqualTo(startIndex)
    assertThat(response.path("itemsPerPage").asInt()).isEqualTo(itemsPerPage)
    assertThat(response.path("Resources")).isEmpty()
  }

  private fun assertCompleteMeta(
    resource: JsonNode,
    stored: ScimUserRead,
    expectedLocation: String,
  ) {
    assertThat(resource.path("meta").path("resourceType").asText()).isEqualTo("User")
    assertThat(OffsetDateTime.parse(resource.path("meta").path("created").asText())).isEqualTo(stored.createdAt)
    assertThat(OffsetDateTime.parse(resource.path("meta").path("lastModified").asText())).isEqualTo(stored.updatedAt)
    assertThat(resource.path("meta").path("location").asText()).isEqualTo(expectedLocation)
  }

  private fun storedUser(): ScimUserRead =
    ScimUserRead(
      id = mappingId,
      configurationId = configurationId,
      organizationId = organizationId,
      userId = UUID.fromString("44444444-4444-4444-4444-444444444444"),
      externalId = "entra-1",
      userName = "user@example.com",
      primaryEmail = "user@example.com",
      active = true,
      attributes =
        objectMapper.createObjectNode().also {
          it
            .putArray("emails")
            .addObject()
            .put("value", "user@example.com")
            .put("type", "work")
        },
      createdAt = OffsetDateTime.parse("2026-07-17T00:00:00Z"),
      updatedAt = OffsetDateTime.parse("2026-07-17T00:00:00Z"),
    )
}

@Requires(property = "spec.name", value = SCIM_USERS_SPEC)
@Factory
class ScimUsersTestBeans {
  @Singleton
  @Primary
  fun authenticationService(): ScimAuthenticationService = mockk()

  @Singleton
  @Primary
  fun lifecycleService(): ScimUserLifecycleService = mockk()

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
