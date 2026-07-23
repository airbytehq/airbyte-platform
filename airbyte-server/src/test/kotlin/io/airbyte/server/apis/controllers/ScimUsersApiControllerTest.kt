/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.scim.ScimPatchRequest
import io.airbyte.api.scim.ScimUserRequest
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimUserConflictException
import io.airbyte.domain.models.scim.ScimUserListPage
import io.airbyte.domain.models.scim.ScimUserNotFoundException
import io.airbyte.domain.models.scim.ScimUserRead
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimUserLifecycleService
import io.airbyte.server.scim.SCIM_AUTHENTICATION_ATTRIBUTE
import io.airbyte.server.scim.SCIM_LIST_RESPONSE_SCHEMA
import io.airbyte.server.scim.SCIM_USER_SCHEMA
import io.airbyte.server.scim.ScimException
import io.airbyte.server.scim.ScimUserResourceService
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.context.ServerRequestContext
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class ScimUsersApiControllerTest {
  private val objectMapper = jacksonObjectMapper().findAndRegisterModules()
  private val lifecycleService = mockk<ScimUserLifecycleService>()
  private val mutationService = mockk<ScimMutationService>()
  private val resourceService = ScimUserResourceService(objectMapper)
  private val controller = ScimUsersApiController(lifecycleService, mutationService, resourceService)
  private val configurationId = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val organizationId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val context = ScimAuthenticationContext(configurationId, OrganizationId(organizationId), "a".repeat(64))

  @BeforeEach
  fun setUp() {
    every { mutationService.execute<Any>(context, any()) } answers { secondArg<() -> Any>().invoke() }
  }

  @Test
  fun `create parses mutates and returns canonical projected location headers`() {
    val stored = user(1)
    val body = userBody(active = true)
    every { lifecycleService.create(configurationId, organizationId, any()) } returns stored

    val response =
      withRequest(HttpRequest.POST("https://airbyte.example.com/scim/v2/Users", body)) {
        controller.createUser(ScimUserRequest(body), "userName", null)
      }

    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Users/${stored.id}")
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Users/${stored.id}")
    assertThat(response.body().userName).isEqualTo("user1@example.com")
    assertThat(response.body().meta).isNull()
    verify(exactly = 1) { mutationService.execute<Any>(context, any()) }
  }

  @Test
  fun `create preserves the configured base URL path in headers and meta location`() {
    val stored = user(1)
    val body = userBody(active = true)
    val pathPrefixedController =
      ScimUsersApiController(
        lifecycleService,
        mutationService,
        resourceService,
        "https://airbyte.example.com/airbyte/",
      )
    every { lifecycleService.create(configurationId, organizationId, any()) } returns stored

    val response =
      withRequest(HttpRequest.POST("https://internal.example.com/scim/v2/Users", body)) {
        pathPrefixedController.createUser(ScimUserRequest(body), null, null)
      }

    val expectedLocation = "https://airbyte.example.com/airbyte/scim/v2/Users/${stored.id}"
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(expectedLocation)
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo(expectedLocation)
    assertThat(
      response
        .body()
        .meta
        ?.location
        .toString(),
    ).isEqualTo(expectedLocation)
  }

  @Test
  fun `list maps supported filters before stable pagination and projection`() {
    val second = user(2)
    val filters = slot<List<io.airbyte.domain.models.scim.ScimUserFilterClause>>()
    every { lifecycleService.list(configurationId, organizationId, capture(filters), 1, 1) } returns ScimUserListPage(listOf(second), 2)
    every { lifecycleService.enrichGroups(configurationId, organizationId, listOf(second)) } returns listOf(second)

    val response =
      withRequest(HttpRequest.GET<Any>("https://airbyte.example.com/scim/v2/Users")) {
        controller.listUsers("userName eq \"user1@example.com\"", 2, 1, "userName", null)
      }

    assertThat(filters.captured).hasSize(1)
    assertThat(
      filters.captured
        .single()
        .attribute.name,
    ).isEqualTo("USER_NAME")
    assertThat(filters.captured.single().value).isEqualTo("user1@example.com")
    assertThat(response.body().schemas).containsExactly(SCIM_LIST_RESPONSE_SCHEMA)
    assertThat(response.body().totalResults).isEqualTo(2)
    assertThat(response.body().startIndex).isEqualTo(2)
    assertThat(response.body().itemsPerPage).isEqualTo(1)
    assertThat(
      response
        .body()
        .resources
        .single()
        .userName,
    ).isEqualTo("user2@example.com")
    assertThat(
      response
        .body()
        .resources
        .single()
        .active,
    ).isNull()
    verify(exactly = 1) { lifecycleService.enrichGroups(configurationId, organizationId, listOf(second)) }
  }

  @Test
  fun `zero count preserves totals without group enrichment`() {
    every { lifecycleService.list(configurationId, organizationId, emptyList(), 0, 0) } returns ScimUserListPage(emptyList(), 2)

    val response =
      withRequest(HttpRequest.GET<Any>("https://airbyte.example.com/scim/v2/Users?count=0")) {
        controller.listUsers(null, null, 0, null, null)
      }

    assertThat(response.body().totalResults).isEqualTo(2)
    assertThat(response.body().resources).isEmpty()
    assertThat(response.body().itemsPerPage).isZero()
    verify(exactly = 0) { lifecycleService.enrichGroups(any(), any(), any()) }
  }

  @Test
  fun `patch applies operations sequentially inside the authenticated mutation`() {
    val current = user(1)
    val updated = current.copy(active = true, updatedAt = current.updatedAt.plusMinutes(1))
    val transitions = slot<List<Boolean>>()
    every { lifecycleService.get(configurationId, organizationId, current.id) } returns current
    every { lifecycleService.patch(configurationId, organizationId, current.id, any(), capture(transitions)) } returns updated
    val patch =
      objectMapper.readTree(
        """
        {"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[
          {"op":"replace","path":"active","value":false},
          {"op":"replace","path":"active","value":true}
        ]}
        """.trimIndent(),
      ) as com.fasterxml.jackson.databind.node.ObjectNode

    val response =
      withRequest(HttpRequest.PATCH("https://airbyte.example.com/scim/v2/Users/${current.id}", patch)) {
        controller.patchUser(current.id, ScimPatchRequest(patch), null, null)
      }

    assertThat(transitions.captured).containsExactly(false, true)
    assertThat(response.body().active).isTrue()
    verify(exactly = 1) { mutationService.execute<Any>(context, any()) }
  }

  @Test
  fun `delete performs the lifecycle cleanup inside the authenticated mutation`() {
    val id = UUID.randomUUID()
    every { lifecycleService.delete(configurationId, organizationId, id) } returns Unit

    val response =
      withRequest(HttpRequest.DELETE<Any>("https://airbyte.example.com/scim/v2/Users/$id")) {
        controller.deleteUser(id)
      }

    assertThat(response.status).isEqualTo(HttpStatus.NO_CONTENT)
    verify(exactly = 1) { lifecycleService.delete(configurationId, organizationId, id) }
    verify(exactly = 1) { mutationService.execute<Any>(context, any()) }
  }

  @Test
  fun `domain identifier conflicts become SCIM uniqueness errors`() {
    val body = userBody(active = true)
    every { lifecycleService.create(configurationId, organizationId, any()) } throws ScimUserConflictException()

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.POST("https://airbyte.example.com/scim/v2/Users", body)) {
          controller.createUser(ScimUserRequest(body), null, null)
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.CONFLICT)
    assertThat(error.scimType).isEqualTo("uniqueness")
  }

  @Test
  fun `POST rejects an unknown projection before mutation`() {
    val body = userBody(active = true)
    every { lifecycleService.create(configurationId, organizationId, any()) } returns user(1)

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.POST("https://airbyte.example.com/scim/v2/Users", body)) {
          controller.createUser(ScimUserRequest(body), "unsupported", null)
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    verify(exactly = 0) { mutationService.execute<Any>(any(), any()) }
    verify(exactly = 0) { lifecycleService.create(any(), any(), any()) }
  }

  @Test
  fun `PUT rejects mutually exclusive projections before mutation`() {
    val body = userBody(active = true)
    val id = UUID.randomUUID()
    every { lifecycleService.replace(configurationId, organizationId, id, any()) } returns user(1)

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.PUT("https://airbyte.example.com/scim/v2/Users/$id", body)) {
          controller.replaceUser(id, ScimUserRequest(body), "userName", "active")
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    verify(exactly = 0) { mutationService.execute<Any>(any(), any()) }
    verify(exactly = 0) { lifecycleService.replace(any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH rejects a malformed projection before lifecycle effects`() {
    val current = user(1)
    val patch =
      objectMapper.readTree(
        """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"active","value":false}]}""",
      ) as com.fasterxml.jackson.databind.node.ObjectNode
    every { lifecycleService.get(configurationId, organizationId, current.id) } returns current
    every { lifecycleService.patch(configurationId, organizationId, current.id, any(), any()) } returns current.copy(active = false)

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.PATCH("https://airbyte.example.com/scim/v2/Users/${current.id}", patch)) {
          controller.patchUser(current.id, ScimPatchRequest(patch), "name..givenName", null)
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    verify(exactly = 0) { mutationService.execute<Any>(any(), any()) }
    verify(exactly = 0) { lifecycleService.get(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH rejects wrong-shaped ignored attributes before mutation and lifecycle effects`() {
    val current = user(1)
    val patch =
      objectMapper.readTree(
        """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"groups","value":false}]}""",
      ) as com.fasterxml.jackson.databind.node.ObjectNode
    every { lifecycleService.get(configurationId, organizationId, current.id) } returns current

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.PATCH("https://airbyte.example.com/scim/v2/Users/${current.id}", patch)) {
          controller.patchUser(current.id, ScimPatchRequest(patch), null, null)
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(error.scimType).isEqualTo("invalidValue")
    verify(exactly = 0) { mutationService.execute<Any>(any(), any()) }
    verify(exactly = 0) { lifecycleService.get(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH rejects unsupported mutable paths before mutation or unknown User lookup`() {
    val id = UUID.randomUUID()
    val patch =
      objectMapper.readTree(
        """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"enterprise.department","value":"Platform"}]}""",
      ) as com.fasterxml.jackson.databind.node.ObjectNode
    every { lifecycleService.get(configurationId, organizationId, id) } throws ScimUserNotFoundException()

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.PATCH("https://airbyte.example.com/scim/v2/Users/$id", patch)) {
          controller.patchUser(id, ScimPatchRequest(patch), null, null)
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(error.scimType).isEqualTo("invalidPath")
    verify(exactly = 0) { mutationService.execute<Any>(any(), any()) }
    verify(exactly = 0) { lifecycleService.get(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `PATCH rejects malformed mutable values before mutation or unknown User lookup`() {
    val id = UUID.randomUUID()
    val patch =
      objectMapper.readTree(
        """{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"replace","path":"active","value":"false"}]}""",
      ) as com.fasterxml.jackson.databind.node.ObjectNode
    every { lifecycleService.get(configurationId, organizationId, id) } throws ScimUserNotFoundException()

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.PATCH("https://airbyte.example.com/scim/v2/Users/$id", patch)) {
          controller.patchUser(id, ScimPatchRequest(patch), null, null)
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(error.scimType).isEqualTo("invalidValue")
    verify(exactly = 0) { mutationService.execute<Any>(any(), any()) }
    verify(exactly = 0) { lifecycleService.get(any(), any(), any()) }
    verify(exactly = 0) { lifecycleService.patch(any(), any(), any(), any(), any()) }
  }

  private fun user(index: Int): ScimUserRead {
    val body = userBody(active = true)
    return ScimUserRead(
      id = UUID(0, index.toLong()),
      configurationId = configurationId,
      organizationId = organizationId,
      userId = UUID(1, index.toLong()),
      externalId = "external-$index",
      userName = "user$index@example.com",
      primaryEmail = "user$index@example.com",
      active = true,
      attributes = body.deepCopy().also { it.remove(listOf("schemas", "userName", "externalId", "active")) },
      createdAt = OffsetDateTime.parse("2026-07-17T00:00:00Z").plusMinutes(index.toLong()),
      updatedAt = OffsetDateTime.parse("2026-07-17T00:00:00Z").plusMinutes(index.toLong()),
    )
  }

  private fun userBody(active: Boolean) =
    objectMapper.createObjectNode().also {
      it.putArray("schemas").add(SCIM_USER_SCHEMA)
      it.put("userName", "user1@example.com")
      it.put("externalId", "external-1")
      it.put("active", active)
      it
        .putArray("emails")
        .addObject()
        .put("value", "user1@example.com")
        .put("type", "work")
        .put("primary", true)
    }

  private fun <T> withRequest(
    request: HttpRequest<*>,
    block: () -> T,
  ): T {
    request.setAttribute(SCIM_AUTHENTICATION_ATTRIBUTE, context)
    return ServerRequestContext.with(request, java.util.concurrent.Callable { block() })
  }
}
