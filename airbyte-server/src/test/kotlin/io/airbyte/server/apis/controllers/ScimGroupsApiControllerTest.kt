/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.scim.ScimGroupRequest
import io.airbyte.api.scim.ScimPatchRequest
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimGroupConflictException
import io.airbyte.domain.models.scim.ScimGroupInvalidMemberException
import io.airbyte.domain.models.scim.ScimGroupListPage
import io.airbyte.domain.models.scim.ScimGroupMember
import io.airbyte.domain.models.scim.ScimGroupNotFoundException
import io.airbyte.domain.models.scim.ScimGroupRead
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimGroupLifecycleService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.server.scim.SCIM_AUTHENTICATION_ATTRIBUTE
import io.airbyte.server.scim.SCIM_GROUP_SCHEMA
import io.airbyte.server.scim.SCIM_LIST_RESPONSE_SCHEMA
import io.airbyte.server.scim.SCIM_PATCH_OP_SCHEMA
import io.airbyte.server.scim.ScimException
import io.airbyte.server.scim.ScimGroupResourceService
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

class ScimGroupsApiControllerTest {
  private val objectMapper = jacksonObjectMapper().findAndRegisterModules()
  private val lifecycleService = mockk<ScimGroupLifecycleService>()
  private val mutationService = mockk<ScimMutationService>()
  private val resourceService = ScimGroupResourceService(objectMapper)
  private val controller = ScimGroupsApiController(lifecycleService, mutationService, resourceService)
  private val configurationId = UUID.fromString("11111111-1111-1111-1111-111111111111")
  private val organizationId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val userMappingId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val context = ScimAuthenticationContext(configurationId, OrganizationId(organizationId), "a".repeat(64))

  @BeforeEach
  fun setUp() {
    every { mutationService.execute<Any>(context, any()) } answers { secondArg<() -> Any>().invoke() }
  }

  @Test
  fun `create parses mutates and returns canonical projected location headers`() {
    val stored = group(1)
    val body = groupBody()
    every { lifecycleService.create(configurationId, organizationId, any()) } returns stored

    val response =
      withRequest(HttpRequest.POST("https://airbyte.example.com/scim/v2/Groups", body)) {
        controller.createGroup(ScimGroupRequest(body), "displayName", null)
      }

    assertThat(response.status).isEqualTo(HttpStatus.CREATED)
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Groups/${stored.id}")
    assertThat(response.header(HttpHeaders.CONTENT_LOCATION)).isEqualTo("https://airbyte.example.com/scim/v2/Groups/${stored.id}")
    assertThat(response.body().displayName).isEqualTo("Engineering 1")
    assertThat(response.body().members).isNull()
    assertThat(response.body().meta).isNull()
    verify(exactly = 1) { mutationService.execute<Any>(context, any()) }
  }

  @Test
  fun `create preserves configured base path in headers and metadata`() {
    val stored = group(1)
    val body = groupBody()
    val pathController = ScimGroupsApiController(lifecycleService, mutationService, resourceService, "https://airbyte.example.com/airbyte/")
    every { lifecycleService.create(configurationId, organizationId, any()) } returns stored

    val response =
      withRequest(HttpRequest.POST("https://internal.example.com/scim/v2/Groups", body)) {
        pathController.createGroup(ScimGroupRequest(body), null, null)
      }

    val location = "https://airbyte.example.com/airbyte/scim/v2/Groups/${stored.id}"
    assertThat(response.header(HttpHeaders.LOCATION)).isEqualTo(location)
    assertThat(
      response
        .body()
        .meta
        ?.location
        .toString(),
    ).isEqualTo(location)
  }

  @Test
  fun `get returns the exact canonical Group representation and honors projection`() {
    val stored = group(1)
    every { lifecycleService.get(configurationId, organizationId, stored.id) } returns stored

    val complete =
      withRequest(HttpRequest.GET<Any>("https://airbyte.example.com/scim/v2/Groups/${stored.id}")) {
        controller.getGroup(stored.id, null, null)
      }
    val projected =
      withRequest(HttpRequest.GET<Any>("https://airbyte.example.com/scim/v2/Groups/${stored.id}?attributes=displayName")) {
        controller.getGroup(stored.id, "displayName", null)
      }

    assertThat(complete.status).isEqualTo(HttpStatus.OK)
    assertThat(complete.body().schemas).containsExactly(SCIM_GROUP_SCHEMA)
    assertThat(complete.body().id).isEqualTo(stored.id.toString())
    assertThat(complete.body().displayName).isEqualTo("Engineering 1")
    assertThat(
      complete
        .body()
        .members
        ?.single()
        ?.value,
    ).isEqualTo(userMappingId.toString())
    assertThat(complete.body().meta?.resourceType).isEqualTo("Group")
    assertThat(
      complete
        .body()
        .meta
        ?.location
        .toString(),
    ).isEqualTo("https://airbyte.example.com/scim/v2/Groups/${stored.id}")
    assertThat(projected.body().displayName).isEqualTo("Engineering 1")
    assertThat(projected.body().members).isNull()
    assertThat(projected.body().meta).isNull()
  }

  @Test
  fun `list maps Group filters before stable pagination and projection`() {
    val second = group(2)
    val filter = slot<io.airbyte.domain.models.scim.ScimGroupFilterClause?>()
    every { lifecycleService.list(configurationId, organizationId, captureNullable(filter), 1, 1) } returns
      ScimGroupListPage(listOf(second), 2)

    val response =
      withRequest(HttpRequest.GET<Any>("https://airbyte.example.com/scim/v2/Groups")) {
        controller.listGroups("members[value eq \"$userMappingId\"]", 2, 1, "displayName", null)
      }

    assertThat(filter.captured?.attribute?.name).isEqualTo("MEMBER")
    assertThat(filter.captured?.value).isEqualTo(userMappingId.toString())
    assertThat(response.body().schemas).containsExactly(SCIM_LIST_RESPONSE_SCHEMA)
    assertThat(response.body().totalResults).isEqualTo(2)
    assertThat(response.body().startIndex).isEqualTo(2)
    assertThat(response.body().itemsPerPage).isEqualTo(1)
    assertThat(
      response
        .body()
        .resources
        .single()
        .displayName,
    ).isEqualTo("Engineering 2")
    assertThat(
      response
        .body()
        .resources
        .single()
        .members,
    ).isNull()
  }

  @Test
  fun `zero count preserves Group totals without resources`() {
    every { lifecycleService.list(configurationId, organizationId, null, 0, 0) } returns ScimGroupListPage(emptyList(), 2)

    val response =
      withRequest(HttpRequest.GET<Any>("https://airbyte.example.com/scim/v2/Groups?count=0")) {
        controller.listGroups(null, null, 0, null, null)
      }

    assertThat(response.body().totalResults).isEqualTo(2)
    assertThat(response.body().resources).isEmpty()
    assertThat(response.body().itemsPerPage).isZero()
  }

  @Test
  fun `PATCH returns no content by default after applying a complete membership replacement`() {
    val current = group(1)
    val updated = current.copy(displayName = "Platform")
    val input = slot<io.airbyte.domain.models.scim.ScimGroupWrite>()
    every { lifecycleService.get(configurationId, organizationId, current.id) } returns current
    every { lifecycleService.areActiveUsers(configurationId, organizationId, setOf(userMappingId)) } returns true
    every { lifecycleService.replace(configurationId, organizationId, current.id, capture(input)) } returns updated
    val patch =
      patchBody(
        """{"op":"replace","path":"displayName","value":"Platform"}""",
        """{"op":"replace","path":"members","value":[{"value":"$userMappingId"}]}""",
      )

    val response =
      withRequest(HttpRequest.PATCH("https://airbyte.example.com/scim/v2/Groups/${current.id}", patch)) {
        controller.patchGroup(current.id, ScimPatchRequest(patch), null, null)
      }

    assertThat(response.status).isEqualTo(HttpStatus.NO_CONTENT)
    assertThat(response.body()).isNull()
    assertThat(input.captured.displayName).isEqualTo("Platform")
    assertThat(input.captured.memberIds).containsExactly(userMappingId)
  }

  @Test
  fun `PATCH returns a projected representation when attributes are requested`() {
    val current = group(1)
    val updated = current.copy(displayName = "Platform")
    every { lifecycleService.get(configurationId, organizationId, current.id) } returns current
    every { lifecycleService.areActiveUsers(configurationId, organizationId, setOf(userMappingId)) } returns true
    every { lifecycleService.replace(configurationId, organizationId, current.id, any()) } returns updated
    val patch = patchBody("""{"op":"replace","path":"displayName","value":"Platform"}""")

    val response =
      withRequest(HttpRequest.PATCH("https://airbyte.example.com/scim/v2/Groups/${current.id}", patch)) {
        controller.patchGroup(current.id, ScimPatchRequest(patch), "displayName", null)
      }

    assertThat(response.status).isEqualTo(HttpStatus.OK)
    assertThat(response.body().displayName).isEqualTo("Platform")
    assertThat(response.body().meta).isNull()
  }

  @Test
  fun `replace and delete execute within authenticated mutations`() {
    val current = group(1)
    val body = groupBody()
    every { lifecycleService.replace(configurationId, organizationId, current.id, any()) } returns current
    every { lifecycleService.delete(configurationId, organizationId, current.id) } returns Unit

    val replaced =
      withRequest(HttpRequest.PUT("https://airbyte.example.com/scim/v2/Groups/${current.id}", body)) {
        controller.replaceGroup(current.id, ScimGroupRequest(body), null, null)
      }
    val deleted =
      withRequest(HttpRequest.DELETE<Any>("https://airbyte.example.com/scim/v2/Groups/${current.id}")) {
        controller.deleteGroup(current.id)
      }

    assertThat(replaced.status).isEqualTo(HttpStatus.OK)
    assertThat(deleted.status).isEqualTo(HttpStatus.NO_CONTENT)
    verify(exactly = 2) { mutationService.execute<Any>(context, any()) }
  }

  @Test
  fun `domain failures become the corresponding SCIM errors`() {
    val body = groupBody()
    every { lifecycleService.create(configurationId, organizationId, any()) } throws ScimGroupConflictException()
    val conflict =
      assertThrows<ScimException> {
        withRequest(HttpRequest.POST("https://airbyte.example.com/scim/v2/Groups", body)) {
          controller.createGroup(ScimGroupRequest(body), null, null)
        }
      }
    assertThat(conflict.status).isEqualTo(HttpStatus.CONFLICT)
    assertThat(conflict.scimType).isEqualTo("uniqueness")

    every { lifecycleService.create(configurationId, organizationId, any()) } throws ScimGroupInvalidMemberException()
    val invalid =
      assertThrows<ScimException> {
        withRequest(HttpRequest.POST("https://airbyte.example.com/scim/v2/Groups", body)) {
          controller.createGroup(ScimGroupRequest(body), null, null)
        }
      }
    assertThat(invalid.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(invalid.scimType).isEqualTo("invalidValue")

    every { lifecycleService.get(configurationId, organizationId, any()) } throws ScimGroupNotFoundException()
    val missing =
      assertThrows<ScimException> {
        withRequest(HttpRequest.GET<Any>("https://airbyte.example.com/scim/v2/Groups/${UUID.randomUUID()}")) {
          controller.getGroup(UUID.randomUUID(), null, null)
        }
      }
    assertThat(missing.status).isEqualTo(HttpStatus.NOT_FOUND)
  }

  @Test
  fun `invalid projections are rejected before mutation`() {
    val body = groupBody()
    every { lifecycleService.create(configurationId, organizationId, any()) } returns group(1)

    val error =
      assertThrows<ScimException> {
        withRequest(HttpRequest.POST("https://airbyte.example.com/scim/v2/Groups", body)) {
          controller.createGroup(ScimGroupRequest(body), "unsupported", null)
        }
      }

    assertThat(error.status).isEqualTo(HttpStatus.BAD_REQUEST)
    verify(exactly = 0) { mutationService.execute<Any>(any(), any()) }
  }

  private fun group(index: Int) =
    ScimGroupRead(
      id = UUID(0, index.toLong()),
      configurationId = configurationId,
      organizationId = organizationId,
      groupId = UUID(1, index.toLong()),
      externalId = "external-$index",
      displayName = "Engineering $index",
      createdAt = OffsetDateTime.parse("2026-07-17T00:00:00Z").plusMinutes(index.toLong()),
      updatedAt = OffsetDateTime.parse("2026-07-17T00:00:00Z").plusMinutes(index.toLong()),
      members = listOf(ScimGroupMember(userMappingId, UUID(2, index.toLong()), "Alice Example")),
    )

  private fun groupBody(): ObjectNode =
    objectMapper.createObjectNode().also {
      it.putArray("schemas").add(SCIM_GROUP_SCHEMA)
      it.put("displayName", "Engineering")
      it.put("externalId", "external-1")
      it.putArray("members").addObject().put("value", userMappingId.toString())
    }

  private fun patchBody(vararg operations: String): ObjectNode =
    objectMapper.readTree(
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[${operations.joinToString(",")}]}""",
    ) as ObjectNode

  private fun <T> withRequest(
    request: HttpRequest<*>,
    block: () -> T,
  ): T {
    request.setAttribute(SCIM_AUTHENTICATION_ATTRIBUTE, context)
    return ServerRequestContext.with(request, java.util.concurrent.Callable { block() })
  }
}
