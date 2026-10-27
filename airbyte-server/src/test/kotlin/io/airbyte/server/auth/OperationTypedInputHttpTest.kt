/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.QueryValue
import io.micronaut.http.exceptions.HttpStatusException
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecurityRule
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class OperationTypedInputHttpTest {
  @Test
  fun `valid scoped tokens reach typed SHARED authorization without legacy body reference headers`() {
    runRequest(body(listOf(FIRST)), expected = 200, expectedCalls = 1, scoped = true, workspaceOnly = true, expectedBody = "$FIRST:$ORGANIZATION:0")
  }

  @Test
  fun `typed SHARED resources still reject a mismatched token scope after an earlier ALLOWED`() {
    runRequest(body(listOf(SECOND)), expected = 403, scoped = true, workspaceOnly = true, earlierAllow = true)
  }

  @Test
  fun `SELF rejects scoped tokens after an earlier ALLOWED even when the subject matches`() {
    runRequest(body(listOf(FIRST)), expected = 403, scoped = true, earlierAllow = true)
  }

  @Test
  fun `typed body nested list path and query references are authorized against saved tenants with zero counters`() {
    runRequest(body(), expected = 200, expectedCalls = 1)
  }

  @Test
  fun `required missing null empty and duplicate references reject before protected work`() {
    for (invalid in listOf("{}", "null", "{\"references\":null,\"counter\":0}", body(emptyList()), body(listOf(FIRST, FIRST)))) {
      runRequest(invalid, expected = 400)
    }
  }

  @Test
  fun `mixed permitted and denied list never approves a partial target set`() {
    runRequest(body(), expected = 403, grants = listOf(FIRST))
  }

  @Test
  fun `SELF mismatch rejects even with access to every workspace`() {
    runRequest(body(), expected = 403, subject = "other")
  }

  @Test
  fun `saved organization mismatch rejects even with permission on both workspaces`() {
    runRequest(body(), expected = 400, mismatchedOrganization = true)
  }

  @Test
  fun `missing required workspace returns not found without approving the resources that exist`() {
    runRequest(body(), expected = 404, missingWorkspace = true)
  }

  @Test
  fun `workspace lookup and permission dependency failures remain server errors without business work`() {
    runRequest(body(), expected = 500, workspaceLookupFailure = true)
    runRequest(body(), expected = 500, permissionFailure = true)
  }

  private fun runRequest(
    json: String,
    expected: Int,
    expectedCalls: Int = 0,
    grants: List<UUID> = listOf(FIRST, SECOND),
    subject: String = "caller",
    mismatchedOrganization: Boolean = false,
    missingWorkspace: Boolean = false,
    workspaceLookupFailure: Boolean = false,
    permissionFailure: Boolean = false,
    scoped: Boolean = false,
    workspaceOnly: Boolean = false,
    earlierAllow: Boolean = false,
    expectedBody: String = "$FIRST,$SECOND:$ORGANIZATION:0",
  ) {
    operationContext(checkerEnabled = true, typed = true, scopeRule = scoped, earlierAllow = earlierAllow).use { context ->
      if (scoped) {
        val headers = mockk<AuthenticationHeaderResolver>()
        every { headers.resolveWorkspace(any()) } returns emptyList()
        context.registerSingleton(AuthenticationHeaderResolver::class.java, headers)
      }
      val workspaces = mockk<WorkspaceService>()
      for (workspace in listOf(FIRST, SECOND)) {
        every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } returns
          StandardWorkspace()
            .withWorkspaceId(workspace)
            .withOrganizationId(if (mismatchedOrganization && workspace == SECOND) UUID.randomUUID() else ORGANIZATION)
      }
      if (missingWorkspace) {
        every { workspaces.getStandardWorkspaceNoSecrets(SECOND, false) } throws
          ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, SECOND)
      }
      if (workspaceLookupFailure) every { workspaces.getStandardWorkspaceNoSecrets(SECOND, false) } throws IOException("workspace lookup unavailable")
      val permissions = mockk<PermissionService>()
      every { permissions.getPermissionsByAuthUserId("caller") } returns
        grants.map { Permission().withWorkspaceId(it).withPermissionType(Permission.PermissionType.WORKSPACE_READER) }
      if (permissionFailure) every { permissions.getPermissionsByAuthUserId("caller") } throws IOException("permission store unavailable")
      val data =
        AuthorizationDataService(
          workspaces,
          permissions,
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          privateLinks = mockk(),
          connectionEvents = mockk(),
          actorRepository = mockk(),
          attempts = mockk(),
          entitlements = mockk(),
          featureFlags = mockk(),
        )
      context.registerSingleton(AuthorizationDataService::class.java, data)
      context.registerSingleton(
        OperationPermissions::class.java,
        OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
      )
      context.start()
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val request =
            HttpRequest
              .newBuilder(URI("${server.url}/api/v1/operation-typed${if (workspaceOnly) "/workspace" else ""}/$ORGANIZATION?subject=$subject"))
              .header("Authorization", if (scoped) "Bearer scoped" else "Bearer valid")
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(json))
              .build()
          val response = client.send(request, HttpResponse.BodyHandlers.ofString())
          assertEquals(expected, response.statusCode(), response.body())
          assertEquals(expectedCalls, context.getBean(OperationTypedBusiness::class.java).calls.get())
          if (expected == 200) assertEquals(expectedBody, response.body())
        }
      }
    }
  }

  private fun body(ids: List<UUID> = listOf(FIRST, SECOND)) = "{\"references\":[${ids.joinToString { "{\"workspaceId\":\"$it\"}" }}],\"counter\":0}"

  companion object {
    private val FIRST = UUID.fromString("00000000-0000-0000-0000-000000000001")
    private val SECOND = UUID.fromString("00000000-0000-0000-0000-000000000002")
    private val ORGANIZATION = UUID.fromString("00000000-0000-0000-0000-000000000003")
  }
}

data class OperationTypedReference(
  var workspaceId: UUID,
)

data class OperationTypedBody(
  val references: MutableList<OperationTypedReference>,
  val counter: Int,
)

data class OperationTypedInput(
  val organizationId: UUID,
  val subject: String,
  val workspaces: List<UUID>,
  val counter: Int,
) : OperationInput

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationTypedRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationTypedInput {
    val body = args[0] as OperationTypedBody
    val ids = body.references.map { it.workspaceId }
    if (ids.isEmpty() || ids.distinct().size != ids.size ||
      body.counter < 0
    ) {
      throw HttpStatusException(HttpStatus.BAD_REQUEST, "Invalid resource references")
    }
    return OperationTypedInput(args[1] as UUID, args[2] as String, ids.toList(), body.counter)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    input as OperationTypedInput
    permissions.requireSelf(caller, input.subject)
    authorizeWorkspaces(caller, input)
  }

  fun authorizeWorkspaces(
    caller: Authentication,
    input: OperationTypedInput,
  ) {
    val workspacesAndOrganizations = input.workspaces.map(data::workspaceAndOrganization)
    if (workspacesAndOrganizations.any {
        it.organizationId != input.organizationId
      }
    ) {
      throw HttpStatusException(HttpStatus.BAD_REQUEST, "Invalid resource relationship")
    }
    permissions.requireWorkspaces(caller, workspacesAndOrganizations, setOf(AuthRoleConstants.WORKSPACE_READER), setOf(OperationCaller.USER))
  }
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationWorkspaceTypedRule(
  private val typed: OperationTypedRule,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationTypedInput = typed.readInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = typed.authorizeWorkspaces(caller, input as OperationTypedInput)
}

@Singleton
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
class OperationTypedBusiness {
  val calls = AtomicInteger()

  fun read(
    body: OperationTypedBody,
    organizationId: UUID,
  ): String {
    calls.incrementAndGet()
    return "${body.references.joinToString(",") { it.workspaceId.toString() }}:$organizationId:${body.counter}"
  }
}

@Controller("/api/v1/operation-typed")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(property = "spec.name", value = "OperationAuthorizationHttpTest")
@Requires(property = "test.typed", value = "true")
class OperationTypedController(
  private val business: OperationTypedBusiness,
) {
  @Post("/{organizationId}")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationTypedRule::class)
  fun read(
    @Body body: OperationTypedBody,
    @PathVariable organizationId: UUID,
    @QueryValue subject: String,
  ): String = business.read(body, organizationId)

  @Post("/workspace/{organizationId}")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationWorkspaceTypedRule::class)
  fun readWorkspace(
    @Body body: OperationTypedBody,
    @PathVariable organizationId: UUID,
    @QueryValue subject: String,
  ): String = business.read(body, organizationId)
}
