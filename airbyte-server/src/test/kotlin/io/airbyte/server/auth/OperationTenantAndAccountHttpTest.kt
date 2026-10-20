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
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.PropertySource
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
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

class OperationTenantAndAccountHttpTest {
  private val org = UUID.randomUUID()

  @Test
  fun `organization route allows saved org grants and denies another org without business work`() {
    request("organizations", "[\"$org\"]", listOf(orgGrant(org)), 200)
    request("organizations", "[\"$org\",\"${UUID.randomUUID()}\"]", listOf(orgGrant(org)), 403)
    request("organizations", "[]", listOf(instance()), 403)
  }

  @Test
  fun `organization route rejects workspace-only and scoped instance admins`() {
    request(
      "organizations",
      "[\"$org\"]",
      listOf(Permission().withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN).withWorkspaceId(UUID.randomUUID())),
      403,
    )
    request("organizations", "[\"$org\"]", listOf(instance()), 403, scoped = true)
  }

  @Test
  fun `account route accepts linked self or a saved instance admin`() {
    request("account", "[\"linked\",\"caller\"]", emptyList(), 200)
    request("account", "[\"other\"]", listOf(instance()), 200)
    request("account", "[\"other\"]", listOf(orgGrant(org)), 403)
    request("account", "[]", listOf(instance()), 403)
  }

  @Test
  fun `scoped account tokens cannot take either self or instance admin branch`() {
    request("account", "[\"caller\"]", emptyList(), 403, scoped = true)
    request("account", "[\"other\"]", listOf(instance()), 403, scoped = true)
  }

  @Test
  fun `missing organization stays not found and dependency errors stay server errors`() {
    request("organizations", "[\"$org\"]", listOf(instance()), 404, missing = true)
    request("organizations", "[\"$org\"]", listOf(instance()), 500, permissionFailure = true)
    request("account", "[\"other\"]", listOf(instance()), 500, permissionFailure = true)
  }

  private fun request(
    route: String,
    json: String,
    grants: List<Permission>,
    expected: Int,
    scoped: Boolean = false,
    missing: Boolean = false,
    permissionFailure: Boolean = false,
  ) {
    operationContext(checkerEnabled = true).use { context ->
      context.environment.addPropertySource(PropertySource.of("tenant-test", mapOf("test.tenant" to true)))
      val data = mockk<AuthorizationDataService>()
      every { data.organization(any()) } answers { firstArg() }
      if (missing) every { data.organization(any()) } throws AuthorizationResourceNotFoundException()
      val permissionService = mockk<PermissionService>()
      every { permissionService.getPermissionsByAuthUserId("caller") } returns grants
      if (permissionFailure) every { permissionService.getPermissionsByAuthUserId("caller") } throws IOException("permissions unavailable")
      context.registerSingleton(AuthorizationDataService::class.java, data)
      context.registerSingleton(
        OperationPermissions::class.java,
        OperationPermissions(permissionService, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
      )
      context.start()
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val response =
            client.send(
              HttpRequest
                .newBuilder(URI("${server.url}/api/v1/operation-tenant/$route"))
                .header("Authorization", if (scoped) "Bearer scoped" else "Bearer valid")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build(),
              HttpResponse.BodyHandlers.ofString(),
            )
          assertEquals(expected, response.statusCode(), response.body())
          assertEquals(if (expected == 200) 1 else 0, context.getBean(OperationTenantBusiness::class.java).calls.get())
        }
      }
    }
  }

  private fun orgGrant(id: UUID) = Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN).withOrganizationId(id)

  private fun instance() = Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
}

data class OperationTenantInput(
  val organizations: List<UUID>,
) : OperationInput

data class OperationAccountInput(
  val subjects: Set<String>,
) : OperationInput

@Singleton
@Requires(property = "test.tenant", value = "true")
class OperationTenantRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = OperationTenantInput((args.single() as List<*>).map { it as UUID })

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireOrganizations(
      caller,
      (input as OperationTenantInput).organizations.map(data::organization),
      setOf(AuthRoleConstants.ORGANIZATION_READER),
      setOf(OperationCaller.USER),
    )
  }
}

@Singleton
@Requires(property = "test.tenant", value = "true")
class OperationAccountRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = OperationAccountInput((args.single() as List<*>).map { it as String }.toSet())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireSelfOrInstanceAdmin(caller, (input as OperationAccountInput).subjects, setOf(OperationCaller.USER))
  }
}

@Singleton
@Requires(property = "test.tenant", value = "true")
class OperationTenantBusiness {
  val calls = AtomicInteger()

  fun read(): String {
    calls.incrementAndGet()
    return "ok"
  }
}

@Controller("/api/v1/operation-tenant")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(property = "test.tenant", value = "true")
class OperationTenantController(
  private val business: OperationTenantBusiness,
) {
  @Post("/organizations")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationTenantRule::class)
  fun organizations(
    @Body ids: List<UUID>,
  ): String = business.read()

  @Post("/account")
  @AuthorizeOperation(RouteAccessPolicy.PROTECTED, RouteMigration.SHARED, OperationAccountRule::class)
  fun account(
    @Body subjects: List<String>,
  ): String = business.read()
}
