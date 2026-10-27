/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.OrganizationUserReadList
import io.airbyte.api.server.generated.models.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.server.generated.models.UserRead
import io.airbyte.api.server.generated.models.UserWithPermissionInfoReadList
import io.airbyte.api.server.generated.models.WorkspaceUserAccessInfoReadList
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_NAME
import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver
import io.airbyte.commons.auth.support.UserAuthenticationResolver
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.ApplicationAuthorizationService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.ApplicationsController
import io.airbyte.server.apis.controllers.UserApiController
import io.micronaut.context.ApplicationContext
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.utils.SecurityService
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID

abstract class RemainingUserAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `user SELF onboarding identity and application owner checks run before business`() {
    val cases =
      listOf(
        Triple("/api/v1/users/get", """{"userId":"$USER"}""", 403),
        Triple("/api/v1/users/get_or_create_by_auth_id", """{"authUserId":"other-subject"}""", 500),
        Triple("/api/v1/applications/create", """{"name":"fixture"}""", 403),
        Triple("/api/v1/applications/delete", """{"applicationId":"$APPLICATION"}""", 500),
      )
    assertAll(
      cases.map { (path, body, expected) ->
        Executable {
          RemainingOperationHttpSupport(
            cloud,
            listOf(UserApiController::class.java, ApplicationsController::class.java),
            setOf(
              "io.airbyte.server.auth.UserSelfRule",
              "io.airbyte.server.auth.UserOnboardingRule",
              "io.airbyte.server.auth.CurrentUserApplicationRule",
              "io.airbyte.server.auth.OwnedApplicationRule",
            ),
          ).run(
            configure = { context, business ->
              val user = UserRead(USER, "fixture@example.com", emptyMap<String, String>())
              business.forEach { instance ->
                when (instance) {
                  is UserHandler -> {
                    every { instance.getUser(match { it.userId == USER }) } returns user
                    every { instance.getOrCreateUserByAuthId(match { it.authUserId == "other-subject" }) } returns
                      UserGetOrCreateByAuthIdResponse(user, "other-subject")
                  }
                  is CurrentUserService -> every { instance.getCurrentUser() } returns AuthenticatedUser().withUserId(USER).withAuthUserId("caller")
                  is ApplicationService -> {
                    val application =
                      Application()
                        .withId(
                          APPLICATION.toString(),
                        ).withName("fixture")
                        .withClientId("fixture")
                        .withClientSecret("fixture")
                        .withCreatedOn("2026-10-01T00:00:00Z")
                    every { instance.createApplication(match { it.userId == USER }, "fixture") } returns application
                    every { instance.deleteApplication(match { it.userId == USER }, APPLICATION.toString()) } returns application
                  }
                }
              }
              val grants = mockk<PermissionService>()
              every { grants.getPermissionsByAuthUserId("caller") } returns emptyList()
              val data = mockk<AuthorizationDataService>()
              every { data.userAuthSubjects(USER) } returns setOf("other-subject")
              context.registerSingleton(AuthorizationDataService::class.java, data)
              val applicationData = mockk<ApplicationAuthorizationService>()
              every { applicationData.currentUserSubject("caller") } returns "caller"
              every { applicationData.requireApplicationOwner("caller", APPLICATION) } throws
                IllegalArgumentException("application was not found with the userId and applicationId provided")
              context.registerSingleton(ApplicationAuthorizationService::class.java, applicationData)
              registerIdentities(context, true)
              context.registerSingleton(
                OperationPermissions::class.java,
                OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
              )
            },
            check = { _, server, business ->
              val response =
                RemainingOperationHttpSupport.request(
                  server,
                  "POST",
                  path,
                  body,
                  if (path.endsWith("/applications/create")) "workload" else "user",
                )
              assertEquals(expected, response.statusCode(), "$path: ${response.body()}")
              RemainingOperationHttpSupport.requireNoBusiness(business)
            },
          )
        }
      },
    )
  }

  @Test
  fun `all three application operations execute current owner policies and reject before credentials`() {
    for (scenario in applicationScenarios()) {
      exerciseApplication(scenario, 200)
      exerciseApplication(scenario, 200, token = "application-owner")
      for (token in listOf(
        "internal",
        "dataplane",
        "service",
        "embedded",
        "workload",
        "scoped:$WORKSPACE",
      )) {
        exerciseApplication(scenario, 403, token = token)
      }
    }
  }

  @Test
  fun `application deletion requires saved ownership even for administrator and preserves provider failures`() {
    val deletion = applicationScenarios().single { it.rule == OwnedApplicationRule::class.java }
    exerciseApplication(deletion, 500, foreign = true)
    exerciseApplication(deletion, 500, providerFailure = true)
    exerciseApplication(deletion, 500, unsupported = true)
    for (body in listOf("{}", "null", """{"applicationId":null}""")) exerciseApplication(deletion.copy(body = body), 400)
    for (scenario in applicationScenarios()) exerciseApplication(scenario, 401, missingUser = true)
  }

  @Test
  fun `no auth application management uses the saved community auth subject rather than its synthetic principal`() {
    for (scenario in applicationScenarios()) exerciseApplication(scenario, 200, security = false)
    exerciseApplication(applicationScenarios().single { it.rule == OwnedApplicationRule::class.java }, 500, security = false, foreign = true)
  }

  private fun exerciseApplication(
    scenario: ApplicationScenario,
    expected: Int,
    token: String = "user",
    security: Boolean = true,
    foreign: Boolean = false,
    providerFailure: Boolean = false,
    unsupported: Boolean = false,
    missingUser: Boolean = false,
  ) {
    val data = mockk<ApplicationAuthorizationService>()
    val grants = mockk<PermissionService>()
    val owner = if (security) "caller" else "community-subject"
    RemainingOperationHttpSupport(
      cloud,
      listOf(ApplicationsController::class.java),
      applicationScenarios().map { it.rule.name }.toSet(),
      security,
    ).run(
      configure = { context, business ->
        val user = AuthenticatedUser().withUserId(USER).withAuthUserId(owner)
        val application =
          Application()
            .withId(
              APPLICATION.toString(),
            ).withName("fixture")
            .withClientId("fixture")
            .withClientSecret("fixture")
            .withCreatedOn("2026-10-01T00:00:00Z")
        business.filterIsInstance<CurrentUserService>().forEach { service -> every { service.getCurrentUser() } returns user }
        business.filterIsInstance<ApplicationService>().forEach { provider ->
          every { provider.createApplication(match { it.userId == USER && it.authUserId == owner }, "fixture") } returns application
          every { provider.listApplicationsByUser(match { it.userId == USER && it.authUserId == owner }) } returns listOf(application)
          every { provider.deleteApplication(match { it.userId == USER && it.authUserId == owner }, APPLICATION.toString()) } returns application
        }
        every { data.currentUserSubject("caller") } returns "caller"
        every { data.defaultUserSubject() } returns "community-subject"
        if (missingUser) every { data.currentUserSubject("caller") } throws NoSuchElementException("current user is missing or ambiguous")
        every { data.requireApplicationOwner(owner, APPLICATION) } just Runs
        if (foreign) {
          every { data.requireApplicationOwner(owner, APPLICATION) } throws
            IllegalArgumentException("application was not found with the userId and applicationId provided")
        }
        if (providerFailure) {
          every { data.requireApplicationOwner(owner, APPLICATION) } throws
            java.io.IOException("application ownership unavailable")
        }
        if (unsupported) every { data.requireApplicationOwner(owner, APPLICATION) } throws UnsupportedOperationException()
        context.registerSingleton(ApplicationAuthorizationService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule.name)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${server.url}${scenario.path}: ${response.headers().map()} ${response.body()}")
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200 && scenario.rule != OwnedApplicationRule::class.java) assertTrue(response.body().contains("fixture"), scenario.path)
        if (expected == 403) verify { data wasNot Called }
        if (!security) verify(exactly = 0) { data.currentUserSubject(any()) }
        verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
        verify(exactly = 0) { grants.getPermissionsByServiceAccountId(any()) }
      },
    )
  }

  private data class ApplicationScenario(
    val path: String,
    val body: String,
    val rule: Class<*>,
  )

  private fun applicationScenarios() =
    listOf(
      ApplicationScenario("/api/v1/applications/create", """{"name":"fixture"}""", CurrentUserApplicationRule::class.java),
      ApplicationScenario("/api/v1/applications/list", "{}", CurrentUserApplicationRule::class.java),
      ApplicationScenario("/api/v1/applications/delete", """{"applicationId":"$APPLICATION"}""", OwnedApplicationRule::class.java),
    )

  @Test
  fun `all nine user operations execute independent expected rules and normal responses`() {
    assertAll(userScenarios().map { scenario -> Executable { exerciseUser(scenario, 200) } })
  }

  @Test
  fun `all nine user operations deny before profile permission or onboarding business`() {
    assertAll(
      userScenarios().map { scenario ->
        Executable {
          exerciseUser(
            scenario.copy(body = scenario.deniedBody),
            403,
            granted = false,
            self = false,
            token =
              if (scenario.rule ==
                UserOnboardingRule::class.java
              ) {
                "workload"
              } else {
                "user"
              },
          )
        }
      },
    )
  }

  @Test
  fun `user SELF conversion admin unlinked users and profile workspace preferences preserve existing policies`() {
    for (scenario in userScenarios().filter { it.rule == UserSelfRule::class.java && it.path != "/api/v1/users/get_by_auth_id" }) {
      exerciseUser(scenario, 200, grants = listOf(Permission.PermissionType.INSTANCE_ADMIN), unlinked = true)
      exerciseUser(scenario, 403, self = false, grants = listOf(Permission.PermissionType.ORGANIZATION_ADMIN))
      exerciseUser(scenario, 403, token = "scoped:$WORKSPACE", grants = listOf(Permission.PermissionType.INSTANCE_ADMIN))
    }
    val email = userScenarios().single { it.rule == UserEmailRule::class.java }
    exerciseUser(email, 403, granted = false)
    val onboarding = userScenarios().single { it.rule == UserOnboardingRule::class.java }
    exerciseUser(onboarding.copy(body = """{"authUserId":"other-subject"}"""), 500)
  }

  @Test
  fun `user saved target absence and dependency errors stop business while first login needs no saved user`() {
    exerciseUser(userScenarios().first(), 404, grants = listOf(Permission.PermissionType.INSTANCE_ADMIN), missing = true)
    exerciseUser(userScenarios().first(), 500, lookupError = true)
    exerciseUser(userScenarios().single { it.rule == UserEmailRule::class.java }, 404, missing = true)
    exerciseUser(userScenarios().single { it.path.endsWith("/get_by_auth_id") }, 404, missing = true)
    exerciseUser(userScenarios().single { it.rule == UserOnboardingRule::class.java }, 200, missing = true)
    for (body in listOf("{}", "null", """{"userId":null}""")) exerciseUser(userScenarios().first().copy(body = body), 400)
  }

  @Test
  fun `user unsupported callers fail despite administrator labels and no auth keeps profile responses`() {
    for (scenario in userScenarios()) {
      exerciseUser(scenario, 403, token = "workload", grants = listOf(Permission.PermissionType.INSTANCE_ADMIN))
      exerciseUser(scenario, 403, token = "embedded", grants = listOf(Permission.PermissionType.INSTANCE_ADMIN))
      exerciseUser(scenario, 200, granted = false, security = false)
    }
  }

  private fun exerciseUser(
    scenario: UserScenario,
    expected: Int,
    grants: List<Permission.PermissionType> = scenario.grants,
    granted: Boolean = true,
    self: Boolean = true,
    token: String = "user",
    security: Boolean = true,
    unlinked: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val permissions = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, listOf(UserApiController::class.java), userScenarios().map { it.rule.name }.toSet(), security).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<UserHandler>().single()
        val user = UserRead(USER, "fixture@example.com", emptyMap<String, String>())
        every { handler.getUser(match { it.userId == USER }) } returns user
        every { handler.getUserByAuthId(match { it.authUserId == "caller" }) } returns user
        every { handler.getUserByEmail(match { it.email == "fixture@example.com" }) } returns user
        every { handler.updateUser(match { it.userId == USER && it.name == "fixture" && it.defaultWorkspaceId == UUID(0, 99) }) } returns
          user.copy(name = "fixture", defaultWorkspaceId = UUID(0, 99))
        every { handler.deleteUser(match { it.userId == USER }) } just Runs
        every { handler.listUsersInOrganization(match { it.organizationId == ORGANIZATION }) } returns OrganizationUserReadList(emptyList())
        every { handler.listAccessInfoByWorkspaceId(match { it.workspaceId == WORKSPACE }) } returns WorkspaceUserAccessInfoReadList(emptyList())
        every { handler.listInstanceAdminUsers() } returns UserWithPermissionInfoReadList(emptyList())
        every { handler.getOrCreateUserByAuthId(match { it.authUserId == "caller" }) } returns
          UserGetOrCreateByAuthIdResponse(user, "caller", newUserCreated = true)
        every { data.userAuthSubjects(USER) } returns setOf(if (self) "caller" else "other-subject")
        if (unlinked) every { data.userAuthSubjects(USER) } throws AuthorizationResourceNotFoundException()
        every { data.requireUser(USER) } just Runs
        every { data.requireAuthUserSubject("caller") } just Runs
        every { data.requireUserByEmail("fixture@example.com") } just Runs
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        every { data.workspaceAndOrganization(WORKSPACE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        if (missing) {
          every { data.requireUser(USER) } throws AuthorizationResourceNotFoundException()
          every { data.requireAuthUserSubject("caller") } throws AuthorizationResourceNotFoundException()
          every { data.requireUserByEmail("fixture@example.com") } throws AuthorizationResourceNotFoundException()
        }
        if (lookupError) every { data.userAuthSubjects(USER) } throws java.io.IOException("user ownership unavailable")
        val saved =
          if (!granted) {
            emptyList()
          } else {
            grants.map { grant ->
              Permission().withPermissionType(grant).also {
                if (grant.name.startsWith("ORGANIZATION")) it.organizationId = ORGANIZATION
                if (grant.name.startsWith("WORKSPACE")) it.workspaceId = WORKSPACE
              }
            }
          }
        every { permissions.getPermissionsByAuthUserId("caller") } returns saved
        every { permissions.getPermissionsByServiceAccountId(UUID(0, 9)) } returns saved
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
        registerIdentities(context, security)
      },
      check = { context, server, business ->
        RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule.name)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${server.url}${scenario.path}: ${response.headers().map()} ${response.body()}")
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200 && !scenario.emptyResponse) assertTrue(response.body().isNotBlank(), scenario.path)
        if (scenario.rule == UserOnboardingRule::class.java) {
          verify(exactly = 0) { data.userAuthSubjects(any()) }
          verify(exactly = 0) { data.requireAuthUserSubject(any()) }
          verify(exactly = 0) { data.requireUser(any()) }
        }
        if (unlinked) verify(exactly = 0) { data.userAuthSubjects(any()) }
        if (!security) {
          verify(exactly = 0) { permissions.getPermissionsByAuthUserId(any()) }
          verify(exactly = 0) { permissions.getPermissionsByServiceAccountId(any()) }
        }
      },
    )
  }

  private fun registerIdentities(
    context: ApplicationContext,
    security: Boolean,
  ) {
    val service = mockk<SecurityService>()
    every { service.username() } returns Optional.of("caller")
    every { service.authentication } returns
      Optional.of(Authentication.build("caller", emptyList(), mapOf(JWT_USER_EMAIL to "fixture@example.com", JWT_USER_NAME to "fixture")))
    context.registerSingleton(
      UserAuthenticationResolver::class.java,
      if (security) JwtUserAuthenticationResolver(service) else JwtUserAuthenticationResolver(null),
    )
  }

  private data class UserScenario(
    val path: String,
    val body: String,
    val rule: Class<*>,
    val grants: List<Permission.PermissionType> = emptyList(),
    val emptyResponse: Boolean = false,
    val deniedBody: String = body,
  )

  private fun userScenarios(): List<UserScenario> {
    val id = """{"userId":"$USER"}"""
    return listOf(
      UserScenario("/api/v1/users/get", id, UserSelfRule::class.java),
      UserScenario("/api/v1/users/delete", id, UserSelfRule::class.java, emptyResponse = true),
      UserScenario("/api/v1/users/update", """{"userId":"$USER","name":"fixture","defaultWorkspaceId":"${UUID(0, 99)}"}""", UserSelfRule::class.java),
      UserScenario(
        "/api/v1/users/get_by_auth_id",
        """{"authUserId":"caller","isAgenticUser":true}""",
        UserSelfRule::class.java,
        deniedBody = """{"authUserId":"other-subject"}""",
      ),
      UserScenario(
        "/api/v1/users/get_by_email",
        """{"email":"fixture@example.com"}""",
        UserEmailRule::class.java,
        listOf(Permission.PermissionType.INSTANCE_ADMIN),
      ),
      UserScenario(
        "/api/v1/users/list_by_organization_id",
        """{"organizationId":"$ORGANIZATION"}""",
        OrganizationMemberRule::class.java,
        listOf(Permission.PermissionType.ORGANIZATION_MEMBER),
      ),
      UserScenario(
        "/api/v1/users/list_access_info_by_workspace_id",
        """{"workspaceId":"$WORKSPACE"}""",
        UserWorkspaceReadRule::class.java,
        listOf(Permission.PermissionType.WORKSPACE_READER),
      ),
      UserScenario("/api/v1/users/list_instance_admin", "{}", GlobalAdminRule::class.java, listOf(Permission.PermissionType.INSTANCE_ADMIN)),
      UserScenario("/api/v1/users/get_or_create_by_auth_id", """{"authUserId":"caller"}""", UserOnboardingRule::class.java),
    )
  }

  companion object {
    val USER: UUID = UUID(0, 8)
    val APPLICATION: UUID = UUID(0, 16)
    val ORGANIZATION: UUID = UUID(0, 4)
    val WORKSPACE: UUID = UUID(0, 3)
  }
}
