/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.commons.server.authorization.AuthenticationFactory
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.helpers.WorkspaceHelper
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.micronaut.security.utils.SecurityService
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.Called
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

private const val SPEC = "AuthorizationScopeConsistencyHttpTest"
private const val ATTACKER_AUTH_USER_ID = "attacker-auth-user"

/**
 * Drives the request bodies from CVE-2026-80049 through the real Netty header extraction, JWT authentication, role
 * resolution and `@Secured` check. Only the persistence-backed collaborators are replaced. The caller is a reader in
 * their own workspace and holds nothing on the victim's.
 *
 * The JWT [AuthenticationFactory] is rebuilt here around a real [RoleResolver] because another test in this module
 * replaces `RoleResolver` module-wide with a relaxed mock (see `SecretStorageApiControllerTest`), which would otherwise
 * leave every authenticated request without roles.
 */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class AuthorizationScopeConsistencyHttpTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var jwtTokenGenerator: JwtTokenGenerator

  private val attackerWs = UUID.randomUUID()
  private val victimWs = UUID.randomUUID()
  private val attackerOrg = UUID.randomUUID()
  private val victimOrg = UUID.randomUUID()
  private val attackerSource = UUID.randomUUID()
  private val victimSource = UUID.randomUUID()
  private val attackerConn = UUID.randomUUID()
  private val attackerJob = 4242L
  private val victimJob = 7L
  private val victimUser = UUID.randomUUID()

  @BeforeEach
  fun stub() {
    with(ScopeConsistencyTestBeans) {
      clearMocks(
        permissionHandler,
        workspaceHelper,
        sourceHandler,
        workspacesHandler,
        jobHistoryHandler,
        organizationsHandler,
        userHandler,
        userPersistence,
      )
      every { userPersistence.listAuthUserIdsForUser(victimUser) } returns listOf("victim-auth-user")
      every { workspaceHelper.getWorkspaceForJobId(attackerJob) } returns attackerWs
      every { workspaceHelper.getWorkspaceForJobId(victimJob) } returns victimWs
      every { permissionHandler.getPermissionsByAuthUserId(ATTACKER_AUTH_USER_ID) } returns
        listOf(Permission().withPermissionType(Permission.PermissionType.WORKSPACE_READER).withWorkspaceId(attackerWs))
      every { workspaceHelper.getWorkspaceForSourceId(attackerSource) } returns attackerWs
      every { workspaceHelper.getWorkspaceForSourceId(victimSource) } returns victimWs
      every { workspaceHelper.getWorkspaceForConnectionId(attackerConn) } returns attackerWs
      every { workspaceHelper.getOrganizationForWorkspace(attackerWs) } returns attackerOrg
      every { workspaceHelper.getOrganizationForWorkspace(victimWs) } returns victimOrg
      every { sourceHandler.getSource(any()) } returns SourceRead().sourceId(attackerSource).workspaceId(attackerWs)
      every { workspacesHandler.getWorkspace(any()) } returns WorkspaceRead().workspaceId(victimWs)
    }
  }

  @Test
  fun `the published request is rejected before the handler runs`() {
    val status =
      postExpectingError(
        "/api/v1/sources/get",
        mapOf("sourceId" to victimSource.toString(), "workspaceId" to attackerWs.toString()),
      )
    assertEquals(HttpStatus.FORBIDDEN, status)
    verify { ScopeConsistencyTestBeans.sourceHandler wasNot Called }
  }

  @Test
  fun `the mirror image request is rejected before the handler runs`() {
    val status =
      postExpectingError(
        "/api/v1/workspaces/get",
        mapOf("workspaceId" to victimWs.toString(), "sourceId" to attackerSource.toString()),
      )
    assertEquals(HttpStatus.FORBIDDEN, status)
    verify { ScopeConsistencyTestBeans.workspacesHandler wasNot Called }
  }

  @Test
  fun `a higher precedence decoy is rejected`() {
    val status =
      postExpectingError(
        "/api/v1/sources/get",
        mapOf("sourceId" to victimSource.toString(), "connectionId" to attackerConn.toString()),
      )
    assertEquals(HttpStatus.FORBIDDEN, status)
    verify { ScopeConsistencyTestBeans.sourceHandler wasNot Called }
  }

  @Test
  fun `a job id written as a decimal is rejected before the handler runs`() {
    // The endpoint reads 7.0 as job 7, which belongs to the victim.
    val status = postExpectingError("/api/v1/jobs/get", mapOf("id" to 7.0, "workspaceId" to attackerWs.toString()))
    assertEquals(HttpStatus.FORBIDDEN, status)
    verify { ScopeConsistencyTestBeans.jobHistoryHandler wasNot Called }
  }

  @Test
  fun `a jobId field that hides the job id of a victim is rejected before the handler runs`() {
    // The endpoint reads `id` and ignores `jobId`.
    val status =
      postExpectingError(
        "/api/v1/jobs/get",
        mapOf("id" to victimJob, "jobId" to attackerJob, "workspaceId" to attackerWs.toString()),
      )
    assertEquals(HttpStatus.FORBIDDEN, status)
    verify { ScopeConsistencyTestBeans.jobHistoryHandler wasNot Called }
  }

  @Test
  fun `an organization_id field that hides the organization of a victim is rejected before the handler runs`() {
    // The endpoint reads `organizationId` and ignores `organization_id`. The caller is an admin of their own organization.
    every { ScopeConsistencyTestBeans.permissionHandler.getPermissionsByAuthUserId(ATTACKER_AUTH_USER_ID) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN).withOrganizationId(attackerOrg))
    val status =
      postExpectingError(
        "/api/v1/organizations/update",
        mapOf(
          "organizationId" to victimOrg.toString(),
          "organization_id" to attackerOrg.toString(),
          "organizationName" to "renamed",
        ),
      )
    assertEquals(HttpStatus.FORBIDDEN, status)
    verify { ScopeConsistencyTestBeans.organizationsHandler wasNot Called }
  }

  @Test
  fun `the user id of a victim next to the caller's own auth id is rejected before the handler runs`() {
    // The endpoint reads `userId`. The caller's own `authUserId` must not be what earns the SELF role.
    val status =
      postExpectingError(
        "/api/v1/users/get",
        mapOf("userId" to victimUser.toString(), "authUserId" to ATTACKER_AUTH_USER_ID),
      )
    assertEquals(HttpStatus.FORBIDDEN, status)
    verify { ScopeConsistencyTestBeans.userHandler wasNot Called }
  }

  @Test
  fun `a request naming only the caller's own source succeeds`() {
    val status = post("/api/v1/sources/get", mapOf("sourceId" to attackerSource.toString()))
    assertEquals(HttpStatus.OK, status)
  }

  @Test
  fun `a request whose identifiers agree succeeds`() {
    val status =
      post(
        "/api/v1/sources/get",
        mapOf("sourceId" to attackerSource.toString(), "workspaceId" to attackerWs.toString()),
      )
    assertEquals(HttpStatus.OK, status)
  }

  private fun post(
    path: String,
    body: Map<String, Any>,
  ): HttpStatus = client.toBlocking().exchange(HttpRequest.POST(path, body).bearerAuth(bearerToken()), String::class.java).status

  private fun postExpectingError(
    path: String,
    body: Map<String, Any>,
  ): HttpStatus = assertThrows<HttpClientResponseException> { post(path, body) }.status

  private fun bearerToken(): String =
    jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to "http://test-url.com",
          "aud" to "airbyte-server",
          "sub" to ATTACKER_AUTH_USER_ID,
          "exp" to Instant.now().plus(10, ChronoUnit.MINUTES).epochSecond,
        ),
      ).orElseThrow()
}

@Requires(property = "spec.name", value = SPEC)
@Factory
class ScopeConsistencyTestBeans {
  @Singleton
  @Primary
  @Replaces(AuthenticationFactory::class)
  fun authenticationFactory(
    authenticationHeaderResolver: AuthenticationHeaderResolver,
    currentUserService: CurrentUserService,
    securityService: SecurityService?,
    permissionHandler: PermissionHandler,
  ): AuthenticationFactory = AuthenticationFactory(RoleResolver(authenticationHeaderResolver, currentUserService, securityService, permissionHandler))

  @Singleton
  @Replaces(PermissionHandler::class)
  fun permissionHandler(): PermissionHandler = Companion.permissionHandler

  @Singleton
  @Replaces(WorkspaceHelper::class)
  fun workspaceHelper(): WorkspaceHelper = Companion.workspaceHelper

  @Singleton
  @Replaces(SourceHandler::class)
  fun sourceHandler(): SourceHandler = Companion.sourceHandler

  @Singleton
  @Replaces(WorkspacesHandler::class)
  fun workspacesHandler(): WorkspacesHandler = Companion.workspacesHandler

  @Singleton
  @Replaces(JobHistoryHandler::class)
  fun jobHistoryHandler(): JobHistoryHandler = Companion.jobHistoryHandler

  @Singleton
  @Replaces(OrganizationsHandler::class)
  fun organizationsHandler(): OrganizationsHandler = Companion.organizationsHandler

  @Singleton
  @Replaces(UserHandler::class)
  fun userHandler(): UserHandler = Companion.userHandler

  @Singleton
  @Replaces(UserPersistence::class)
  fun userPersistence(): UserPersistence = Companion.userPersistence

  companion object {
    val permissionHandler: PermissionHandler = mockk(relaxed = true)
    val workspaceHelper: WorkspaceHelper = mockk()
    val sourceHandler: SourceHandler = mockk()
    val workspacesHandler: WorkspacesHandler = mockk()
    val jobHistoryHandler: JobHistoryHandler = mockk()
    val organizationsHandler: OrganizationsHandler = mockk()
    val userHandler: UserHandler = mockk()
    val userPersistence: UserPersistence = mockk(relaxed = true)
  }
}
