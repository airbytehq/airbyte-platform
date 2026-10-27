/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.SlugRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.server.authorization.AuthenticationFactory
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.limits.ConsumptionService
import io.airbyte.commons.server.limits.ProductLimitsProvider
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HydrateLimits
import io.airbyte.featureflag.Workspace
import io.airbyte.server.apis.publicapi.services.WorkspaceServiceImpl
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
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.UUID

private const val WORKSPACE_ACCESS_SPEC = "WorkspaceOrganizationAccessHttpTest"
private const val AUTH_USER_ID = "workspace-reader"

/** Exercises the real handler and authorization scope lookup; only persistence-backed collaborators are replaced. */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = WORKSPACE_ACCESS_SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class WorkspaceOrganizationAccessHttpTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var jwtTokenGenerator: JwtTokenGenerator

  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val workspace =
    StandardWorkspace()
      .withWorkspaceId(workspaceId)
      .withCustomerId(UUID.randomUUID())
      .withOrganizationId(organizationId)
      .withDataplaneGroupId(UUID.randomUUID())
      .withName("Customer workspace")
      .withSlug("customer-workspace")
      .withInitialSetupComplete(false)
      .withTombstone(false)

  @BeforeEach
  fun stub() {
    with(WorkspaceOrganizationAccessTestBeans) {
      clearMocks(
        workspaceService,
        organizationService,
        permissionHandler,
        permissionService,
        actorRepository,
        connectionService,
        sourceService,
        destinationService,
        limitsProvider,
        consumptionService,
        ffClient,
      )
      every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, any()) } returns workspace
      every { workspaceService.getWorkspaceBySlug("customer-workspace", false) } returns workspace
      every { workspaceService.getStandardWorkspaceFromConnection(connectionId, any()) } returns workspace
      every { organizationService.getActiveOrganization(organizationId) } returns
        Optional.of(Organization().withOrganizationId(organizationId))
      every { connectionService.getStandardSync(connectionId) } returns
        StandardSync().withConnectionId(connectionId).withSourceId(sourceId).withDestinationId(destinationId)
      every { actorRepository.findSourceWorkspaceId(sourceId) } returns Optional.of(workspaceId)
      every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withWorkspaceId(workspaceId)
      every { destinationService.getDestinationConnection(destinationId) } returns DestinationConnection().withWorkspaceId(workspaceId)
      every { ffClient.boolVariation(HydrateLimits, Workspace(workspaceId)) } returns false
      grant(Permission.PermissionType.WORKSPACE_READER)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = ["ORGANIZATION_ADMIN", "WORKSPACE_READER"])
  fun `authorized readers can read workspaces in active organizations`(permission: Permission.PermissionType) {
    grant(permission)

    for ((path, body) in normalReads()) {
      val response = client.toBlocking().exchange(request(path, body), String::class.java)
      assertEquals(HttpStatus.OK, response.status, path)
      assertEquals(true, response.body()!!.contains(workspaceId.toString()), path)
    }
  }

  @ParameterizedTest
  @CsvSource("ORGANIZATION_ADMIN, true", "WORKSPACE_READER, true", "ORGANIZATION_ADMIN, false", "WORKSPACE_READER, false")
  fun `authorized readers receive not found when the workspace parent is inactive`(
    permission: Permission.PermissionType,
    parentExists: Boolean,
  ) {
    grant(permission)
    val organization = if (parentExists) Optional.of(Organization().withOrganizationId(organizationId)) else Optional.empty()
    every { WorkspaceOrganizationAccessTestBeans.organizationService.getOrganization(organizationId) } returns organization
    every { WorkspaceOrganizationAccessTestBeans.organizationService.getActiveOrganization(organizationId) } returns Optional.empty()

    assertAll(
      normalReads().map { (path, body) ->
        Executable { assertEquals(HttpStatus.NOT_FOUND, status(path, body), "$permission, parentExists=$parentExists, $path") }
      },
    )
    verify(exactly = 0) { WorkspaceOrganizationAccessTestBeans.ffClient.boolVariation(HydrateLimits, any()) }
    verify(exactly = 0) { WorkspaceOrganizationAccessTestBeans.limitsProvider.getLimitForWorkspace(any()) }
    verify(exactly = 0) { WorkspaceOrganizationAccessTestBeans.consumptionService.getForWorkspace(any()) }
  }

  @ParameterizedTest
  @ValueSource(strings = ["ORGANIZATION_ADMIN", "WORKSPACE_READER"])
  fun `explicit include tombstone reads remain available under deleted organizations`(permission: Permission.PermissionType) {
    grant(permission)
    every { WorkspaceOrganizationAccessTestBeans.organizationService.getActiveOrganization(organizationId) } returns Optional.empty()

    assertEquals(HttpStatus.OK, status("get", mapOf("workspaceId" to workspaceId, "includeTombstone" to true)))
    assertEquals(HttpStatus.OK, status("get_by_connection_id_with_tombstone", mapOf("connectionId" to connectionId)))
  }

  @ParameterizedTest
  @ValueSource(booleans = [false, true])
  fun `include tombstone does not grant access to another workspace`(includeTombstone: Boolean) {
    every { WorkspaceOrganizationAccessTestBeans.permissionHandler.getPermissionsByAuthUserId(AUTH_USER_ID) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN).withOrganizationId(UUID.randomUUID()))
    every { WorkspaceOrganizationAccessTestBeans.permissionService.getPermissionsByAuthUserId(AUTH_USER_ID) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN).withOrganizationId(UUID.randomUUID()))

    assertEquals(HttpStatus.FORBIDDEN, status("get", mapOf("workspaceId" to workspaceId, "includeTombstone" to includeTombstone)))
    assertEquals(
      HttpStatus.FORBIDDEN,
      status(if (includeTombstone) "get_by_connection_id_with_tombstone" else "get_by_connection_id", mapOf("connectionId" to connectionId)),
    )
  }

  @Test
  fun `parent lookup failures remain server errors and preserve the cause`() {
    val failure = IOException("organization lookup unavailable")
    every { WorkspaceOrganizationAccessTestBeans.organizationService.getActiveOrganization(organizationId) } throws failure

    assertAll(normalReads().map { (path, body) -> Executable { assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, status(path, body), path) } })
    val actual =
      assertThrows<AbstractThrowableProblem> {
        WorkspaceOrganizationAccessTestBeans.handler().getWorkspace(WorkspaceIdRequestBody(workspaceId))
      }
    assertEquals(500, actual.problem.getStatus())
    assertSame(failure, actual.cause)
  }

  @ParameterizedTest
  @ValueSource(booleans = [false, true])
  fun `missing and tombstoned workspaces retain not found behavior`(tombstoned: Boolean) {
    val failure = ConfigNotFoundException("workspace", workspaceId.toString())
    if (tombstoned) workspace.withTombstone(true)
    every { WorkspaceOrganizationAccessTestBeans.workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } throws failure
    every { WorkspaceOrganizationAccessTestBeans.workspaceService.getWorkspaceBySlug("customer-workspace", false) } throws failure
    every { WorkspaceOrganizationAccessTestBeans.workspaceService.getStandardWorkspaceFromConnection(connectionId, false) } throws failure
    val handler = WorkspaceOrganizationAccessTestBeans.handler()

    assertSame(failure, assertThrows<ConfigNotFoundException> { handler.getWorkspace(WorkspaceIdRequestBody(workspaceId)) })
    assertSame(failure, assertThrows<ConfigNotFoundException> { handler.getWorkspaceBySlug(SlugRequestBody("customer-workspace")) })
    assertSame(
      failure,
      assertThrows<ConfigNotFoundException> {
        handler.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId), false)
      },
    )
  }

  @Test
  fun `public API preserves not found for a workspace in a deleted organization`() {
    every { WorkspaceOrganizationAccessTestBeans.organizationService.getActiveOrganization(organizationId) } returns Optional.empty()
    val dataplaneGroupService = mockk<DataplaneGroupService>()
    every { dataplaneGroupService.getDataplaneGroup(workspace.dataplaneGroupId) } returns DataplaneGroup().withName("US")
    val service = WorkspaceServiceImpl(mockk(), mockk(), WorkspaceOrganizationAccessTestBeans.handler(), mockk(), mockk(), dataplaneGroupService)

    assertThrows<ResourceNotFoundProblem> { service.getWorkspace(workspaceId) }
  }

  @Test
  fun `public API reports parent database failures as server errors`() {
    val failure = IOException("organization lookup unavailable")
    every { WorkspaceOrganizationAccessTestBeans.organizationService.getActiveOrganization(organizationId) } throws failure
    val service = WorkspaceServiceImpl(mockk(), mockk(), WorkspaceOrganizationAccessTestBeans.handler(), mockk(), mockk(), mockk())

    val actual = assertThrows<AbstractThrowableProblem> { service.getWorkspace(workspaceId) }
    assertEquals(500, actual.problem.getStatus())
    assertSame(failure, actual.cause)
    assertFalse(
      actual.problem
        .getDetail()
        .orEmpty()
        .contains("organization lookup unavailable"),
    )
  }

  private fun grant(permission: Permission.PermissionType) {
    val granted = Permission().withPermissionType(permission)
    if (permission ==
      Permission.PermissionType.ORGANIZATION_ADMIN
    ) {
      granted.withOrganizationId(organizationId)
    } else {
      granted.withWorkspaceId(workspaceId)
    }
    every { WorkspaceOrganizationAccessTestBeans.permissionHandler.getPermissionsByAuthUserId(AUTH_USER_ID) } returns listOf(granted)
    every { WorkspaceOrganizationAccessTestBeans.permissionService.getPermissionsByAuthUserId(AUTH_USER_ID) } returns listOf(granted)
  }

  private fun normalReads(): List<Pair<String, Map<String, Any>>> =
    listOf(
      "get" to mapOf("workspaceId" to workspaceId),
      "get" to mapOf("workspaceId" to workspaceId, "includeTombstone" to false),
      // Slug alone is not an authorization scope; callers still need to identify the workspace they can access.
      "get_by_slug" to mapOf("slug" to "customer-workspace", "workspaceId" to workspaceId),
      "get_by_connection_id" to mapOf("connectionId" to connectionId),
    )

  private fun request(
    path: String,
    body: Map<String, Any>,
  ): HttpRequest<Map<String, Any>> {
    val token =
      jwtTokenGenerator
        .generateToken(
          mapOf(
            "iss" to "http://test-url.com",
            "aud" to "airbyte-server",
            "sub" to AUTH_USER_ID,
            "exp" to Instant.now().plus(10, ChronoUnit.MINUTES).epochSecond,
          ),
        ).orElseThrow()
    return HttpRequest.POST("/api/v1/workspaces/$path", body).bearerAuth(token)
  }

  private fun status(
    path: String,
    body: Map<String, Any>,
  ): HttpStatus =
    try {
      client.toBlocking().exchange(request(path, body), String::class.java).status
    } catch (e: HttpClientResponseException) {
      e.status
    }
}

@Requires(property = "spec.name", value = WORKSPACE_ACCESS_SPEC)
@Factory
class WorkspaceOrganizationAccessTestBeans {
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
  @Replaces(PermissionService::class)
  fun permissionService(): PermissionService = Companion.permissionService

  @Singleton
  @Replaces(OperationPermissions::class)
  fun operationPermissions(
    authenticationHeaderResolver: AuthenticationHeaderResolver,
    currentUserService: CurrentUserService,
    securityService: SecurityService?,
    permissionHandler: PermissionHandler,
  ): OperationPermissions =
    OperationPermissions(
      Companion.permissionService,
      RoleResolver(authenticationHeaderResolver, currentUserService, securityService, permissionHandler),
      true,
    )

  @Singleton
  @Replaces(WorkspaceService::class)
  fun workspaceService(): WorkspaceService = Companion.workspaceService

  @Singleton
  @Replaces(ConnectionService::class)
  fun connectionService(): ConnectionService = Companion.connectionService

  @Singleton
  @Replaces(ActorRepository::class)
  fun actorRepository(): ActorRepository = Companion.actorRepository

  @Singleton
  @Replaces(WorkspaceHelper::class)
  fun workspaceHelper(): WorkspaceHelper = Companion.workspaceHelper()

  @Singleton
  @Replaces(WorkspacesHandler::class)
  fun workspacesHandler(): WorkspacesHandler = handler()

  companion object {
    val permissionHandler: PermissionHandler = mockk(relaxed = true)
    val permissionService: PermissionService = mockk()
    val actorRepository: ActorRepository = mockk()
    val workspaceService: WorkspaceService = mockk()
    val organizationService: OrganizationService = mockk()
    val connectionService: ConnectionService = mockk()
    val sourceService: SourceService = mockk()
    val destinationService: DestinationService = mockk()
    val limitsProvider: ProductLimitsProvider = mockk()
    val consumptionService: ConsumptionService = mockk()
    val ffClient: FeatureFlagClient = mockk()

    fun workspaceHelper(): WorkspaceHelper = WorkspaceHelper(mockk(), connectionService, sourceService, destinationService, mockk(), workspaceService)

    fun handler(): WorkspacesHandler =
      WorkspacesHandler(
        workspacePersistence = mockk(),
        organizationService = organizationService,
        permissionHandler = permissionHandler,
        connectionsHandler = mockk(),
        destinationHandler = mockk(),
        sourceHandler = mockk(),
        uuidSupplier = mockk(),
        workspaceService = workspaceService,
        workspaceHelper = workspaceHelper(),
        dataplaneGroupService = mockk(),
        entitlementService = mockk(),
        trackingClient = mockk(),
        limitsProvider = limitsProvider,
        consumptionService = consumptionService,
        ffClient = ffClient,
        airbyteEdition = AirbyteEdition.CLOUD,
        roleResolver = mockk(),
        scimConfigurationService = mockk(),
        scimAccessGate = mockk(),
      )
  }
}
