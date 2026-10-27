/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectionState
import io.airbyte.api.server.generated.models.ConnectionStateType
import io.airbyte.commons.server.authorization.AuthenticationFactory
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.StateHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.data.auth.AirbyteJwtGenerator
import io.airbyte.data.auth.TokenType
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
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
import java.util.UUID

private const val SPEC = "ServiceAccountTokenHttpTest"

// Sends a real service-account token through JWT authentication and the shared rule; saved metadata and business work are mocked.
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class ServiceAccountTokenHttpTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var airbyteJwtGenerator: AirbyteJwtGenerator

  private val serviceAccountId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()

  @BeforeEach
  fun stub() {
    with(ServiceAccountTokenTestBeans) {
      clearMocks(permissionHandler, workspaceHelper, stateHandler, permissions, data)
      every { workspaceHelper.getWorkspaceForConnectionId(connectionId) } returns workspaceId
      every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
      val owner = WorkspaceAndOrganization(workspaceId, organizationId)
      every { data.savedConnectionActorIds(connectionId) } returns (sourceId to destinationId)
      every { data.savedSourceWorkspaceAndOrganization(sourceId) } returns owner
      every { data.savedDestinationWorkspaceAndOrganization(destinationId) } returns owner
      every { stateHandler.getState(any()) } returns ConnectionState(connectionId = connectionId, stateType = ConnectionStateType.NOT_SET)
    }
  }

  @Test
  fun `a service account token gets its roles from the service account's permissions`() {
    every { ServiceAccountTokenTestBeans.permissionHandler.getPermissionsByServiceAccountId(serviceAccountId) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withServiceAccountId(serviceAccountId))
    every { ServiceAccountTokenTestBeans.permissions.getPermissionsByServiceAccountId(serviceAccountId) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withServiceAccountId(serviceAccountId))

    assertEquals(HttpStatus.OK, getState(serviceAccountToken()))

    verify { ServiceAccountTokenTestBeans.permissionHandler.getPermissionsByServiceAccountId(serviceAccountId) }
    verify { ServiceAccountTokenTestBeans.permissions.getPermissionsByServiceAccountId(serviceAccountId) }
    verify(exactly = 0) { ServiceAccountTokenTestBeans.permissions.getPermissionsByAuthUserId(any()) }
    verify(exactly = 0) { ServiceAccountTokenTestBeans.permissionHandler.getPermissionsByAuthUserId(any()) }
  }

  @Test
  fun `a service account token with a bad signature is rejected`() {
    every { ServiceAccountTokenTestBeans.permissionHandler.getPermissionsByServiceAccountId(serviceAccountId) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withServiceAccountId(serviceAccountId))
    every { ServiceAccountTokenTestBeans.permissions.getPermissionsByServiceAccountId(serviceAccountId) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withServiceAccountId(serviceAccountId))
    val (header, payload, signature) = serviceAccountToken().split(".")
    // Change the first character. Changing the last one may not change the signature.
    val tampered = "$header.$payload.${if (signature[0] == 'A') 'B' else 'A'}${signature.substring(1)}"

    assertEquals(HttpStatus.UNAUTHORIZED, assertThrows<HttpClientResponseException> { getState(tampered) }.status)
    verify { ServiceAccountTokenTestBeans.stateHandler wasNot Called }
  }

  @Test
  fun `a service account without permissions is rejected`() {
    every { ServiceAccountTokenTestBeans.permissionHandler.getPermissionsByServiceAccountId(serviceAccountId) } returns emptyList()
    every { ServiceAccountTokenTestBeans.permissions.getPermissionsByServiceAccountId(serviceAccountId) } returns emptyList()

    assertEquals(HttpStatus.FORBIDDEN, assertThrows<HttpClientResponseException> { getState(serviceAccountToken()) }.status)
    verify { ServiceAccountTokenTestBeans.stateHandler wasNot Called }
  }

  private fun getState(token: String): HttpStatus =
    client
      .toBlocking()
      .exchange(
        HttpRequest.POST("/api/v1/state/get", mapOf("connectionId" to connectionId.toString())).bearerAuth(token),
        String::class.java,
      ).status

  private fun serviceAccountToken(): String = airbyteJwtGenerator.generateToken(serviceAccountId.toString(), TokenType.SERVICE_ACCOUNT, 10)
}

@Requires(property = "spec.name", value = SPEC)
@Factory
class ServiceAccountTokenTestBeans {
  // Another test in this module mocks RoleResolver, so build a real one here.
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
  @Replaces(AuthorizationDataService::class)
  fun data(): AuthorizationDataService = Companion.data

  @Singleton
  @Replaces(PermissionService::class)
  fun permissions(): PermissionService = Companion.permissions

  @Singleton
  @Replaces(OperationPermissions::class)
  fun operationPermissions(): OperationPermissions =
    OperationPermissions(Companion.permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)

  @Singleton
  @Replaces(PermissionHandler::class)
  fun permissionHandler(): PermissionHandler = Companion.permissionHandler

  @Singleton
  @Replaces(WorkspaceHelper::class)
  fun workspaceHelper(): WorkspaceHelper = Companion.workspaceHelper

  @Singleton
  @Replaces(StateHandler::class)
  fun stateHandler(): StateHandler = Companion.stateHandler

  companion object {
    val data: AuthorizationDataService = mockk()
    val permissions: PermissionService = mockk()
    val permissionHandler: PermissionHandler = mockk(relaxed = true)
    val workspaceHelper: WorkspaceHelper = mockk()
    val stateHandler: StateHandler = mockk()
  }
}
