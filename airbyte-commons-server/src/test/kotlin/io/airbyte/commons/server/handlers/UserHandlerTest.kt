/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.problems.throwable.generated.SSORequiredProblem
import io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem
import io.airbyte.api.server.generated.models.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationUserRead
import io.airbyte.api.server.generated.models.OrganizationUserReadList
import io.airbyte.api.server.generated.models.PermissionRead
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.api.server.generated.models.UserAuthIdRequestBody
import io.airbyte.api.server.generated.models.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.server.generated.models.UserWithPermissionInfoRead
import io.airbyte.api.server.generated.models.UserWithPermissionInfoReadList
import io.airbyte.api.server.generated.models.WorkspaceCreateWithId
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.api.server.generated.models.WorkspaceRead
import io.airbyte.api.server.generated.models.WorkspaceReadList
import io.airbyte.api.server.generated.models.WorkspaceUserAccessInfoRead
import io.airbyte.api.server.generated.models.WorkspaceUserAccessInfoReadList
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.config.InitialUserConfig
import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.RbacRolesEntitlement
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.config.Application
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthUser
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Organization
import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.config.Permission
import io.airbyte.config.SsoConfig
import io.airbyte.config.User
import io.airbyte.config.UserPermission
import io.airbyte.config.WorkspaceUserAccessInfo
import io.airbyte.config.helpers.AuthenticatedUserConverter
import io.airbyte.config.helpers.AuthenticatedUserConverter.toUser
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.scim.ScimFirstLoginAttachmentResult
import io.airbyte.domain.services.scim.ScimFirstLoginService
import io.airbyte.domain.services.sso.SsoRbacEntitlementChecker
import io.airbyte.featureflag.BypassSsoDomainValidationEnforcement
import io.airbyte.featureflag.ConfigurableSsoDefaultRole
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.RestrictLoginsForSSODomains
import io.airbyte.featureflag.TestClient
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.NullSource
import org.junit.jupiter.params.provider.ValueSource
import java.sql.Connection
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Stream
import io.airbyte.featureflag.Organization as FeatureFlagOrganization

class UserHandlerTest {
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var userHandler: UserHandler
  private lateinit var userPersistence: UserPersistence

  lateinit var permissionHandler: PermissionHandler
  lateinit var workspacesHandler: WorkspacesHandler
  lateinit var organizationService: OrganizationService
  lateinit var ssoConfigService: SsoConfigService
  lateinit var organizationEmailDomainService: OrganizationEmailDomainService
  lateinit var organizationsHandler: OrganizationsHandler
  lateinit var jwtUserAuthenticationResolver: JwtUserAuthenticationResolver
  lateinit var initialUserConfig: InitialUserConfig
  lateinit var externalUserService: ExternalUserService
  lateinit var applicationService: ApplicationService
  lateinit var featureFlagClient: FeatureFlagClient
  lateinit var ssoRbacEntitlementChecker: SsoRbacEntitlementChecker
  lateinit var scimFirstLoginService: ScimFirstLoginService
  private var transactionCallbackActive = false
  private val transactionOperations =
    object : TransactionOperations<Connection> {
      override fun getConnection(): Connection = mockk()

      override fun hasConnection(): Boolean = false

      override fun findTransactionStatus(): Optional<out TransactionStatus<*>> = Optional.empty()

      override fun <R> execute(
        definition: TransactionDefinition,
        callback: TransactionCallback<Connection, R>,
      ): R {
        transactionCallbackActive = true
        return try {
          callback.call(mockk())
        } finally {
          transactionCallbackActive = false
        }
      }
    }

  private val user: AuthenticatedUser =
    AuthenticatedUser()
      .withUserId(userId)
      .withAuthUserId(userId.toString())
      .withEmail(USER_EMAIL)
      .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
      .withStatus(User.Status.INVITED)
      .withName(USER_NAME)
  private lateinit var resourceBootstrapHandler: ResourceBootstrapHandler

  @BeforeEach
  fun setUp() {
    userPersistence = mockk()
    permissionHandler = mockk()
    workspacesHandler = mockk()
    organizationService = mockk()
    ssoConfigService = mockk()
    organizationEmailDomainService = mockk()
    organizationsHandler = mockk()
    uuidSupplier = mockk()
    jwtUserAuthenticationResolver = mockk()
    initialUserConfig = mockk(relaxed = true)
    resourceBootstrapHandler = mockk()
    externalUserService = mockk()
    applicationService = mockk()
    featureFlagClient = mockk<TestClient>()
    ssoRbacEntitlementChecker = mockk()
    scimFirstLoginService = mockk()

    every { featureFlagClient.boolVariation(any(), any()) } returns false
    every { jwtUserAuthenticationResolver.resolveRealm() } returns null
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganizationBySsoConfigRealm(any()) } returns Optional.empty()
    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { scimFirstLoginService.isScimManagedUser(any()) } returns false
    every { userPersistence.getUser(any()) } returns Optional.empty()
    every { userPersistence.getUserByAuthId(any()) } returns Optional.empty()
    every { userPersistence.getUserByEmail(any()) } returns Optional.empty()
    every { userPersistence.listAuthUsersForUser(any()) } returns emptyList()
    every { applicationService.deletesApplicationsTransactionally() } returns false
    every { userPersistence.writeUser(any<User>()) } just Runs
    every { userPersistence.writeAuthenticatedUser(any<AuthenticatedUser>()) } just Runs
    every { permissionHandler.createPermission(any()) } returns Permission()
    every { permissionHandler.grantInstanceAdmin(any()) } just Runs
    every { permissionHandler.listPermissionsForUser(any()) } returns emptyList()
    every { externalUserService.deleteUserByExternalId(any(), any()) } just Runs
    every { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) } just Runs
    every { applicationService.listApplicationsByUser(any()) } returns emptyList()
    every { applicationService.deleteApplication(any(), any()) } returns Application()
    every { featureFlagClient.boolVariation(RestrictLoginsForSSODomains, any()) } returns true
    // ConfigurableSsoDefaultRole defaults ON in tests so existing assertions exercise the configured
    // role; prod default is OFF (dark launch). Flag-off behavior is covered explicitly below.
    every { featureFlagClient.boolVariation(ConfigurableSsoDefaultRole, any()) } returns true
    every { ssoRbacEntitlementChecker.check(any()) } returns EntitlementResult(RbacRolesEntitlement.featureId, true)
    every { featureFlagClient.boolVariation(BypassSsoDomainValidationEnforcement, any()) } returns true

    // SEC-14: Most tests use an org without claimed domains and the bypass enabled above, preserving
    // their pre-enforcement behavior. Domain-enforcement tests override both inputs explicitly.
    every { organizationEmailDomainService.findByOrganizationId(any()) } returns emptyList()
    every {
      scimFirstLoginService.attachIfPreProvisioned(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns ScimFirstLoginAttachmentResult.NoMatch
    every { userPersistence.createAuthenticatedUserIfNoScimMapping(any()) } returns true
    every { userPersistence.writeAuthUser(any(), any(), any()) } returns true
    every { userPersistence.replaceAuthUserForUserId(any(), any(), any()) } returns true
    every { userPersistence.enableAgenticUser(any(), any()) } answers { secondArg() }

    userHandler =
      UserHandler(
        userPersistence,
        externalUserService,
        organizationService,
        ssoConfigService,
        organizationEmailDomainService,
        Optional.of(applicationService),
        permissionHandler,
        workspacesHandler,
        uuidSupplier,
        jwtUserAuthenticationResolver,
        Optional.of(initialUserConfig),
        resourceBootstrapHandler,
        featureFlagClient,
        ssoRbacEntitlementChecker,
        scimFirstLoginService,
        transactionOperations,
      )
  }

  @Test
  fun `verified current SCIM mapping attaches login to the mapped User without bootstrap side effects`() {
    val authUserId = "first-login-auth-user"
    val mappedUserId = UUID.randomUUID()
    val currentMappingEmail = "current-mapping@example.com"
    val staleGlobalEmail = "stale-global@example.com"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(currentMappingEmail)
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    val attachedUser =
      AuthenticatedUser()
        .withUserId(mappedUserId)
        .withEmail(staleGlobalEmail)
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns currentMappingEmail
    every { userPersistence.getUserByAuthId(authUserId) } returnsMany listOf(Optional.empty(), Optional.of(attachedUser))
    every {
      scimFirstLoginService.attachIfPreProvisioned(
        currentMappingEmail,
        currentMappingEmail,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      )
    } returns ScimFirstLoginAttachmentResult.Attached(mappedUserId)

    val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = authUserId))

    Assertions.assertEquals(false, result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertEquals(staleGlobalEmail, result.userRead.email)
    verify(exactly = 0) { userPersistence.getUserByEmail(any()) }
    verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }
    verify(exactly = 0) { userPersistence.replaceAuthUserForUserId(any(), any(), any()) }
    verify { resourceBootstrapHandler wasNot Called }
  }

  @Test
  fun `new SCIM attachment upgrades the mapped User when agentic login is requested`() {
    val authUserId = "agentic-first-login-auth-user"
    val mappedUserId = UUID.randomUUID()
    val email = "agentic-mapped@example.com"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(email)
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    val attachedUser =
      AuthenticatedUser()
        .withUserId(mappedUserId)
        .withEmail("stale-global@example.com")
        .withName("Mapped User")
        .withStatus(User.Status.INVITED)
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns email
    every { userPersistence.getUserByAuthId(authUserId) } returnsMany listOf(Optional.empty(), Optional.of(attachedUser))
    every {
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      )
    } returns ScimFirstLoginAttachmentResult.Attached(mappedUserId)

    val result =
      userHandler.getOrCreateUserByAuthId(
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = true),
      )

    Assertions.assertEquals(false, result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertNotNull(result.userRead.agenticEnabledAt)
    verify(exactly = 1) { userPersistence.enableAgenticUser(mappedUserId, result.userRead.agenticEnabledAt!!) }
    verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }
    verify(exactly = 0) { userPersistence.getUserByEmail(any()) }
    verify {
      permissionHandler wasNot Called
      workspacesHandler wasNot Called
      resourceBootstrapHandler wasNot Called
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(booleans = [false])
  fun `new SCIM attachment preserves non-agentic mapped User without bootstrap side effects`(isAgenticUser: Boolean?) {
    val authUserId = "non-agentic-first-login-auth-user"
    val mappedUserId = UUID.randomUUID()
    val email = "non-agentic-mapped@example.com"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(email)
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    val attachedUser =
      AuthenticatedUser()
        .withUserId(mappedUserId)
        .withEmail("stale-global@example.com")
        .withName("Mapped User")
        .withStatus(User.Status.INVITED)
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns email
    every { userPersistence.getUserByAuthId(authUserId) } returnsMany listOf(Optional.empty(), Optional.of(attachedUser))
    every {
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      )
    } returns ScimFirstLoginAttachmentResult.Attached(mappedUserId)
    val request = UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = isAgenticUser)

    val result = userHandler.getOrCreateUserByAuthId(request)

    Assertions.assertEquals(false, result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertEquals(io.airbyte.api.server.generated.models.UserStatus.INVITED, result.userRead.status)
    Assertions.assertNull(result.userRead.agenticEnabledAt)
    verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }
    verify(exactly = 0) { userPersistence.getUserByEmail(any()) }
    verify {
      permissionHandler wasNot Called
      workspacesHandler wasNot Called
      resourceBootstrapHandler wasNot Called
    }
  }

  @Test
  fun `agentic upgrade for an already linked identity bypasses SCIM and broad persistence`() {
    val authUserId = "existing-mapped-auth-user"
    val mappedUserId = UUID.randomUUID()
    val persistedAgenticEnabledAt = java.time.OffsetDateTime.parse("2026-07-29T12:00:00Z")
    val email = "mapped-existing@example.com"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(email)
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    val existingUser =
      AuthenticatedUser()
        .withUserId(mappedUserId)
        .withEmail("stale-global@example.com")
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns email
    every { userPersistence.getUserByAuthId(authUserId) } returns Optional.of(existingUser)
    every { userPersistence.enableAgenticUser(mappedUserId, any()) } returns persistedAgenticEnabledAt

    val result =
      userHandler.getOrCreateUserByAuthId(
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = true),
      )

    Assertions.assertEquals(false, result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertEquals(persistedAgenticEnabledAt, result.userRead.agenticEnabledAt)
    verify { scimFirstLoginService wasNot Called }
    verify(exactly = 0) { userPersistence.getUserByEmail(any()) }
    verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }
    verify(exactly = 1) { userPersistence.enableAgenticUser(mappedUserId, any()) }
    verify(exactly = 0) { userPersistence.replaceAuthUserForUserId(any(), any(), any()) }
  }

  @Test
  fun `matching SCIM mapping with unverified email fails before duplicate User creation`() {
    val authUserId = "unverified-first-login"
    val email = "mapped@example.com"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(email)
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns null
    every { userPersistence.getUserByAuthId(authUserId) } returns Optional.empty()
    every {
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        null,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      )
    } returns ScimFirstLoginAttachmentResult.EmailNotVerified

    Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
      userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = authUserId))
    }

    verify(exactly = 0) { userPersistence.getUserByEmail(any()) }
    verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }
    verify(exactly = 0) { userPersistence.replaceAuthUserForUserId(any(), any(), any()) }
  }

  @Test
  fun `ambiguous raw-subject ownership remains on the fail-closed attachment path`() {
    val authUserId = "ambiguous-first-login"
    val email = "ambiguous@example.com"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(email)
        .withName("Ambiguous User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns email
    every { userPersistence.getUserByAuthId(authUserId) } returns Optional.empty()
    every {
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      )
    } returns ScimFirstLoginAttachmentResult.AmbiguousIdentity

    Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
      userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = authUserId))
    }

    verify(exactly = 0) { userPersistence.getUserByEmail(any()) }
    verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }
    verify(exactly = 0) { userPersistence.replaceAuthUserForUserId(any(), any(), any()) }
  }

  @Test
  fun `conflicting SCIM first-login resolution fails without normal login writes`() {
    val authUserId = "conflicting-first-login"
    val email = "mapped@example.com"
    val incomingUser =
      AuthenticatedUser()
        .withEmail(email)
        .withName("Mapped User")
        .withAuthUserId(authUserId)
        .withAuthProvider(AuthProvider.KEYCLOAK)
    every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
    every { jwtUserAuthenticationResolver.resolveVerifiedEmail() } returns email
    every { userPersistence.getUserByAuthId(authUserId) } returns Optional.empty()
    every {
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      )
    } returns ScimFirstLoginAttachmentResult.Conflict

    Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
      userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = authUserId))
    }

    verify(exactly = 0) { userPersistence.getUserByEmail(any()) }
    verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }
    verify(exactly = 0) { userPersistence.replaceAuthUserForUserId(any(), any(), any()) }
  }

  @Test
  fun testListUsersInOrg() {
    val organizationId = UUID.randomUUID()
    val userID = UUID.randomUUID()

    // expecting the default user to be excluded from the response
    val defaultUserPermission =
      UserPermission()
        .withUser(User().withName("default").withUserId(DEFAULT_USER_ID).withEmail("default@airbyte.io"))
        .withPermission(Permission().withPermissionId(UUID.randomUUID()).withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))

    val realUserPermission =
      UserPermission()
        .withUser(User().withName(USER_NAME).withUserId(userID).withEmail(USER_EMAIL))
        .withPermission(Permission().withPermissionId(permission1Id).withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))

    every { permissionHandler.listUsersInOrganization(organizationId) } returns listOf<UserPermission>(defaultUserPermission, realUserPermission)

    // no default user present
    val expectedListResult =
      OrganizationUserReadList(
        users =
          listOf<OrganizationUserRead>(
            OrganizationUserRead(
              name = USER_NAME,
              userId = userID,
              email = USER_EMAIL,
              organizationId = organizationId,
              permissionId = permission1Id,
              permissionType = PermissionType.ORGANIZATION_ADMIN,
            ),
          ),
      )

    val result = userHandler.listUsersInOrganization(OrganizationIdRequestBody(organizationId = organizationId))
    Assertions.assertEquals(expectedListResult, result)
  }

  @Test
  fun testListInstanceAdminUser() {
    every { permissionHandler.listInstanceAdminUsers() } returns
      listOf<UserPermission>(
        UserPermission()
          .withUser(
            User().withName(USER_NAME).withUserId(userId).withEmail(USER_EMAIL),
          ).withPermission(Permission().withPermissionId(permission1Id).withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)),
      )

    val result = userHandler.listInstanceAdminUsers()

    val expectedResult =
      UserWithPermissionInfoReadList(
        users =
          listOf<UserWithPermissionInfoRead>(
            UserWithPermissionInfoRead(
              name = USER_NAME,
              userId = userId,
              email = USER_EMAIL,
              permissionId = permission1Id,
            ),
          ),
      )
    Assertions.assertEquals(expectedResult, result)
  }

  @Test
  fun testListAccessInfoByWorkspaceId() {
    val workspaceId = UUID.randomUUID()
    every { userPersistence.listWorkspaceUserAccessInfo(workspaceId) } returns
      listOf<WorkspaceUserAccessInfo>(
        WorkspaceUserAccessInfo()
          .withUserId(DEFAULT_USER_ID), // expect the default user to be filtered out.
        WorkspaceUserAccessInfo()
          .withUserId(userId)
          .withUserName(USER_NAME)
          .withUserEmail(USER_EMAIL)
          .withWorkspaceId(workspaceId)
          .withWorkspacePermission(
            Permission()
              .withPermissionId(permission1Id)
              .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
              .withUserId(userId)
              .withWorkspaceId(workspaceId),
          ),
      )

    val result = userHandler.listAccessInfoByWorkspaceId(WorkspaceIdRequestBody(workspaceId = workspaceId))

    val expected =
      WorkspaceUserAccessInfoReadList(
        usersWithAccess =
          listOf<WorkspaceUserAccessInfoRead>(
            WorkspaceUserAccessInfoRead(
              userId = userId,
              userName = USER_NAME,
              userEmail = USER_EMAIL,
              workspaceId = workspaceId,
              workspacePermission =
                PermissionRead(
                  permissionId = permission1Id,
                  permissionType = PermissionType.WORKSPACE_ADMIN,
                  userId = userId,
                  workspaceId = workspaceId,
                ),
            ),
          ),
      )

    Assertions.assertEquals(expected, result)
  }

  @Nested
  internal inner class GetOrCreateUserByAuthIdTest {
    @Test
    fun testAgenticFeaturesEnabledDuringUserCreation() {
      val authUserId = "test-auth-user-id"
      val newUserId = UUID.randomUUID()

      val jwtUser =
        AuthenticatedUser()
          .withEmail("agentic@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)

      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns jwtUser
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.empty()
      every { uuidSupplier.get() } returns newUserId

      val createdUser =
        User()
          .withUserId(newUserId)
          .withEmail("agentic@test.com")
          .withAgenticEnabledAt(java.time.OffsetDateTime.now())
      every { userPersistence.getUser(newUserId) } returns Optional.of(createdUser)

      val defaultWorkspace = testWorkspaceRead(UUID.randomUUID())
      every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any()) } returns defaultWorkspace

      val requestBody =
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = true)

      userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that the user was written with agenticEnabledAt set to a timestamp
      verify(exactly = 1) {
        userPersistence.createAuthenticatedUserIfNoScimMapping(
          match { user: AuthenticatedUser? ->
            user!!.agenticEnabledAt != null
          },
        )
      }
    }

    @Test
    fun testNewUserWithoutAgenticFlagCreatesNonAgenticUser() {
      val authUserId = "test-auth-user-id"
      val newUserId = UUID.randomUUID()

      val jwtUser =
        AuthenticatedUser()
          .withEmail("nonagentic@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)

      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns jwtUser
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.empty()
      every { uuidSupplier.get() } returns newUserId

      val createdUser =
        User()
          .withUserId(newUserId)
          .withEmail("nonagentic@test.com")
          .withAgenticEnabledAt(null)
      every { userPersistence.getUser(newUserId) } returns Optional.of(createdUser)

      val defaultWorkspace = testWorkspaceRead(UUID.randomUUID())
      every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any()) } returns defaultWorkspace

      val requestBody =
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = false)

      userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that the user was written with agenticEnabledAt = null
      verify(exactly = 1) {
        userPersistence.createAuthenticatedUserIfNoScimMapping(
          match { user: AuthenticatedUser? ->
            user!!.agenticEnabledAt == null
          },
        )
      }
    }

    @Test
    fun testExistingNonAgenticUserUpgradedWhenFlagIsTrue() {
      val authUserId = "test-auth-user-id"
      val userId = UUID.randomUUID()
      val originalTimestamp = java.time.OffsetDateTime.now()

      val jwtUser =
        AuthenticatedUser()
          .withEmail("upgrade@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)

      val existingNonAgenticUser =
        AuthenticatedUser()
          .withUserId(userId)
          .withEmail("upgrade@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)
          .withAgenticEnabledAt(null) // Non-agentic user

      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns jwtUser
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.of(existingNonAgenticUser)

      val requestBody =
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = true) // Request to upgrade

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      verify(exactly = 1) { userPersistence.enableAgenticUser(userId, response.userRead.agenticEnabledAt!!) }
      verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }

      // Verify response indicates existing user (not new)
      Assertions.assertEquals(false, response.newUserCreated)
    }

    @Test
    fun testExistingNonAgenticUserStaysNonAgenticWhenFlagIsFalse() {
      val authUserId = "test-auth-user-id"
      val userId = UUID.randomUUID()

      val jwtUser =
        AuthenticatedUser()
          .withEmail("stay@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)

      val existingNonAgenticUser =
        AuthenticatedUser()
          .withUserId(userId)
          .withEmail("stay@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)
          .withAgenticEnabledAt(null)

      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns jwtUser
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.of(existingNonAgenticUser)

      val requestBody =
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = false)

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that writeAuthenticatedUser was NOT called (no upgrade)
      verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }

      // Verify user returned is non-agentic
      Assertions.assertNull(response.userRead.agenticEnabledAt)
    }

    @Test
    fun testExistingAgenticUserPreservesTimestampWhenFlagIsTrue() {
      val authUserId = "test-auth-user-id"
      val userId = UUID.randomUUID()
      val originalTimestamp = java.time.OffsetDateTime.of(2024, 1, 15, 10, 0, 0, 0, java.time.ZoneOffset.UTC)

      val jwtUser =
        AuthenticatedUser()
          .withEmail("preserve@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)

      val existingAgenticUser =
        AuthenticatedUser()
          .withUserId(userId)
          .withEmail("preserve@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)
          .withAgenticEnabledAt(originalTimestamp) // Already agentic

      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns jwtUser
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.of(existingAgenticUser)

      val requestBody =
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = true)

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that writeAuthenticatedUser was NOT called (timestamp immutable)
      verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }

      // Verify original timestamp preserved
      Assertions.assertEquals(originalTimestamp, response.userRead.agenticEnabledAt)
    }

    @Test
    fun testExistingAgenticUserCannotBeDowngraded() {
      val authUserId = "test-auth-user-id"
      val userId = UUID.randomUUID()
      val originalTimestamp = java.time.OffsetDateTime.of(2024, 1, 15, 10, 0, 0, 0, java.time.ZoneOffset.UTC)

      val jwtUser =
        AuthenticatedUser()
          .withEmail("nodowngrade@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)

      val existingAgenticUser =
        AuthenticatedUser()
          .withUserId(userId)
          .withEmail("nodowngrade@test.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)
          .withAgenticEnabledAt(originalTimestamp)

      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns jwtUser
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.of(existingAgenticUser)

      val requestBody =
        UserAuthIdRequestBody(authUserId = authUserId, isAgenticUser = false) // Attempt to downgrade

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that writeAuthenticatedUser was NOT called (cannot downgrade)
      verify(exactly = 0) { userPersistence.writeAuthenticatedUser(any()) }

      // Verify timestamp still present (downgrade rejected)
      Assertions.assertEquals(originalTimestamp, response.userRead.agenticEnabledAt)
    }

    @ParameterizedTest
    @EnumSource(AuthProvider::class)
    fun authIdExists(authProvider: AuthProvider) {
      // set the auth provider for the existing user to match the test case
      user.authProvider = authProvider

      // authUserId is for the existing user
      val authUserId = user.authUserId
      val apiAuthProvider =
        authProvider.convertTo<io.airbyte.api.server.generated.models.AuthProvider>()

      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns user
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.of<AuthenticatedUser>(user)

      val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = authUserId))
      val userRead = response.userRead

      Assertions.assertEquals(userRead.userId, userId)
      Assertions.assertEquals(userRead.email, USER_EMAIL)
      Assertions.assertEquals(response.authUserId, authUserId)
      Assertions.assertEquals(response.authProvider, apiAuthProvider)
    }

    @Test
    fun `existing identity resolves previous Keycloak users after the initial transaction`() {
      val authUserId = "current-auth-user"
      val previousAuthUserId = "previous-auth-user"
      val existingUserId = UUID.randomUUID()
      val incomingUser =
        AuthenticatedUser()
          .withUserId(existingUserId)
          .withEmail("existing@example.com")
          .withAuthUserId(authUserId)
          .withAuthProvider(AuthProvider.KEYCLOAK)
      val authUsers =
        listOf(
          AuthUser()
            .withUserId(existingUserId)
            .withAuthUserId(authUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK),
          AuthUser()
            .withUserId(existingUserId)
            .withAuthUserId(previousAuthUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK),
        )
      every { jwtUserAuthenticationResolver.resolveUser(authUserId) } returns incomingUser
      every { userPersistence.getUserByAuthId(authUserId) } returns Optional.of(incomingUser)
      every { userPersistence.listAuthUsersForUser(existingUserId) } returns authUsers
      every { externalUserService.getRealmByAuthUserId(previousAuthUserId) } answers {
        Assertions.assertFalse(transactionCallbackActive)
        null
      }

      val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = authUserId))

      Assertions.assertEquals(false, response.newUserCreated)
      Assertions.assertEquals(existingUserId, response.userRead.userId)
      verify(exactly = 1) { userPersistence.replaceAuthUserForUserId(existingUserId, authUserId, AuthProvider.KEYCLOAK) }
    }

    @Nested
    internal inner class ExistingEmailTest {
      private var jwtUser: AuthenticatedUser? = null
      private var existingUser: User? = null

      @BeforeEach
      fun setup() {
        jwtUser = AuthenticatedUser().withEmail(email).withAuthUserId(newAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK)
        existingUser = User().withUserId(existingUserId).withEmail(email)
      }

      // the user persisted for the incoming JWT; unlike the JWT user it has a user id
      private fun persistedJwtUser(): AuthenticatedUser =
        AuthenticatedUser()
          .withUserId(existingUserId)
          .withEmail(jwtUser!!.email)
          .withAuthUserId(jwtUser!!.authUserId)
          .withAuthProvider(jwtUser!!.authProvider)

      @ParameterizedTest
      @CsvSource("true", "false")
      fun testNonSSOSignInEmailExistsThrowsError(isExistingUserSSO: Boolean) {
        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.empty<AuthenticatedUser>()
        every { userPersistence.getUserByEmail(email) } returns Optional.of<User>(existingUser!!)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returns
          listOf<AuthUser>(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK))
        every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns realm

        if (isExistingUserSSO) {
          every { ssoConfigService.getSsoConfigByRealmName(realm) } returns SsoConfig()
        }

        Assertions.assertThrows(
          UserAlreadyExistsProblem::class.java,
        ) { userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId)) }
      }

      @Test
      fun testExistingDefaultUserWithEmailUpdatesDefault() {
        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.empty<AuthenticatedUser>()

        // A separate UUID object, like one loaded from the database.
        val defaultUser = User().withUserId(UUID.fromString(DEFAULT_USER_ID.toString())).withEmail(email)
        every { userPersistence.getUserByEmail(email) } returns Optional.of(defaultUser)

        val newUser =
          AuthenticatedUser()
            .withUserId(UUID.randomUUID())
            .withEmail(email)
            .withAuthUserId(newAuthUserId)
            .withDefaultWorkspaceId(UUID.randomUUID())
        every { uuidSupplier.get() } returns newUser.userId
        every { userPersistence.getUser(newUser.userId) } returns Optional.of<User>(toUser(newUser))

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        Assertions.assertEquals(true, res.newUserCreated)
        Assertions.assertEquals(res.userRead.userId, newUser.userId)
        Assertions.assertEquals(res.userRead.email, email)
        Assertions.assertEquals(res.authUserId, newAuthUserId)

        verify(exactly = 1) { userPersistence.writeUser(defaultUser.withEmail("")) }
        verify(exactly = 1) {
          userPersistence.createAuthenticatedUserIfNoScimMapping(
            match { user: AuthenticatedUser? ->
              user!!.email ==
                jwtUser!!.email &&
                user.authUserId == jwtUser!!.authUserId
            },
          )
        }
      }

      @Test
      fun testRelinkOrphanedUser() {
        // Auth user in JWT is not linked to any user in the database
        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.empty<AuthenticatedUser>()

        // A user with the same email exists in the database
        every { userPersistence.getUserByEmail(email) } returns Optional.of<User>(existingUser!!)
        every { userPersistence.getUser(existingUserId) } returns Optional.of<User>(existingUser!!)

        // None of the auth users configured for the existing user actually exist in the external user
        // service
        val existingAuthUsers =
          listOf(
            AuthUser()
              .withUserId(existingUserId)
              .withAuthUserId(existingAuthUserId)
              .withAuthProvider(AuthProvider.KEYCLOAK),
          )
        val pendingRelinkAuthUsers =
          existingAuthUsers +
            AuthUser()
              .withUserId(existingUserId)
              .withAuthUserId(newAuthUserId)
              .withAuthProvider(AuthProvider.KEYCLOAK)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returnsMany
          listOf(existingAuthUsers, existingAuthUsers, pendingRelinkAuthUsers)
        every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } answers {
          Assertions.assertFalse(transactionCallbackActive)
          null
        }

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        Assertions.assertEquals(false, res.newUserCreated)
        Assertions.assertEquals(res.userRead.userId, existingUserId)

        // Verify the incoming identity is staged before the old identity is removed.
        verify(exactly = 1) { userPersistence.writeAuthUser(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }
        verify(exactly = 1) { userPersistence.replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }
        // Only the default user's email gets cleared on first login.
        verify(exactly = 0) { userPersistence.writeUser(match { user: User? -> user?.email == "" }) }
      }

      @Test
      fun testRelinkOrphanedUserRejectsAuthenticationIdentityOwnedByAnotherUser() {
        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.empty<AuthenticatedUser>()
        every { userPersistence.getUserByEmail(email) } returns Optional.of<User>(existingUser!!)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returns
          listOf(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK))
        every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns null
        every {
          scimFirstLoginService.attachIfPreProvisioned(
            email,
            null,
            newAuthUserId,
            AuthProvider.KEYCLOAK,
            null,
            null,
          )
        } returns ScimFirstLoginAttachmentResult.NoMatch
        every {
          scimFirstLoginService.attachIfPreProvisioned(
            email,
            null,
            newAuthUserId,
            AuthProvider.KEYCLOAK,
            null,
            existingUserId,
          )
        } returns ScimFirstLoginAttachmentResult.ExistingIdentity(UUID.randomUUID())

        Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        }

        verify(exactly = 0) { userPersistence.getUser(existingUserId) }
      }

      @Test
      fun `email match fails closed when authentication identities change during realm resolution`() {
        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.empty<AuthenticatedUser>()
        every { userPersistence.getUserByEmail(email) } returns Optional.of(existingUser!!)
        every { userPersistence.getUser(existingUserId) } returns Optional.of(existingUser!!)

        val existingAuthUser =
          AuthUser()
            .withUserId(existingUserId)
            .withAuthUserId(existingAuthUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK)
        val concurrentAuthUser =
          AuthUser()
            .withUserId(existingUserId)
            .withAuthUserId("concurrent-auth-user")
            .withAuthProvider(AuthProvider.KEYCLOAK)
        val incomingAuthUser =
          AuthUser()
            .withUserId(existingUserId)
            .withAuthUserId(newAuthUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK)
        var authUserListReads = 0
        every { userPersistence.listAuthUsersForUser(existingUserId) } answers {
          authUserListReads += 1
          when (authUserListReads) {
            1 -> listOf(existingAuthUser)
            2, 3 -> listOf(existingAuthUser, concurrentAuthUser)
            else -> listOf(existingAuthUser, concurrentAuthUser, incomingAuthUser)
          }
        }
        every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns null

        Assertions.assertThrows(IllegalStateException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        }

        verify(exactly = 0) { userPersistence.writeAuthUser(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }
        verify(exactly = 0) { userPersistence.replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }
      }

      @ParameterizedTest
      @MethodSource("io.airbyte.commons.server.handlers.UserHandlerTest#ssoSignInArgsProvider")
      fun testSSOSignInEmailExistsMigratesAuthUser(
        isExistingUserSSO: Boolean,
        doesExistingUserHaveOrgPermission: Boolean,
      ) {
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns
          Optional.of<Organization>(
            organization,
          )
        // SEC-14: The email domain must be claimed by the SSO org for migration to proceed
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns
          listOf(
            OrganizationEmailDomain()
              .withOrganizationId(organization.organizationId)
              .withEmailDomain("airbyte.io"),
          )

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.empty<AuthenticatedUser>()
        every { userPersistence.getUserByEmail(email) } returns Optional.of<User>(existingUser!!)
        every { userPersistence.getUser(existingUserId) } returns Optional.of<User>(existingUser!!)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returns
          listOf<AuthUser>(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK))

        if (isExistingUserSSO) {
          every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns ssoRealm
          every { ssoConfigService.getSsoConfigByRealmName(ssoRealm) } returns SsoConfig()

          Assertions.assertThrows(
            UserAlreadyExistsProblem::class.java,
          ) { userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId)) }
          return
        }

        every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns realm
        every { ssoConfigService.getSsoConfigByRealmName(realm) } returns null

        val existingAuthUsers =
          listOf(
            AuthUser()
              .withUserId(existingUserId)
              .withAuthUserId(existingAuthUserId)
              .withAuthProvider(AuthProvider.KEYCLOAK),
          )
        val pendingMigrationAuthUsers =
          existingAuthUsers +
            AuthUser()
              .withUserId(existingUserId)
              .withAuthUserId(newAuthUserId)
              .withAuthProvider(AuthProvider.KEYCLOAK)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returnsMany
          listOf(existingAuthUsers, existingAuthUsers, pendingMigrationAuthUsers)

        val existingAuthedUser =
          AuthenticatedUserConverter.toAuthenticatedUser(existingUser!!, existingAuthUserId, AuthProvider.KEYCLOAK)

        every { applicationService.listApplicationsByUser(existingAuthedUser) } returns
          listOf<Application>(
            Application().withId("app_id"),
          )
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every {
          workspacesHandler
            .listWorkspacesInOrganization(ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId))
        } returns
          WorkspaceReadList(
            workspaces = listOf(testWorkspaceRead(UUID.randomUUID())),
          )

        if (doesExistingUserHaveOrgPermission) {
          every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
            listOf<UserPermission>(UserPermission().withUser(existingUser))
        } else {
          every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
            listOf<UserPermission>(UserPermission().withUser(User().withUserId(UUID.randomUUID())))
        }

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        Assertions.assertEquals(false, res.newUserCreated)

        // verify apps are revoked
        verify(exactly = 1) { applicationService.deleteApplication(existingAuthedUser, "app_id") }

        // verify auth user is replaced
        verify(exactly = 1) { userPersistence.replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }

        // verify old auth user is deleted from other realms
        verify(exactly = 1) { externalUserService.deleteUserByEmailOnOtherRealms(email, ssoRealm) }

        // verify org permission is created (if it doesn't already exist)
        if (!doesExistingUserHaveOrgPermission) {
          verify(exactly = 1) {
            permissionHandler.createPermission(
              Permission()
                .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
                .withOrganizationId(organization.organizationId)
                .withUserId(existingUserId),
            )
          }
        }

        // verify user read
        val userRead = res.userRead
        Assertions.assertEquals(userRead.userId, existingUserId)
        Assertions.assertEquals(userRead.email, email)
      }

      @Test
      fun `SSO login with only a legacy google_identity_platform previous identity does not sweep other realms`() {
        val incomingUser =
          AuthenticatedUser()
            .withUserId(existingUserId)
            .withEmail(email)
            .withAuthUserId(newAuthUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK)

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns incomingUser
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of(incomingUser)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returns
          listOf(
            AuthUser().withUserId(existingUserId).withAuthUserId(newAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK),
            AuthUser()
              .withUserId(existingUserId)
              .withAuthUserId(existingAuthUserId)
              .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM),
          )
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of(organization)

        val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        Assertions.assertEquals(false, response.newUserCreated)
        Assertions.assertEquals(existingUserId, response.userRead.userId)
        // The legacy pre-Keycloak identity is not a migration in progress - the destructive cross-realm
        // sweep must not run for it.
        verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(any(), any()) }
        // The stale row is still replaced through the benign ResolveExistingAuthCleanup collapse.
        verify(exactly = 1) { userPersistence.replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }
      }

      @Test
      fun `SSO login with a previous Keycloak identity still sweeps other realms to resume migration`() {
        val incomingUser =
          AuthenticatedUser()
            .withUserId(existingUserId)
            .withEmail(email)
            .withAuthUserId(newAuthUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK)

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns incomingUser
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of(incomingUser)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returns
          listOf(
            AuthUser().withUserId(existingUserId).withAuthUserId(newAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK),
            AuthUser().withUserId(existingUserId).withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK),
          )
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of(organization)

        val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        Assertions.assertEquals(false, response.newUserCreated)
        Assertions.assertEquals(existingUserId, response.userRead.userId)
        // A previous Keycloak identity is the signal that this resumes an interrupted SSO migration -
        // the destructive cross-realm sweep must still run.
        verify(exactly = 1) { externalUserService.deleteUserByEmailOnOtherRealms(email, ssoRealm) }
        verify(exactly = 1) { userPersistence.replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }
      }

      @Test
      fun testSsoLoginWithInvalidDomainRejectedBeforeExistingUserMigration() {
        every { featureFlagClient.boolVariation(BypassSsoDomainValidationEnforcement, any()) } returns false
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of<Organization>(organization)
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns
          listOf(
            OrganizationEmailDomain()
              .withOrganizationId(organization.organizationId)
              .withEmailDomain("attacker.com"),
          )

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.empty<AuthenticatedUser>()
        every { userPersistence.getUserByEmail(email) } returns Optional.of<User>(existingUser!!)
        every { userPersistence.getUser(existingUserId) } returns Optional.of<User>(existingUser!!)
        every { userPersistence.listAuthUsersForUser(existingUserId) } returns
          listOf(
            AuthUser()
              .withAuthUserId(existingAuthUserId)
              .withAuthProvider(AuthProvider.KEYCLOAK),
          )
        every { externalUserService.getRealmByAuthUserId(existingAuthUserId) } returns realm
        every { ssoConfigService.getSsoConfigByRealmName(realm) } returns null

        val existingAuthedUser =
          AuthenticatedUserConverter.toAuthenticatedUser(existingUser!!, existingAuthUserId, AuthProvider.KEYCLOAK)
        every { applicationService.listApplicationsByUser(existingAuthedUser) } returns listOf(Application().withId("app_id"))
        every {
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
          )
        } returns WorkspaceReadList(workspaces = listOf(testWorkspaceRead(UUID.randomUUID())))
        every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
          listOf(UserPermission().withUser(User().withUserId(UUID.randomUUID())))

        Assertions.assertThrows(OperationNotAllowedException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        }

        verify(exactly = 0) { externalUserService.deleteUserByExternalId(newAuthUserId, ssoRealm) }
        verify(exactly = 0) { userPersistence.replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK) }
        verify(exactly = 0) { externalUserService.deleteUserByEmailOnOtherRealms(email, ssoRealm) }
        verify(exactly = 0) { applicationService.deleteApplication(existingAuthedUser, "app_id") }
        verify(exactly = 0) { permissionHandler.createPermission(any()) }
      }

      @Test
      fun testSsoLoginWithMismatchedDomainAllowedWhenOrganizationIsBypassed() {
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of<Organization>(organization)
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns
          listOf(
            OrganizationEmailDomain()
              .withOrganizationId(organization.organizationId)
              .withEmailDomain("attacker.com"),
          )

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of<AuthenticatedUser>(persistedJwtUser())

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        // Login succeeds despite domain mismatch — no exception thrown
        Assertions.assertNotNull(result)
        verify(exactly = 1) {
          featureFlagClient.boolVariation(
            BypassSsoDomainValidationEnforcement,
            FeatureFlagOrganization(organization.organizationId),
          )
        }
      }

      @Test
      fun testSsoLoginWithMismatchedDomainRejectedWhenOrganizationIsNotBypassed() {
        every { featureFlagClient.boolVariation(BypassSsoDomainValidationEnforcement, any()) } returns false
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of<Organization>(organization)
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns
          listOf(
            OrganizationEmailDomain()
              .withOrganizationId(organization.organizationId)
              .withEmailDomain("attacker.com"),
          )

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of<AuthenticatedUser>(persistedJwtUser())

        Assertions.assertThrows(OperationNotAllowedException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        }
        verify(exactly = 0) { externalUserService.deleteUserByExternalId(newAuthUserId, ssoRealm) }
      }

      @Test
      fun testSsoLoginWithNoClaimedDomainsAllowedWhenOrganizationIsBypassed() {
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of<Organization>(organization)
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns emptyList()

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of<AuthenticatedUser>(persistedJwtUser())

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        // Login succeeds — no exception thrown
        Assertions.assertNotNull(result)
      }

      @Test
      fun testSsoLoginWithNoClaimedDomainsRejectedWhenOrganizationIsNotBypassed() {
        every { featureFlagClient.boolVariation(BypassSsoDomainValidationEnforcement, any()) } returns false
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of<Organization>(organization)
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns emptyList()

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of<AuthenticatedUser>(persistedJwtUser())

        Assertions.assertThrows(OperationNotAllowedException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))
        }
        verify(exactly = 0) { externalUserService.deleteUserByExternalId(newAuthUserId, ssoRealm) }
      }

      @Test
      fun testSsoLoginWithMatchingDomainAllowedWhenOrganizationIsNotBypassed() {
        every { featureFlagClient.boolVariation(BypassSsoDomainValidationEnforcement, any()) } returns false
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of<Organization>(organization)
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns
          listOf(
            OrganizationEmailDomain()
              .withOrganizationId(organization.organizationId)
              .withEmailDomain("airbyte.io"),
          )

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of<AuthenticatedUser>(persistedJwtUser())

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        Assertions.assertNotNull(result)
        verify(exactly = 0) {
          featureFlagClient.boolVariation(
            BypassSsoDomainValidationEnforcement,
            any(),
          )
        }
      }

      @Test
      fun testSsoLoginWithInvalidDomainAllowedWhenBypassFlagIsAbsent() {
        userHandler =
          UserHandler(
            userPersistence,
            externalUserService,
            organizationService,
            ssoConfigService,
            organizationEmailDomainService,
            Optional.of(applicationService),
            permissionHandler,
            workspacesHandler,
            uuidSupplier,
            jwtUserAuthenticationResolver,
            Optional.of(initialUserConfig),
            resourceBootstrapHandler,
            TestClient(emptyMap()),
            ssoRbacEntitlementChecker,
            scimFirstLoginService,
            transactionOperations,
          )
        every { organizationService.getOrganizationBySsoConfigRealm(ssoRealm) } returns Optional.of<Organization>(organization)
        every { organizationEmailDomainService.findByOrganizationId(organization.organizationId) } returns emptyList()

        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns jwtUser!!
        every { jwtUserAuthenticationResolver.resolveRealm() } returns ssoRealm
        every { userPersistence.getUserByAuthId(newAuthUserId) } returns Optional.of<AuthenticatedUser>(persistedJwtUser())

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        Assertions.assertNotNull(result)
        verify(exactly = 0) { externalUserService.deleteUserByExternalId(any(), any()) }
      }

      private val existingUserId: UUID = UUID.randomUUID()
      private val existingAuthUserId = "existing_auth_user_id"
      private val newAuthUserId = "new_auth_user_id"
      private val email = "user@airbyte.io"
      private val ssoRealm = "airbyte-realm"
      private val realm = "_airbyte-users"
    }

    @Nested
    internal inner class NewUserTest {
      private val newAuthUserId = "new_auth_user_id"
      private val newUserId: UUID = UUID.randomUUID()
      private val newEmail = "new@gmail.com"
      private val existingUserId: UUID = UUID.randomUUID()
      private val existingEmail = "existing@gmail.com"
      private val workspaceId: UUID = UUID.randomUUID()

      private var newAuthedUser: AuthenticatedUser? = null
      private var newUser: User? = null
      private var existingUser: User? = null
      private var defaultWorkspace: WorkspaceRead? = null

      @BeforeEach
      fun setUp() {
        newAuthedUser = AuthenticatedUser().withUserId(newUserId).withEmail(newEmail).withAuthUserId(newAuthUserId)
        newUser = toUser(newAuthedUser!!)
        existingUser = User().withUserId(existingUserId).withEmail(existingEmail)
        defaultWorkspace = testWorkspaceRead(workspaceId)
        every { userPersistence.getUserByAuthId(any()) } returns Optional.empty<AuthenticatedUser>()
        every { jwtUserAuthenticationResolver.resolveUser(newAuthUserId) } returns newAuthedUser!!
        every { uuidSupplier.get() } returns newUserId
        every { userPersistence.getUser(newUserId) } returns Optional.of<User>(newUser!!)
        every { resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any()) } returns defaultWorkspace!!
      }

      @ParameterizedTest
      @ArgumentsSource(NewUserArgumentsProvider::class)
      fun testNewUserCreation(
        authProvider: AuthProvider,
        authRealm: String?,
        initialUserEmail: String?,
        initialUserPresent: Boolean,
        isFirstOrgUser: Boolean,
        isDefaultWorkspaceForOrgPresent: Boolean,
        domainRestrictedToOrgId: UUID?,
      ) {
        newAuthedUser!!.authProvider = authProvider

        if (domainRestrictedToOrgId != null) {
          val emailDomain =
            newUser!!
              .email
              .split("@".toRegex())
              .dropLastWhile { it.isEmpty() }
              .toTypedArray()[1]
          every { organizationEmailDomainService.findByEmailDomain(emailDomain) } returns
            listOf<OrganizationEmailDomain>(
              OrganizationEmailDomain()
                .withOrganizationId(domainRestrictedToOrgId)
                .withEmailDomain(emailDomain),
            )
        }

        every { jwtUserAuthenticationResolver.resolveRealm() } returns authRealm
        if (authRealm != null) {
          every { organizationService.getOrganizationBySsoConfigRealm(authRealm) } returns
            Optional.of<Organization>(
              organization,
            )
        }

        if (initialUserPresent) {
          if (initialUserEmail != null) {
            every { initialUserConfig.email } returns initialUserEmail
          }
        } else {
          // replace default user handler with one that doesn't use initial user config (ie to test what
          // happens in Cloud)
          userHandler =
            UserHandler(
              userPersistence,
              externalUserService,
              organizationService,
              ssoConfigService,
              organizationEmailDomainService,
              Optional.of<ApplicationService>(applicationService),
              permissionHandler,
              workspacesHandler,
              uuidSupplier,
              jwtUserAuthenticationResolver,
              Optional.empty<InitialUserConfig>(),
              resourceBootstrapHandler,
              featureFlagClient,
              ssoRbacEntitlementChecker,
              scimFirstLoginService,
              transactionOperations,
            )
        }

        if (isFirstOrgUser) {
          every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns mutableListOf()
        } else {
          // add a pre-existing admin user for the org if this isn't the first user
          val existingUserPermission =
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))

          every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
            listOf<UserPermission>(existingUserPermission)
        }

        if (isDefaultWorkspaceForOrgPresent) {
          every {
            workspacesHandler.listWorkspacesInOrganization(
              ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
            )
          } returns WorkspaceReadList(workspaces = listOf(defaultWorkspace!!))
          if (newUser!!.defaultWorkspaceId == null) {
            newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId
          }
        } else {
          every {
            workspacesHandler.listWorkspacesInOrganization(
              any<ListWorkspacesInOrganizationRequestBody>(),
            )
          } returns WorkspaceReadList(workspaces = mutableListOf<WorkspaceRead>())
        }

        val apiAuthProvider = authProvider.convertTo<io.airbyte.api.server.generated.models.AuthProvider>()

        if (domainRestrictedToOrgId != null && (authRealm == null || domainRestrictedToOrgId !== organization.organizationId)) {
          Assertions.assertThrows(
            SSORequiredProblem::class.java,
          ) {
            userHandler.getOrCreateUserByAuthId(
              UserAuthIdRequestBody(authUserId = newAuthUserId),
            )
          }
          verify(exactly = 0) { userPersistence.createAuthenticatedUserIfNoScimMapping(any()) }
          if (authRealm != null) {
            verify(exactly = 0) { externalUserService.deleteUserByExternalId(newAuthedUser!!.authUserId, authRealm) }
          }
          return
        }

        val response =
          userHandler.getOrCreateUserByAuthId(
            UserAuthIdRequestBody(authUserId = newAuthUserId),
          )

        Assertions.assertEquals(true, response.newUserCreated)
        verifyCreatedUser(authProvider)
        verifyUserRes(response, apiAuthProvider)
        verifyInstanceAdminPermissionCreation(initialUserEmail, initialUserPresent)
        verifyOrganizationPermissionCreation(authRealm, isFirstOrgUser)
        verifyDefaultWorkspaceCreation(isDefaultWorkspaceForOrgPresent, authProvider)
      }

      @ParameterizedTest
      @CsvSource(
        "ORGANIZATION_MEMBER,ORGANIZATION_MEMBER",
        "ORGANIZATION_EDITOR,ORGANIZATION_EDITOR",
        "ORGANIZATION_ADMIN,ORGANIZATION_ADMIN",
      )
      fun testNewSsoUserCreationUsesConfiguredDefaultRole(
        configuredDefaultRole: Permission.PermissionType,
        expectedPermissionType: Permission.PermissionType,
      ) {
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        every { jwtUserAuthenticationResolver.resolveRealm() } returns "airbyte-realm"
        every { organizationService.getOrganizationBySsoConfigRealm("airbyte-realm") } returns Optional.of(organization)
        every { ssoConfigService.getSsoConfig(organization.organizationId) } returns SsoConfig().withDefaultRole(configuredDefaultRole)
        every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          )
        every {
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
          )
        } returns WorkspaceReadList(workspaces = listOf(defaultWorkspace!!))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        verify(exactly = 1) {
          permissionHandler.createPermission(
            Permission()
              .withPermissionType(expectedPermissionType)
              .withOrganizationId(organization.organizationId)
              .withUserId(newUserId),
          )
        }
      }

      @Test
      fun testNewSsoUserCreationFallsBackToMemberWhenConfigurableSsoDefaultRoleFlagOff() {
        // Flag OFF (dark launch): the configured EDITOR role must be ignored and provisioning must
        // fall back to ORGANIZATION_MEMBER, preserving pre-feature behavior.
        every { featureFlagClient.boolVariation(ConfigurableSsoDefaultRole, any()) } returns false
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        every { jwtUserAuthenticationResolver.resolveRealm() } returns "airbyte-realm"
        every { organizationService.getOrganizationBySsoConfigRealm("airbyte-realm") } returns Optional.of(organization)
        every { ssoConfigService.getSsoConfig(organization.organizationId) } returns
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR)
        every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          )
        every {
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
          )
        } returns WorkspaceReadList(workspaces = listOf(defaultWorkspace!!))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        verify(exactly = 1) {
          permissionHandler.createPermission(
            Permission()
              .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
              .withOrganizationId(organization.organizationId)
              .withUserId(newUserId),
          )
        }
      }

      @Test
      fun testNewSsoUserCreationFallsBackToAdminWhenRbacRolesEntitlementIsAbsent() {
        every { ssoRbacEntitlementChecker.check(OrganizationId(organization.organizationId)) } returns
          EntitlementResult(RbacRolesEntitlement.featureId, false)
        every { featureFlagClient.boolVariation(ConfigurableSsoDefaultRole, any()) } returns false
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        every { jwtUserAuthenticationResolver.resolveRealm() } returns "airbyte-realm"
        every { organizationService.getOrganizationBySsoConfigRealm("airbyte-realm") } returns Optional.of(organization)
        every { ssoConfigService.getSsoConfig(organization.organizationId) } returns
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR)
        every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          )
        every {
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
          )
        } returns WorkspaceReadList(workspaces = listOf(defaultWorkspace!!))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        verify(exactly = 1) {
          permissionHandler.createPermission(
            Permission()
              .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
              .withOrganizationId(organization.organizationId)
              .withUserId(newUserId),
          )
        }
        verify(exactly = 1) { ssoRbacEntitlementChecker.check(OrganizationId(organization.organizationId)) }
        verify(exactly = 0) {
          featureFlagClient.boolVariation(
            ConfigurableSsoDefaultRole,
            any(),
          )
        }
      }

      @ParameterizedTest
      @CsvSource(
        "false,ORGANIZATION_MEMBER",
        "true,ORGANIZATION_EDITOR",
      )
      fun testNewSsoUserCreationPreservesExistingRoleSelectionWhenRbacRolesEntitlementCheckIsIndeterminate(
        configurableRoleEnabled: Boolean,
        expectedPermissionType: Permission.PermissionType,
      ) {
        every { ssoRbacEntitlementChecker.check(OrganizationId(organization.organizationId)) } returns
          EntitlementResult(
            RbacRolesEntitlement.featureId,
            false,
            isEntitlementCheckSuccessful = false,
          )
        every { featureFlagClient.boolVariation(ConfigurableSsoDefaultRole, any()) } returns configurableRoleEnabled
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        every { jwtUserAuthenticationResolver.resolveRealm() } returns "airbyte-realm"
        every { organizationService.getOrganizationBySsoConfigRealm("airbyte-realm") } returns Optional.of(organization)
        every { ssoConfigService.getSsoConfig(organization.organizationId) } returns
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR)
        every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          )
        every {
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
          )
        } returns WorkspaceReadList(workspaces = listOf(defaultWorkspace!!))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        verify(exactly = 1) {
          permissionHandler.createPermission(
            Permission()
              .withPermissionType(expectedPermissionType)
              .withOrganizationId(organization.organizationId)
              .withUserId(newUserId),
          )
        }
        verify(exactly = 1) { ssoRbacEntitlementChecker.check(OrganizationId(organization.organizationId)) }
        verify(exactly = 1) {
          featureFlagClient.boolVariation(
            ConfigurableSsoDefaultRole,
            FeatureFlagOrganization(organization.organizationId),
          )
        }
      }

      @Test
      fun testNewSsoUserCreationDefaultsToMemberWhenConfiguredDefaultRoleIsNull() {
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        every { jwtUserAuthenticationResolver.resolveRealm() } returns "airbyte-realm"
        every { organizationService.getOrganizationBySsoConfigRealm("airbyte-realm") } returns Optional.of(organization)
        every { ssoConfigService.getSsoConfig(organization.organizationId) } returns SsoConfig()
        every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          )
        every {
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
          )
        } returns WorkspaceReadList(workspaces = listOf(defaultWorkspace!!))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        verify(exactly = 1) {
          permissionHandler.createPermission(
            Permission()
              .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
              .withOrganizationId(organization.organizationId)
              .withUserId(newUserId),
          )
        }
      }

      @Test
      fun testFirstNewSsoUserCreationAlwaysGrantsAdmin() {
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        every { jwtUserAuthenticationResolver.resolveRealm() } returns "airbyte-realm"
        every { organizationService.getOrganizationBySsoConfigRealm("airbyte-realm") } returns Optional.of(organization)
        every { ssoConfigService.getSsoConfig(organization.organizationId) } returns
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR)
        every { permissionHandler.listPermissionsForOrganization(organization.organizationId) } returns mutableListOf()
        every {
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody(organizationId = organization.organizationId),
          )
        } returns WorkspaceReadList(workspaces = listOf(defaultWorkspace!!))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody(authUserId = newAuthUserId))

        verify(exactly = 1) {
          permissionHandler.createPermission(
            Permission()
              .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
              .withOrganizationId(organization.organizationId)
              .withUserId(newUserId),
          )
        }
      }

      private fun isCreatedUser(
        user: AuthenticatedUser?,
        expectedAuthProvider: AuthProvider?,
      ): Boolean =
        user!!.userId == newUserId &&
          newEmail == user.email &&
          newAuthUserId == user.authUserId &&
          user.authProvider == expectedAuthProvider

      private fun verifyCreatedUser(expectedAuthProvider: AuthProvider?) {
        verify(exactly = 1) {
          userPersistence.createAuthenticatedUserIfNoScimMapping(
            match { user: AuthenticatedUser? -> isCreatedUser(user, expectedAuthProvider) },
          )
        }
      }

      private fun verifyDefaultWorkspaceCreation(
        isDefaultWorkspaceForOrgPresent: Boolean,
        expectedAuthProvider: AuthProvider?,
      ) {
        // No need to deal with other vars because SSO users and first org users etc. are all directed
        // through the same codepath now.
        if (!isDefaultWorkspaceForOrgPresent) {
          // create a default workspace for the org if one doesn't yet exist
          verify(exactly = 1) {
            resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(
              any<WorkspaceCreateWithId>(),
            )
          }

          // if a workspace was created, verify that the user's defaultWorkspaceId was updated
          // and that a workspaceAdmin permission was created for them.
          verifyOrder {
            userPersistence.createAuthenticatedUserIfNoScimMapping(
              match { user: AuthenticatedUser? -> isCreatedUser(user, expectedAuthProvider) },
            )
            userPersistence.writeUser(
              match { user: User? ->
                user!!.defaultWorkspaceId.equals(
                  workspaceId,
                )
              },
            )
          }
        } else {
          // never create an additional workspace for the org if one already exists.
          verify(exactly = 0) {
            resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(
              any<WorkspaceCreateWithId>(),
            )
          }
        }
      }

      private fun verifyUserRes(
        userRes: UserGetOrCreateByAuthIdResponse,
        expectedAuthProvider: io.airbyte.api.server.generated.models.AuthProvider?,
      ) {
        val userRead = userRes.userRead
        Assertions.assertEquals(userRead.userId, newUserId)
        Assertions.assertEquals(userRead.email, newEmail)
        Assertions.assertEquals(userRes.authUserId, newAuthUserId)
        Assertions.assertEquals(userRes.authProvider, expectedAuthProvider)
      }

      private fun verifyInstanceAdminPermissionCreation(
        initialUserEmail: String?,
        initialUserPresent: Boolean,
      ) {
        // instance_admin permissions should only ever be created when the initial user config is present
        // (which should never be true in Cloud).
        // also, if the initial user email is null or doesn't match the new user's email, no instance_admin
        // permission should be created
        if (!initialUserPresent || initialUserEmail == null || !initialUserEmail.equals(newEmail, ignoreCase = true)) {
          verify(exactly = 0) {
            permissionHandler.createPermission(
              match { permission: Permission? ->
                permission!!.permissionType ==
                  Permission.PermissionType.INSTANCE_ADMIN
              },
            )
          }
          verify(exactly = 0) { permissionHandler.grantInstanceAdmin(any()) }
        } else {
          // otherwise, instance_admin permission should be created
          verify(exactly = 1) { permissionHandler.grantInstanceAdmin(any()) }
        }
      }

      private fun verifyOrganizationPermissionCreation(
        ssoRealm: String?,
        isFirstOrgUser: Boolean,
      ) {
        // if the SSO Realm is null, no organization permission should be created
        if (ssoRealm == null) {
          verify(exactly = 0) {
            permissionHandler.createPermission(
              match { permission: Permission? ->
                permission!!.permissionType ==
                  Permission.PermissionType.ORGANIZATION_ADMIN
              },
            )
          }
        } else {
          val expectedPermissionType =
            if (isFirstOrgUser) {
              Permission.PermissionType.ORGANIZATION_ADMIN
            } else {
              Permission.PermissionType.ORGANIZATION_MEMBER
            }
          // otherwise, organization permission should be created for the associated user and org.
          verify(exactly = 1) {
            permissionHandler.createPermission(
              Permission()
                .withPermissionType(expectedPermissionType)
                .withOrganizationId(organization.organizationId)
                .withUserId(newUserId),
            )
          }
        }
      }
    }
  }

  companion object {
    private val userId: UUID = UUID.randomUUID()
    private const val USER_NAME = "user 1"
    private const val USER_EMAIL = "user_1@whatever.com"

    private val organization: Organization = Organization().withOrganizationId(UUID.randomUUID()).withName(USER_NAME).withEmail(USER_EMAIL)
    private val permission1Id: UUID = UUID.randomUUID()

    class NewUserArgumentsProvider : ArgumentsProvider {
      override fun provideArguments(context: ExtensionContext): Stream<Arguments> {
        val authProviders = listOf(*AuthProvider.entries.toTypedArray())
        val authRealms = mutableListOf("airbyte-realm", null)
        val initialUserEmails = listOf(null, "", "other@gmail.com", "new@gmail.com")
        val domainRestrictedToOrgIds = listOf(null, UUID.randomUUID(), organization.organizationId)
        val initialUserConfigPresent = mutableListOf<Boolean?>(true, false)
        val isFirstOrgUser = mutableListOf<Boolean?>(true, false)
        val isDefaultWorkspaceForOrgPresent = mutableListOf<Boolean?>(true, false)

        // return all permutations of the above input lists so that we can test all combinations.
        return authProviders
          .flatMap { authProvider: AuthProvider? ->
            authRealms
              .flatMap { authRealm: String? ->
                initialUserEmails
                  .flatMap { email: String? ->
                    initialUserConfigPresent
                      .flatMap { initialUserPresent: Boolean? ->
                        isFirstOrgUser
                          .flatMap { firstOrgUser: Boolean? ->
                            isDefaultWorkspaceForOrgPresent
                              .flatMap { orgWorkspacePresent: Boolean? ->
                                domainRestrictedToOrgIds
                                  .flatMap { domainRestrictedToOrgId: UUID? ->
                                    listOf(
                                      Arguments.of(
                                        authProvider,
                                        authRealm,
                                        email,
                                        initialUserPresent,
                                        firstOrgUser,
                                        orgWorkspacePresent,
                                        domainRestrictedToOrgId,
                                      ),
                                    )
                                  }
                              }
                          }
                      }
                  }
              }
          }.stream()
      }
    }

    @JvmStatic
    private fun ssoSignInArgsProvider() =
      listOf( // Existing user is already an SSO user (will error):
        Arguments.of(true, false), // Existing user is regular user (will migrate):
        Arguments.of(false, true),
        Arguments.of(false, false),
      )
  }
}

private fun testWorkspaceRead(workspaceId: UUID) =
  WorkspaceRead(
    workspaceId = workspaceId,
    customerId = UUID.randomUUID(),
    name = "name",
    slug = "slug",
    initialSetupComplete = true,
    organizationId = UUID.randomUUID(),
  )
