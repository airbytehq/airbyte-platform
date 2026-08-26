/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationUserRead
import io.airbyte.api.model.generated.OrganizationUserReadList
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.UserAuthIdRequestBody
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.model.generated.UserWithPermissionInfoRead
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoRead
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoReadList
import io.airbyte.api.problems.throwable.generated.SSORequiredProblem
import io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem
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
import io.mockk.every
import io.mockk.mockk
import jakarta.validation.Valid
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
import org.mockito.InOrder
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
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
      override fun getConnection(): Connection = mock()

      override fun hasConnection(): Boolean = false

      override fun findTransactionStatus(): Optional<out TransactionStatus<*>> = Optional.empty()

      override fun <R> execute(
        definition: TransactionDefinition,
        callback: TransactionCallback<Connection, R>,
      ): R {
        transactionCallbackActive = true
        return try {
          callback.call(mock())
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
    userPersistence = mock()
    permissionHandler = mock()
    workspacesHandler = mock()
    organizationService = mock()
    ssoConfigService = mock()
    organizationEmailDomainService = mock()
    organizationsHandler = mock()
    uuidSupplier = mock()
    jwtUserAuthenticationResolver = mock()
    initialUserConfig = mockk(relaxed = true)
    resourceBootstrapHandler = mock()
    externalUserService = mock()
    applicationService = mock()
    featureFlagClient = mock<TestClient>()
    ssoRbacEntitlementChecker = mock()
    scimFirstLoginService = mock()

    whenever(featureFlagClient.boolVariation(eq(RestrictLoginsForSSODomains), any()))
      .thenReturn(true)
    // ConfigurableSsoDefaultRole defaults ON in tests so existing assertions exercise the configured
    // role; prod default is OFF (dark launch). Flag-off behavior is covered explicitly below.
    whenever(featureFlagClient.boolVariation(eq(ConfigurableSsoDefaultRole), any()))
      .thenReturn(true)
    whenever(ssoRbacEntitlementChecker.check(any()))
      .thenReturn(EntitlementResult(RbacRolesEntitlement.featureId, true))
    whenever(featureFlagClient.boolVariation(eq(BypassSsoDomainValidationEnforcement), any()))
      .thenReturn(true)

    // SEC-14: Most tests use an org without claimed domains and the bypass enabled above, preserving
    // their pre-enforcement behavior. Domain-enforcement tests override both inputs explicitly.
    whenever(organizationEmailDomainService.findByOrganizationId(any()))
      .thenReturn(emptyList())
    whenever(
      scimFirstLoginService.attachIfPreProvisioned(
        any(),
        anyOrNull(),
        any(),
        anyOrNull(),
        anyOrNull(),
        anyOrNull(),
      ),
    ).thenReturn(ScimFirstLoginAttachmentResult.NoMatch)
    whenever(userPersistence.createAuthenticatedUserIfNoScimMapping(any())).thenReturn(true)
    whenever(userPersistence.writeAuthUser(any(), any(), anyOrNull())).thenReturn(true)
    whenever(userPersistence.replaceAuthUserForUserId(any(), any(), anyOrNull())).thenReturn(true)
    whenever(userPersistence.enableAgenticUser(any(), any())).thenAnswer { invocation -> invocation.getArgument(1) }

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
    whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
    whenever(jwtUserAuthenticationResolver.resolveVerifiedEmail()).thenReturn(currentMappingEmail)
    whenever(userPersistence.getUserByAuthId(authUserId))
      .thenReturn(Optional.empty(), Optional.of(attachedUser))
    whenever(
      scimFirstLoginService.attachIfPreProvisioned(
        currentMappingEmail,
        currentMappingEmail,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      ),
    ).thenReturn(ScimFirstLoginAttachmentResult.Attached(mappedUserId))

    val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))

    Assertions.assertFalse(result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertEquals(staleGlobalEmail, result.userRead.email)
    Mockito.verify(userPersistence, Mockito.never()).getUserByEmail(any())
    Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())
    Mockito.verify(userPersistence, Mockito.never()).replaceAuthUserForUserId(any(), any(), anyOrNull())
    Mockito.verifyNoInteractions(resourceBootstrapHandler)
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
    whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
    whenever(jwtUserAuthenticationResolver.resolveVerifiedEmail()).thenReturn(email)
    whenever(userPersistence.getUserByAuthId(authUserId))
      .thenReturn(Optional.empty(), Optional.of(attachedUser))
    whenever(
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      ),
    ).thenReturn(ScimFirstLoginAttachmentResult.Attached(mappedUserId))

    val result =
      userHandler.getOrCreateUserByAuthId(
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(true),
      )

    Assertions.assertFalse(result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertNotNull(result.userRead.agenticEnabledAt)
    Mockito.verify(userPersistence).enableAgenticUser(mappedUserId, result.userRead.agenticEnabledAt!!)
    Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())
    Mockito.verify(userPersistence, Mockito.never()).getUserByEmail(any())
    Mockito.verifyNoInteractions(permissionHandler, workspacesHandler, resourceBootstrapHandler)
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
    whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
    whenever(jwtUserAuthenticationResolver.resolveVerifiedEmail()).thenReturn(email)
    whenever(userPersistence.getUserByAuthId(authUserId))
      .thenReturn(Optional.empty(), Optional.of(attachedUser))
    whenever(
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      ),
    ).thenReturn(ScimFirstLoginAttachmentResult.Attached(mappedUserId))
    val request = UserAuthIdRequestBody().authUserId(authUserId)
    if (isAgenticUser != null) {
      request.isAgenticUser(isAgenticUser)
    }

    val result = userHandler.getOrCreateUserByAuthId(request)

    Assertions.assertFalse(result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertEquals(io.airbyte.api.model.generated.UserStatus.INVITED, result.userRead.status)
    Assertions.assertNull(result.userRead.agenticEnabledAt)
    Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())
    Mockito.verify(userPersistence, Mockito.never()).getUserByEmail(any())
    Mockito.verifyNoInteractions(permissionHandler, workspacesHandler, resourceBootstrapHandler)
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
    whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
    whenever(jwtUserAuthenticationResolver.resolveVerifiedEmail()).thenReturn(email)
    whenever(userPersistence.getUserByAuthId(authUserId))
      .thenReturn(Optional.of(existingUser))
    whenever(userPersistence.enableAgenticUser(eq(mappedUserId), any()))
      .thenReturn(persistedAgenticEnabledAt)

    val result =
      userHandler.getOrCreateUserByAuthId(
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(true),
      )

    Assertions.assertFalse(result.newUserCreated)
    Assertions.assertEquals(mappedUserId, result.userRead.userId)
    Assertions.assertEquals(persistedAgenticEnabledAt, result.userRead.agenticEnabledAt)
    Mockito.verifyNoInteractions(scimFirstLoginService)
    Mockito.verify(userPersistence, Mockito.never()).getUserByEmail(any())
    Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())
    Mockito.verify(userPersistence).enableAgenticUser(eq(mappedUserId), any())
    Mockito.verify(userPersistence, Mockito.never()).replaceAuthUserForUserId(any(), any(), anyOrNull())
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
    whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
    whenever(jwtUserAuthenticationResolver.resolveVerifiedEmail()).thenReturn(null)
    whenever(userPersistence.getUserByAuthId(authUserId))
      .thenReturn(Optional.empty())
    whenever(
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        null,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      ),
    ).thenReturn(ScimFirstLoginAttachmentResult.EmailNotVerified)

    Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
      userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
    }

    Mockito.verify(userPersistence, Mockito.never()).getUserByEmail(any())
    Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())
    Mockito.verify(userPersistence, Mockito.never()).replaceAuthUserForUserId(any(), any(), anyOrNull())
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
    whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
    whenever(jwtUserAuthenticationResolver.resolveVerifiedEmail()).thenReturn(email)
    whenever(userPersistence.getUserByAuthId(authUserId))
      .thenReturn(Optional.empty())
    whenever(
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      ),
    ).thenReturn(ScimFirstLoginAttachmentResult.AmbiguousIdentity)

    Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
      userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
    }

    Mockito.verify(userPersistence, Mockito.never()).getUserByEmail(any())
    Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())
    Mockito.verify(userPersistence, Mockito.never()).replaceAuthUserForUserId(any(), any(), anyOrNull())
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
    whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
    whenever(jwtUserAuthenticationResolver.resolveVerifiedEmail()).thenReturn(email)
    whenever(userPersistence.getUserByAuthId(authUserId))
      .thenReturn(Optional.empty())
    whenever(
      scimFirstLoginService.attachIfPreProvisioned(
        email,
        email,
        authUserId,
        AuthProvider.KEYCLOAK,
        null,
      ),
    ).thenReturn(ScimFirstLoginAttachmentResult.Conflict)

    Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
      userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
    }

    Mockito.verify(userPersistence, Mockito.never()).getUserByEmail(any())
    Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())
    Mockito.verify(userPersistence, Mockito.never()).replaceAuthUserForUserId(any(), any(), anyOrNull())
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

    whenever(permissionHandler.listUsersInOrganization(organizationId))
      .thenReturn(listOf<UserPermission>(defaultUserPermission, realUserPermission))

    // no default user present
    val expectedListResult =
      OrganizationUserReadList().users(
        listOf<@Valid OrganizationUserRead?>(
          OrganizationUserRead()
            .name(USER_NAME)
            .userId(userID)
            .email(USER_EMAIL)
            .organizationId(organizationId)
            .permissionId(permission1Id)
            .permissionType(PermissionType.ORGANIZATION_ADMIN),
        ),
      )

    val result = userHandler.listUsersInOrganization(OrganizationIdRequestBody().organizationId(organizationId))
    Assertions.assertEquals(expectedListResult, result)
  }

  @Test
  fun testListInstanceAdminUser() {
    whenever(permissionHandler.listInstanceAdminUsers()).thenReturn(
      listOf<UserPermission>(
        UserPermission()
          .withUser(
            User().withName(USER_NAME).withUserId(userId).withEmail(USER_EMAIL),
          ).withPermission(Permission().withPermissionId(permission1Id).withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)),
      ),
    )

    val result = userHandler.listInstanceAdminUsers()

    val expectedResult =
      UserWithPermissionInfoReadList().users(
        listOf<@Valid UserWithPermissionInfoRead?>(
          UserWithPermissionInfoRead()
            .name(USER_NAME)
            .userId(userId)
            .email(USER_EMAIL)
            .permissionId(permission1Id),
        ),
      )
    Assertions.assertEquals(expectedResult, result)
  }

  @Test
  fun testListAccessInfoByWorkspaceId() {
    val workspaceId = UUID.randomUUID()
    whenever(userPersistence.listWorkspaceUserAccessInfo(workspaceId)).thenReturn(
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
      ),
    )

    val result = userHandler.listAccessInfoByWorkspaceId(WorkspaceIdRequestBody().workspaceId(workspaceId))

    val expected =
      WorkspaceUserAccessInfoReadList().usersWithAccess(
        listOf<@Valid WorkspaceUserAccessInfoRead?>(
          WorkspaceUserAccessInfoRead()
            .userId(userId)
            .userName(USER_NAME)
            .userEmail(USER_EMAIL)
            .workspaceId(workspaceId)
            .workspacePermission(
              PermissionRead()
                .permissionId(permission1Id)
                .permissionType(PermissionType.WORKSPACE_ADMIN)
                .userId(userId)
                .workspaceId(workspaceId),
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

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(jwtUser)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.empty())
      whenever(uuidSupplier.get()).thenReturn(newUserId)

      val createdUser =
        User()
          .withUserId(newUserId)
          .withEmail("agentic@test.com")
          .withAgenticEnabledAt(java.time.OffsetDateTime.now())
      whenever(userPersistence.getUser(newUserId)).thenReturn(Optional.of(createdUser))

      val defaultWorkspace = WorkspaceRead().workspaceId(UUID.randomUUID())
      whenever(resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any())).thenReturn(defaultWorkspace)

      val requestBody =
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(true)

      userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that the user was written with agenticEnabledAt set to a timestamp
      Mockito.verify(userPersistence).createAuthenticatedUserIfNoScimMapping(
        argThat { user: AuthenticatedUser? ->
          user!!.agenticEnabledAt != null
        },
      )
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

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(jwtUser)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.empty())
      whenever(uuidSupplier.get()).thenReturn(newUserId)

      val createdUser =
        User()
          .withUserId(newUserId)
          .withEmail("nonagentic@test.com")
          .withAgenticEnabledAt(null)
      whenever(userPersistence.getUser(newUserId)).thenReturn(Optional.of(createdUser))

      val defaultWorkspace = WorkspaceRead().workspaceId(UUID.randomUUID())
      whenever(resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any())).thenReturn(defaultWorkspace)

      val requestBody =
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(false)

      userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that the user was written with agenticEnabledAt = null
      Mockito.verify(userPersistence).createAuthenticatedUserIfNoScimMapping(
        argThat { user: AuthenticatedUser? ->
          user!!.agenticEnabledAt == null
        },
      )
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

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(jwtUser)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.of(existingNonAgenticUser))

      val requestBody =
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(true) // Request to upgrade

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      Mockito.verify(userPersistence).enableAgenticUser(userId, response.userRead.agenticEnabledAt!!)
      Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())

      // Verify response indicates existing user (not new)
      Assertions.assertFalse(response.newUserCreated)
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

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(jwtUser)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.of(existingNonAgenticUser))

      val requestBody =
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(false)

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that writeAuthenticatedUser was NOT called (no upgrade)
      Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())

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

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(jwtUser)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.of(existingAgenticUser))

      val requestBody =
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(true)

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that writeAuthenticatedUser was NOT called (timestamp immutable)
      Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())

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

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(jwtUser)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.of(existingAgenticUser))

      val requestBody =
        UserAuthIdRequestBody()
          .authUserId(authUserId)
          .isAgenticUser(false) // Attempt to downgrade

      val response = userHandler.getOrCreateUserByAuthId(requestBody)

      // Verify that writeAuthenticatedUser was NOT called (cannot downgrade)
      Mockito.verify(userPersistence, Mockito.never()).writeAuthenticatedUser(any())

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
        authProvider.convertTo<io.airbyte.api.model.generated.AuthProvider>()

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(user)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.of<AuthenticatedUser>(user))

      val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
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
      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(incomingUser)
      whenever(userPersistence.getUserByAuthId(authUserId)).thenReturn(Optional.of(incomingUser))
      whenever(userPersistence.listAuthUsersForUser(existingUserId)).thenReturn(authUsers)
      whenever(externalUserService.getRealmByAuthUserId(previousAuthUserId)).thenAnswer {
        Assertions.assertFalse(transactionCallbackActive)
        null
      }

      val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))

      Assertions.assertFalse(response.newUserCreated)
      Assertions.assertEquals(existingUserId, response.userRead.userId)
      Mockito.verify(userPersistence).replaceAuthUserForUserId(existingUserId, authUserId, AuthProvider.KEYCLOAK)
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

      @ParameterizedTest
      @CsvSource("true", "false")
      fun testNonSSOSignInEmailExistsThrowsError(isExistingUserSSO: Boolean) {
        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(listOf<AuthUser>(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK)))
        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(realm)

        if (isExistingUserSSO) {
          whenever(ssoConfigService.getSsoConfigByRealmName(realm)).thenReturn(
            SsoConfig(),
          )
        }

        Assertions.assertThrows(
          UserAlreadyExistsProblem::class.java,
        ) { userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId)) }
      }

      @Test
      fun testExistingDefaultUserWithEmailUpdatesDefault() {
        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())

        val defaultUser = User().withUserId(DEFAULT_USER_ID).withEmail(email)
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of(defaultUser))

        val newUser =
          AuthenticatedUser()
            .withUserId(UUID.randomUUID())
            .withEmail(email)
            .withAuthUserId(newAuthUserId)
            .withDefaultWorkspaceId(UUID.randomUUID())
        whenever(uuidSupplier.get()).thenReturn(newUser.userId)
        whenever(userPersistence.getUser(newUser.userId)).thenReturn(Optional.of<User>(toUser(newUser)))

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        Assertions.assertTrue(res.newUserCreated)
        Assertions.assertEquals(res.userRead.userId, newUser.userId)
        Assertions.assertEquals(res.userRead.email, email)
        Assertions.assertEquals(res.authUserId, newAuthUserId)

        Mockito.verify(userPersistence).writeUser(defaultUser.withEmail(""))
        Mockito
          .verify(userPersistence)
          .createAuthenticatedUserIfNoScimMapping(
            argThat { user: AuthenticatedUser? ->
              user!!.email ==
                jwtUser!!.email &&
                user.authUserId == jwtUser!!.authUserId
            },
          )
      }

      @Test
      fun testRelinkOrphanedUser() {
        // Auth user in JWT is not linked to any user in the database
        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())

        // A user with the same email exists in the database
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.getUser(existingUserId)).thenReturn(Optional.of<User>(existingUser!!))

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
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(
            existingAuthUsers,
            existingAuthUsers,
            pendingRelinkAuthUsers,
          )
        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenAnswer {
          Assertions.assertFalse(transactionCallbackActive)
          null
        }

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        Assertions.assertFalse(res.newUserCreated)
        Assertions.assertEquals(res.userRead.userId, existingUserId)

        // Verify the incoming identity is staged before the old identity is removed.
        Mockito.verify(userPersistence).writeAuthUser(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
        Mockito.verify(userPersistence).replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
      }

      @Test
      fun testRelinkOrphanedUserRejectsAuthenticationIdentityOwnedByAnotherUser() {
        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(listOf(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK)))
        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(null)
        whenever(
          scimFirstLoginService.attachIfPreProvisioned(
            email,
            null,
            newAuthUserId,
            AuthProvider.KEYCLOAK,
            null,
            null,
          ),
        ).thenReturn(ScimFirstLoginAttachmentResult.NoMatch)
        whenever(
          scimFirstLoginService.attachIfPreProvisioned(
            email,
            null,
            newAuthUserId,
            AuthProvider.KEYCLOAK,
            null,
            existingUserId,
          ),
        ).thenReturn(ScimFirstLoginAttachmentResult.ExistingIdentity(UUID.randomUUID()))

        Assertions.assertThrows(UserAlreadyExistsProblem::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        }

        Mockito.verify(userPersistence, Mockito.never()).getUser(existingUserId)
      }

      @Test
      fun `email match fails closed when authentication identities change during realm resolution`() {
        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of(existingUser!!))
        whenever(userPersistence.getUser(existingUserId)).thenReturn(Optional.of(existingUser!!))

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
        whenever(userPersistence.listAuthUsersForUser(existingUserId)).thenAnswer {
          authUserListReads += 1
          when (authUserListReads) {
            1 -> listOf(existingAuthUser)
            2, 3 -> listOf(existingAuthUser, concurrentAuthUser)
            else -> listOf(existingAuthUser, concurrentAuthUser, incomingAuthUser)
          }
        }
        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(null)

        Assertions.assertThrows(IllegalStateException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        }

        Mockito.verify(userPersistence, Mockito.never()).writeAuthUser(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
        Mockito.verify(userPersistence, Mockito.never()).replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
      }

      @ParameterizedTest
      @MethodSource("io.airbyte.commons.server.handlers.UserHandlerTest#ssoSignInArgsProvider")
      fun testSSOSignInEmailExistsMigratesAuthUser(
        isExistingUserSSO: Boolean,
        doesExistingUserHaveOrgPermission: Boolean,
      ) {
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(
            organization,
          ),
        )
        // SEC-14: The email domain must be claimed by the SSO org for migration to proceed
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(
            listOf(
              OrganizationEmailDomain()
                .withOrganizationId(organization.organizationId)
                .withEmailDomain("airbyte.io"),
            ),
          )

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.getUser(existingUserId)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(listOf<AuthUser>(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK)))

        if (isExistingUserSSO) {
          whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(ssoRealm)
          whenever(ssoConfigService.getSsoConfigByRealmName(ssoRealm)).thenReturn(
            SsoConfig(),
          )

          Assertions.assertThrows(
            UserAlreadyExistsProblem::class.java,
          ) { userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId)) }
          return
        }

        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(realm)
        whenever(ssoConfigService.getSsoConfigByRealmName(realm)).thenReturn(null)

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
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(
            existingAuthUsers,
            existingAuthUsers,
            pendingMigrationAuthUsers,
          )

        val existingAuthedUser =
          AuthenticatedUserConverter.toAuthenticatedUser(existingUser!!, existingAuthUserId, AuthProvider.KEYCLOAK)

        whenever(applicationService.listApplicationsByUser(existingAuthedUser)).thenReturn(
          listOf<Application>(
            Application().withId("app_id"),
          ),
        )
        Mockito.`when`<String?>(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        Mockito
          .`when`(
            workspacesHandler
              .listWorkspacesInOrganization(ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId)),
          ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(WorkspaceRead().workspaceId(UUID.randomUUID()))))

        if (doesExistingUserHaveOrgPermission) {
          Mockito
            .`when`(permissionHandler.listPermissionsForOrganization(organization.organizationId))
            .thenReturn(listOf<UserPermission>(UserPermission().withUser(existingUser)))
        } else {
          Mockito
            .`when`(permissionHandler.listPermissionsForOrganization(organization.organizationId))
            .thenReturn(listOf<UserPermission>(UserPermission().withUser(User().withUserId(UUID.randomUUID()))))
        }

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        Assertions.assertFalse(res.newUserCreated)

        // verify apps are revoked
        Mockito.verify(applicationService).deleteApplication(existingAuthedUser, "app_id")

        // verify auth user is replaced
        Mockito.verify(userPersistence).replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)

        // verify old auth user is deleted from other realms
        Mockito.verify(externalUserService).deleteUserByEmailOnOtherRealms(email, ssoRealm)

        // verify org permission is created (if it doesn't already exist)
        if (!doesExistingUserHaveOrgPermission) {
          Mockito.verify(permissionHandler).createPermission(
            Permission()
              .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
              .withOrganizationId(organization.organizationId)
              .withUserId(existingUserId),
          )
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

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(incomingUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId)).thenReturn(Optional.of(incomingUser))
        whenever(userPersistence.listAuthUsersForUser(existingUserId)).thenReturn(
          listOf(
            AuthUser().withUserId(existingUserId).withAuthUserId(newAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK),
            AuthUser()
              .withUserId(existingUserId)
              .withAuthUserId(existingAuthUserId)
              .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM),
          ),
        )
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(Optional.of(organization))

        val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Assertions.assertFalse(response.newUserCreated)
        Assertions.assertEquals(existingUserId, response.userRead.userId)
        // The legacy pre-Keycloak identity is not a migration in progress - the destructive cross-realm
        // sweep must not run for it.
        Mockito.verify(externalUserService, Mockito.never()).deleteUserByEmailOnOtherRealms(any(), any())
        // The stale row is still replaced through the benign ResolveExistingAuthCleanup collapse.
        Mockito.verify(userPersistence).replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
      }

      @Test
      fun `SSO login with a previous Keycloak identity still sweeps other realms to resume migration`() {
        val incomingUser =
          AuthenticatedUser()
            .withUserId(existingUserId)
            .withEmail(email)
            .withAuthUserId(newAuthUserId)
            .withAuthProvider(AuthProvider.KEYCLOAK)

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(incomingUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId)).thenReturn(Optional.of(incomingUser))
        whenever(userPersistence.listAuthUsersForUser(existingUserId)).thenReturn(
          listOf(
            AuthUser().withUserId(existingUserId).withAuthUserId(newAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK),
            AuthUser().withUserId(existingUserId).withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK),
          ),
        )
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(Optional.of(organization))

        val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Assertions.assertFalse(response.newUserCreated)
        Assertions.assertEquals(existingUserId, response.userRead.userId)
        // A previous Keycloak identity is the signal that this resumes an interrupted SSO migration -
        // the destructive cross-realm sweep must still run.
        Mockito.verify(externalUserService).deleteUserByEmailOnOtherRealms(email, ssoRealm)
        Mockito.verify(userPersistence).replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
      }

      @Test
      fun testSsoLoginWithInvalidDomainRejectedBeforeExistingUserMigration() {
        whenever(featureFlagClient.boolVariation(eq(BypassSsoDomainValidationEnforcement), any()))
          .thenReturn(false)
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(organization),
        )
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(
            listOf(
              OrganizationEmailDomain()
                .withOrganizationId(organization.organizationId)
                .withEmailDomain("attacker.com"),
            ),
          )

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.getUser(existingUserId)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(
            listOf(
              AuthUser()
                .withAuthUserId(existingAuthUserId)
                .withAuthProvider(AuthProvider.KEYCLOAK),
            ),
          )
        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(realm)
        whenever(ssoConfigService.getSsoConfigByRealmName(realm)).thenReturn(null)

        val existingAuthedUser =
          AuthenticatedUserConverter.toAuthenticatedUser(existingUser!!, existingAuthUserId, AuthProvider.KEYCLOAK)
        whenever(applicationService.listApplicationsByUser(existingAuthedUser))
          .thenReturn(listOf(Application().withId("app_id")))
        whenever(
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
          ),
        ).thenReturn(WorkspaceReadList().workspaces(listOf(WorkspaceRead().workspaceId(UUID.randomUUID()))))
        whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId))
          .thenReturn(listOf(UserPermission().withUser(User().withUserId(UUID.randomUUID()))))

        Assertions.assertThrows(OperationNotAllowedException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        }

        Mockito.verify(externalUserService, Mockito.never()).deleteUserByExternalId(newAuthUserId, ssoRealm)
        Mockito
          .verify(userPersistence, Mockito.never())
          .replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
        Mockito.verify(externalUserService, Mockito.never()).deleteUserByEmailOnOtherRealms(email, ssoRealm)
        Mockito.verify(applicationService, Mockito.never()).deleteApplication(existingAuthedUser, "app_id")
        Mockito.verify(permissionHandler, Mockito.never()).createPermission(any())
      }

      @Test
      fun testSsoLoginWithMismatchedDomainAllowedWhenOrganizationIsBypassed() {
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(organization),
        )
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(
            listOf(
              OrganizationEmailDomain()
                .withOrganizationId(organization.organizationId)
                .withEmailDomain("attacker.com"),
            ),
          )

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.of<AuthenticatedUser>(jwtUser!!))

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        // Login succeeds despite domain mismatch — no exception thrown
        Assertions.assertNotNull(result)
        Mockito.verify(featureFlagClient).boolVariation(
          BypassSsoDomainValidationEnforcement,
          FeatureFlagOrganization(organization.organizationId),
        )
      }

      @Test
      fun testSsoLoginWithMismatchedDomainRejectedWhenOrganizationIsNotBypassed() {
        whenever(featureFlagClient.boolVariation(eq(BypassSsoDomainValidationEnforcement), any()))
          .thenReturn(false)
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(organization),
        )
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(
            listOf(
              OrganizationEmailDomain()
                .withOrganizationId(organization.organizationId)
                .withEmailDomain("attacker.com"),
            ),
          )

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.of<AuthenticatedUser>(jwtUser!!))

        Assertions.assertThrows(OperationNotAllowedException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        }
        Mockito.verify(externalUserService, Mockito.never()).deleteUserByExternalId(newAuthUserId, ssoRealm)
      }

      @Test
      fun testSsoLoginWithNoClaimedDomainsAllowedWhenOrganizationIsBypassed() {
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(organization),
        )
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(emptyList())

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.of<AuthenticatedUser>(jwtUser!!))

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        // Login succeeds — no exception thrown
        Assertions.assertNotNull(result)
      }

      @Test
      fun testSsoLoginWithNoClaimedDomainsRejectedWhenOrganizationIsNotBypassed() {
        whenever(featureFlagClient.boolVariation(eq(BypassSsoDomainValidationEnforcement), any()))
          .thenReturn(false)
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(organization),
        )
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(emptyList())

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.of<AuthenticatedUser>(jwtUser!!))

        Assertions.assertThrows(OperationNotAllowedException::class.java) {
          userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        }
        Mockito.verify(externalUserService, Mockito.never()).deleteUserByExternalId(newAuthUserId, ssoRealm)
      }

      @Test
      fun testSsoLoginWithMatchingDomainAllowedWhenOrganizationIsNotBypassed() {
        whenever(featureFlagClient.boolVariation(eq(BypassSsoDomainValidationEnforcement), any()))
          .thenReturn(false)
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(organization),
        )
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(
            listOf(
              OrganizationEmailDomain()
                .withOrganizationId(organization.organizationId)
                .withEmailDomain("airbyte.io"),
            ),
          )

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.of<AuthenticatedUser>(jwtUser!!))

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Assertions.assertNotNull(result)
        Mockito.verify(featureFlagClient, Mockito.never()).boolVariation(
          eq(BypassSsoDomainValidationEnforcement),
          any(),
        )
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
        whenever(organizationService.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(organization),
        )
        whenever(organizationEmailDomainService.findByOrganizationId(organization.organizationId))
          .thenReturn(emptyList())

        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(ssoRealm)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.of<AuthenticatedUser>(jwtUser!!))

        val result = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Assertions.assertNotNull(result)
        Mockito.verify(externalUserService, Mockito.never()).deleteUserByExternalId(any(), any())
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
        defaultWorkspace = WorkspaceRead().workspaceId(workspaceId)
        whenever(userPersistence.getUserByAuthId(any()))
          .thenReturn(Optional.empty<AuthenticatedUser>())
        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(newAuthedUser)
        whenever(uuidSupplier.get()).thenReturn(newUserId)
        whenever(userPersistence.getUser(newUserId)).thenReturn(Optional.of<User>(newUser!!))
        whenever(
          resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(any()),
        ).thenReturn(defaultWorkspace)
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
          whenever(organizationEmailDomainService.findByEmailDomain(emailDomain))
            .thenReturn(
              listOf<OrganizationEmailDomain>(
                OrganizationEmailDomain()
                  .withOrganizationId(domainRestrictedToOrgId)
                  .withEmailDomain(emailDomain),
              ),
            )
        }

        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn(authRealm)
        if (authRealm != null) {
          whenever(organizationService.getOrganizationBySsoConfigRealm(authRealm)).thenReturn(
            Optional.of<Organization>(
              organization,
            ),
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
          whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId))
            .thenReturn(
              mutableListOf(),
            )
        } else {
          // add a pre-existing admin user for the org if this isn't the first user
          val existingUserPermission =
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))

          whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId))
            .thenReturn(listOf<UserPermission>(existingUserPermission))
        }

        if (isDefaultWorkspaceForOrgPresent) {
          whenever(
            workspacesHandler.listWorkspacesInOrganization(
              ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
            ),
          ).thenReturn(
            WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)),
          )
          if (newUser!!.defaultWorkspaceId == null) {
            newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId
          }
        } else {
          whenever(
            workspacesHandler.listWorkspacesInOrganization(
              any<ListWorkspacesInOrganizationRequestBody>(),
            ),
          ).thenReturn(WorkspaceReadList().workspaces(mutableListOf<@Valid WorkspaceRead?>()))
        }

        val apiAuthProvider = authProvider.convertTo<io.airbyte.api.model.generated.AuthProvider>()

        if (domainRestrictedToOrgId != null && (authRealm == null || domainRestrictedToOrgId !== organization.organizationId)) {
          Assertions.assertThrows(
            SSORequiredProblem::class.java,
          ) {
            userHandler.getOrCreateUserByAuthId(
              UserAuthIdRequestBody().authUserId(newAuthUserId),
            )
          }
          Mockito
            .verify(userPersistence, Mockito.never())
            .createAuthenticatedUserIfNoScimMapping(any())
          if (authRealm != null) {
            Mockito.verify(externalUserService, Mockito.never()).deleteUserByExternalId(newAuthedUser!!.authUserId, authRealm)
          }
          return
        }

        val response =
          userHandler.getOrCreateUserByAuthId(
            UserAuthIdRequestBody().authUserId(newAuthUserId),
          )

        val userPersistenceInOrder = Mockito.inOrder(userPersistence)

        Assertions.assertTrue(response.newUserCreated)
        verifyCreatedUser(authProvider, userPersistenceInOrder)
        verifyUserRes(response, apiAuthProvider)
        verifyInstanceAdminPermissionCreation(initialUserEmail, initialUserPresent)
        verifyOrganizationPermissionCreation(authRealm, isFirstOrgUser)
        verifyDefaultWorkspaceCreation(isDefaultWorkspaceForOrgPresent, userPersistenceInOrder)
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
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn("airbyte-realm")
        whenever(organizationService.getOrganizationBySsoConfigRealm("airbyte-realm")).thenReturn(Optional.of(organization))
        whenever(ssoConfigService.getSsoConfig(organization.organizationId)).thenReturn(
          SsoConfig().withDefaultRole(configuredDefaultRole),
        )
        whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId)).thenReturn(
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          ),
        )
        whenever(
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
          ),
        ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Mockito.verify(permissionHandler).createPermission(
          Permission()
            .withPermissionType(expectedPermissionType)
            .withOrganizationId(organization.organizationId)
            .withUserId(newUserId),
        )
      }

      @Test
      fun testNewSsoUserCreationFallsBackToMemberWhenConfigurableSsoDefaultRoleFlagOff() {
        // Flag OFF (dark launch): the configured EDITOR role must be ignored and provisioning must
        // fall back to ORGANIZATION_MEMBER, preserving pre-feature behavior.
        whenever(featureFlagClient.boolVariation(eq(ConfigurableSsoDefaultRole), any())).thenReturn(false)
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn("airbyte-realm")
        whenever(organizationService.getOrganizationBySsoConfigRealm("airbyte-realm")).thenReturn(Optional.of(organization))
        whenever(ssoConfigService.getSsoConfig(organization.organizationId)).thenReturn(
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR),
        )
        whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId)).thenReturn(
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          ),
        )
        whenever(
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
          ),
        ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Mockito.verify(permissionHandler).createPermission(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
            .withOrganizationId(organization.organizationId)
            .withUserId(newUserId),
        )
      }

      @Test
      fun testNewSsoUserCreationFallsBackToAdminWhenRbacRolesEntitlementIsAbsent() {
        whenever(
          ssoRbacEntitlementChecker.check(OrganizationId(organization.organizationId)),
        ).thenReturn(EntitlementResult(RbacRolesEntitlement.featureId, false))
        whenever(featureFlagClient.boolVariation(eq(ConfigurableSsoDefaultRole), any())).thenReturn(false)
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn("airbyte-realm")
        whenever(organizationService.getOrganizationBySsoConfigRealm("airbyte-realm")).thenReturn(Optional.of(organization))
        whenever(ssoConfigService.getSsoConfig(organization.organizationId)).thenReturn(
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR),
        )
        whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId)).thenReturn(
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          ),
        )
        whenever(
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
          ),
        ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Mockito.verify(permissionHandler).createPermission(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organization.organizationId)
            .withUserId(newUserId),
        )
        Mockito.verify(ssoRbacEntitlementChecker).check(OrganizationId(organization.organizationId))
        Mockito.verify(featureFlagClient, Mockito.never()).boolVariation(
          eq(ConfigurableSsoDefaultRole),
          any(),
        )
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
        whenever(
          ssoRbacEntitlementChecker.check(OrganizationId(organization.organizationId)),
        ).thenReturn(
          EntitlementResult(
            RbacRolesEntitlement.featureId,
            false,
            isEntitlementCheckSuccessful = false,
          ),
        )
        whenever(featureFlagClient.boolVariation(eq(ConfigurableSsoDefaultRole), any()))
          .thenReturn(configurableRoleEnabled)
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn("airbyte-realm")
        whenever(organizationService.getOrganizationBySsoConfigRealm("airbyte-realm")).thenReturn(Optional.of(organization))
        whenever(ssoConfigService.getSsoConfig(organization.organizationId)).thenReturn(
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR),
        )
        whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId)).thenReturn(
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          ),
        )
        whenever(
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
          ),
        ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Mockito.verify(permissionHandler).createPermission(
          Permission()
            .withPermissionType(expectedPermissionType)
            .withOrganizationId(organization.organizationId)
            .withUserId(newUserId),
        )
        Mockito.verify(ssoRbacEntitlementChecker).check(OrganizationId(organization.organizationId))
        Mockito.verify(featureFlagClient).boolVariation(
          ConfigurableSsoDefaultRole,
          FeatureFlagOrganization(organization.organizationId),
        )
      }

      @Test
      fun testNewSsoUserCreationDefaultsToMemberWhenConfiguredDefaultRoleIsNull() {
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn("airbyte-realm")
        whenever(organizationService.getOrganizationBySsoConfigRealm("airbyte-realm")).thenReturn(Optional.of(organization))
        whenever(ssoConfigService.getSsoConfig(organization.organizationId)).thenReturn(SsoConfig())
        whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId)).thenReturn(
          listOf(
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)),
          ),
        )
        whenever(
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
          ),
        ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Mockito.verify(permissionHandler).createPermission(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
            .withOrganizationId(organization.organizationId)
            .withUserId(newUserId),
        )
      }

      @Test
      fun testFirstNewSsoUserCreationAlwaysGrantsAdmin() {
        newAuthedUser!!.authProvider = AuthProvider.KEYCLOAK
        whenever(jwtUserAuthenticationResolver.resolveRealm()).thenReturn("airbyte-realm")
        whenever(organizationService.getOrganizationBySsoConfigRealm("airbyte-realm")).thenReturn(Optional.of(organization))
        whenever(ssoConfigService.getSsoConfig(organization.organizationId)).thenReturn(
          SsoConfig().withDefaultRole(Permission.PermissionType.ORGANIZATION_EDITOR),
        )
        whenever(permissionHandler.listPermissionsForOrganization(organization.organizationId)).thenReturn(mutableListOf())
        whenever(
          workspacesHandler.listWorkspacesInOrganization(
            ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
          ),
        ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)))
        newUser!!.defaultWorkspaceId = defaultWorkspace!!.workspaceId

        userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))

        Mockito.verify(permissionHandler).createPermission(
          Permission()
            .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
            .withOrganizationId(organization.organizationId)
            .withUserId(newUserId),
        )
      }

      private fun verifyCreatedUser(
        expectedAuthProvider: AuthProvider?,
        inOrder: InOrder,
      ) {
        inOrder
          .verify(userPersistence)
          .createAuthenticatedUserIfNoScimMapping(
            argThat { user: AuthenticatedUser? ->
              user!!.userId == newUserId &&
                newEmail == user.email &&
                newAuthUserId == user.authUserId &&
                user.authProvider == expectedAuthProvider
            },
          )
      }

      private fun verifyDefaultWorkspaceCreation(
        isDefaultWorkspaceForOrgPresent: Boolean,
        inOrder: InOrder,
      ) {
        // No need to deal with other vars because SSO users and first org users etc. are all directed
        // through the same codepath now.
        if (!isDefaultWorkspaceForOrgPresent) {
          // create a default workspace for the org if one doesn't yet exist
          Mockito.verify(resourceBootstrapHandler).bootStrapWorkspaceForCurrentUser(
            any<WorkspaceCreateWithId>(),
          )

          // if a workspace was created, verify that the user's defaultWorkspaceId was updated
          // and that a workspaceAdmin permission was created for them.
          inOrder
            .verify(userPersistence)
            .writeUser(
              argThat { user: User? ->
                user!!.defaultWorkspaceId.equals(
                  workspaceId,
                )
              },
            )
        } else {
          // never create an additional workspace for the org if one already exists.
          Mockito
            .verify(resourceBootstrapHandler, Mockito.never())
            .bootStrapWorkspaceForCurrentUser(
              any<WorkspaceCreateWithId>(),
            )
        }
      }

      private fun verifyUserRes(
        userRes: UserGetOrCreateByAuthIdResponse,
        expectedAuthProvider: io.airbyte.api.model.generated.AuthProvider?,
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
          Mockito
            .verify(permissionHandler, Mockito.never())
            .createPermission(
              argThat { permission: Permission? ->
                permission!!.permissionType ==
                  Permission.PermissionType.INSTANCE_ADMIN
              },
            )
          Mockito.verify(permissionHandler, Mockito.never()).grantInstanceAdmin(any())
        } else {
          // otherwise, instance_admin permission should be created
          Mockito.verify(permissionHandler).grantInstanceAdmin(any())
        }
      }

      private fun verifyOrganizationPermissionCreation(
        ssoRealm: String?,
        isFirstOrgUser: Boolean,
      ) {
        // if the SSO Realm is null, no organization permission should be created
        if (ssoRealm == null) {
          Mockito.verify(permissionHandler, Mockito.never()).createPermission(
            argThat { permission: Permission? ->
              permission!!.permissionType ==
                Permission.PermissionType.ORGANIZATION_ADMIN
            },
          )
        } else {
          val expectedPermissionType =
            if (isFirstOrgUser) {
              Permission.PermissionType.ORGANIZATION_ADMIN
            } else {
              Permission.PermissionType.ORGANIZATION_MEMBER
            }
          // otherwise, organization permission should be created for the associated user and org.
          Mockito.verify(permissionHandler).createPermission(
            Permission()
              .withPermissionType(expectedPermissionType)
              .withOrganizationId(organization.organizationId)
              .withUserId(newUserId),
          )
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
