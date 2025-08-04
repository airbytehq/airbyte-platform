/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.ResourceBootstrapHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
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
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.RestrictLoginsForSSODomains
import io.airbyte.featureflag.TestClient
import io.airbyte.validation.json.JsonValidationException
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
import org.mockito.InOrder
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.Arrays
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Stream

class UserHandlerTest {
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var userHandler: UserHandler
  private lateinit var userPersistence: UserPersistence

  lateinit var permissionHandler: PermissionHandler
  lateinit var workspacesHandler: WorkspacesHandler
  lateinit var organizationPersistence: OrganizationPersistence
  lateinit var organizationEmailDomainService: OrganizationEmailDomainService
  lateinit var organizationsHandler: OrganizationsHandler
  lateinit var jwtUserAuthenticationResolver: JwtUserAuthenticationResolver
  lateinit var initialUserConfig: InitialUserConfig
  lateinit var externalUserService: ExternalUserService
  lateinit var applicationService: ApplicationService
  lateinit var featureFlagClient: FeatureFlagClient

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
    organizationPersistence = mock()
    organizationEmailDomainService = mock()
    organizationsHandler = mock()
    uuidSupplier = mock()
    jwtUserAuthenticationResolver = mock()
    initialUserConfig = mockk(relaxed = true)
    resourceBootstrapHandler = mock()
    externalUserService = mock()
    applicationService = mock()
    featureFlagClient = mock<TestClient>()

    whenever(featureFlagClient.boolVariation(eq(RestrictLoginsForSSODomains), any()))
      .thenReturn(true)

    userHandler =
      UserHandler(
        userPersistence,
        externalUserService,
        organizationPersistence,
        organizationEmailDomainService,
        Optional.of(applicationService),
        permissionHandler,
        workspacesHandler,
        uuidSupplier,
        jwtUserAuthenticationResolver,
        Optional.of(initialUserConfig),
        resourceBootstrapHandler,
        featureFlagClient,
      )
  }

  @Test
  @Throws(Exception::class)
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
  @Throws(Exception::class)
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
  @Throws(Exception::class)
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
    @ParameterizedTest
    @EnumSource(AuthProvider::class)
    @Throws(Exception::class)
    fun authIdExists(authProvider: AuthProvider) {
      // set the auth provider for the existing user to match the test case
      user.setAuthProvider(authProvider)

      // authUserId is for the existing user
      val authUserId = user.getAuthUserId()
      val apiAuthProvider =
        authProvider.convertTo<io.airbyte.api.model.generated.AuthProvider>()

      whenever(jwtUserAuthenticationResolver.resolveUser(authUserId)).thenReturn(user)
      whenever(userPersistence.getUserByAuthId(authUserId))
        .thenReturn(Optional.of<AuthenticatedUser>(user))

      val response = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(authUserId))
      val userRead = response.getUserRead()

      Assertions.assertEquals(userRead.getUserId(), userId)
      Assertions.assertEquals(userRead.getEmail(), USER_EMAIL)
      Assertions.assertEquals(response.getAuthUserId(), authUserId)
      Assertions.assertEquals(response.getAuthProvider(), apiAuthProvider)
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
      @Throws(Exception::class)
      fun testNonSSOSignInEmailExistsThrowsError(isExistingUserSSO: Boolean) {
        whenever(jwtUserAuthenticationResolver.resolveUser(newAuthUserId)).thenReturn(jwtUser)
        whenever(userPersistence.getUserByAuthId(newAuthUserId))
          .thenReturn(Optional.empty<AuthenticatedUser>())
        whenever(userPersistence.getUserByEmail(email)).thenReturn(Optional.of<User>(existingUser!!))
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(listOf<AuthUser>(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK)))
        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(realm)

        if (isExistingUserSSO) {
          whenever(organizationPersistence.getSsoConfigByRealmName(realm)).thenReturn(
            Optional.of<SsoConfig>(
              SsoConfig(),
            ),
          )
        }

        Assertions.assertThrows(
          UserAlreadyExistsProblem::class.java,
        ) { userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId)) }
      }

      @Test
      @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, PermissionRedundantException::class)
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
        whenever(uuidSupplier.get()).thenReturn(newUser.getUserId())
        whenever(userPersistence.getUser(newUser.getUserId())).thenReturn(Optional.of<User>(toUser(newUser)))

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        Assertions.assertTrue(res.getNewUserCreated())
        Assertions.assertEquals(res.getUserRead().getUserId(), newUser.getUserId())
        Assertions.assertEquals(res.getUserRead().getEmail(), email)
        Assertions.assertEquals(res.getAuthUserId(), newAuthUserId)

        Mockito.verify(userPersistence).writeUser(defaultUser.withEmail(""))
        Mockito
          .verify(userPersistence)
          .writeAuthenticatedUser(
            argThat { user: AuthenticatedUser? ->
              user!!.getEmail() ==
                jwtUser!!.getEmail() &&
                user.getAuthUserId() == jwtUser!!.getAuthUserId()
            },
          )
      }

      @Test
      @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, PermissionRedundantException::class)
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
        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(listOf<AuthUser>(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK)))
        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(null)

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        Assertions.assertFalse(res.getNewUserCreated())
        Assertions.assertEquals(res.getUserRead().getUserId(), existingUserId)

        // verify auth user is replaced
        Mockito.verify(userPersistence).replaceAuthUserForUserId(existingUserId, newAuthUserId, AuthProvider.KEYCLOAK)
      }

      @ParameterizedTest
      @MethodSource("io.airbyte.commons.server.handlers.UserHandlerTest#ssoSignInArgsProvider")
      @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, PermissionRedundantException::class)
      fun testSSOSignInEmailExistsMigratesAuthUser(
        isExistingUserSSO: Boolean,
        doesExistingUserHaveOrgPermission: Boolean,
      ) {
        whenever(organizationPersistence.getOrganizationBySsoConfigRealm(ssoRealm)).thenReturn(
          Optional.of<Organization>(
            organization,
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
          whenever(organizationPersistence.getSsoConfigByRealmName(ssoRealm)).thenReturn(
            Optional.of<SsoConfig>(
              SsoConfig(),
            ),
          )

          Assertions.assertThrows(
            UserAlreadyExistsProblem::class.java,
          ) { userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId)) }
          return
        }

        whenever(externalUserService.getRealmByAuthUserId(existingAuthUserId)).thenReturn(realm)
        whenever(organizationPersistence.getSsoConfigByRealmName(realm)).thenReturn(Optional.empty<SsoConfig>())

        whenever(userPersistence.listAuthUsersForUser(existingUserId))
          .thenReturn(listOf<AuthUser>(AuthUser().withAuthUserId(existingAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK)))

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
              .listWorkspacesInOrganization(ListWorkspacesInOrganizationRequestBody().organizationId(organization.getOrganizationId())),
          ).thenReturn(WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(WorkspaceRead().workspaceId(UUID.randomUUID()))))

        if (doesExistingUserHaveOrgPermission) {
          Mockito
            .`when`(permissionHandler.listPermissionsForOrganization(organization.getOrganizationId()))
            .thenReturn(listOf<UserPermission>(UserPermission().withUser(existingUser)))
        } else {
          Mockito
            .`when`(permissionHandler.listPermissionsForOrganization(organization.getOrganizationId()))
            .thenReturn(listOf<UserPermission>(UserPermission().withUser(User().withUserId(UUID.randomUUID()))))
        }

        val res = userHandler.getOrCreateUserByAuthId(UserAuthIdRequestBody().authUserId(newAuthUserId))
        Assertions.assertFalse(res.getNewUserCreated())

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
              .withOrganizationId(organization.getOrganizationId())
              .withUserId(existingUserId),
          )
        }

        // verify user read
        val userRead = res.getUserRead()
        Assertions.assertEquals(userRead.getUserId(), existingUserId)
        Assertions.assertEquals(userRead.getEmail(), email)
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
      @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
      fun setUp() {
        newAuthedUser = AuthenticatedUser().withUserId(newUserId).withEmail(newEmail).withAuthUserId(newAuthUserId)
        newUser = AuthenticatedUserConverter.toUser(newAuthedUser!!)
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
      @Throws(Exception::class)
      fun testNewUserCreation(
        authProvider: AuthProvider,
        authRealm: String?,
        initialUserEmail: String?,
        initialUserPresent: Boolean,
        isFirstOrgUser: Boolean,
        isDefaultWorkspaceForOrgPresent: Boolean,
        domainRestrictedToOrgId: UUID?,
      ) {
        newAuthedUser!!.setAuthProvider(authProvider)

        if (domainRestrictedToOrgId != null) {
          val emailDomain =
            newUser!!
              .getEmail()
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
          whenever(organizationPersistence.getOrganizationBySsoConfigRealm(authRealm)).thenReturn(
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
              organizationPersistence,
              organizationEmailDomainService,
              Optional.of<ApplicationService>(applicationService),
              permissionHandler,
              workspacesHandler,
              uuidSupplier,
              jwtUserAuthenticationResolver,
              Optional.empty<InitialUserConfig>(),
              resourceBootstrapHandler,
              featureFlagClient,
            )
        }

        if (isFirstOrgUser) {
          whenever(permissionHandler.listPermissionsForOrganization(organization.getOrganizationId()))
            .thenReturn(
              mutableListOf(),
            )
        } else {
          // add a pre-existing admin user for the org if this isn't the first user
          val existingUserPermission =
            UserPermission()
              .withUser(existingUser)
              .withPermission(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))

          whenever(permissionHandler.listPermissionsForOrganization(organization.getOrganizationId()))
            .thenReturn(listOf<UserPermission>(existingUserPermission))
        }

        if (isDefaultWorkspaceForOrgPresent) {
          whenever(
            workspacesHandler.listWorkspacesInOrganization(
              ListWorkspacesInOrganizationRequestBody().organizationId(organization.getOrganizationId()),
            ),
          ).thenReturn(
            WorkspaceReadList().workspaces(listOf<@Valid WorkspaceRead?>(defaultWorkspace)),
          )
          if (newUser!!.getDefaultWorkspaceId() == null) {
            newUser!!.setDefaultWorkspaceId(defaultWorkspace!!.getWorkspaceId())
          }
        } else {
          whenever(
            workspacesHandler.listWorkspacesInOrganization(
              any<ListWorkspacesInOrganizationRequestBody>(),
            ),
          ).thenReturn(WorkspaceReadList().workspaces(mutableListOf<@Valid WorkspaceRead?>()))
        }

        val apiAuthProvider = authProvider.convertTo<io.airbyte.api.model.generated.AuthProvider>()

        if (domainRestrictedToOrgId != null && (authRealm == null || domainRestrictedToOrgId !== organization.getOrganizationId())) {
          Assertions.assertThrows(
            SSORequiredProblem::class.java,
          ) {
            userHandler.getOrCreateUserByAuthId(
              UserAuthIdRequestBody().authUserId(newAuthUserId),
            )
          }
          Mockito
            .verify(userPersistence, Mockito.never())
            .writeAuthenticatedUser(any())
          if (authRealm != null) {
            Mockito.verify(externalUserService).deleteUserByExternalId(newAuthedUser!!.getAuthUserId(), authRealm)
          }
          return
        }

        val response =
          userHandler.getOrCreateUserByAuthId(
            UserAuthIdRequestBody().authUserId(newAuthUserId),
          )

        val userPersistenceInOrder = Mockito.inOrder(userPersistence)

        Assertions.assertTrue(response.getNewUserCreated())
        verifyCreatedUser(authProvider, userPersistenceInOrder)
        verifyUserRes(response, apiAuthProvider)
        verifyInstanceAdminPermissionCreation(initialUserEmail, initialUserPresent)
        verifyOrganizationPermissionCreation(authRealm, isFirstOrgUser)
        verifyDefaultWorkspaceCreation(isDefaultWorkspaceForOrgPresent, userPersistenceInOrder)
      }

      @Throws(IOException::class)
      private fun verifyCreatedUser(
        expectedAuthProvider: AuthProvider?,
        inOrder: InOrder,
      ) {
        inOrder
          .verify(userPersistence)
          .writeAuthenticatedUser(
            argThat { user: AuthenticatedUser? ->
              user!!.getUserId() == newUserId &&
                newEmail == user.getEmail() &&
                newAuthUserId == user.getAuthUserId() &&
                user.getAuthProvider() == expectedAuthProvider
            },
          )
      }

      @Throws(IOException::class)
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
                user!!.getDefaultWorkspaceId().equals(
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
        val userRead = userRes.getUserRead()
        Assertions.assertEquals(userRead.getUserId(), newUserId)
        Assertions.assertEquals(userRead.getEmail(), newEmail)
        Assertions.assertEquals(userRes.getAuthUserId(), newAuthUserId)
        Assertions.assertEquals(userRes.getAuthProvider(), expectedAuthProvider)
      }

      @Throws(Exception::class)
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
                permission!!.getPermissionType() ==
                  Permission.PermissionType.INSTANCE_ADMIN
              },
            )
          Mockito.verify(permissionHandler, Mockito.never()).grantInstanceAdmin(any())
        } else {
          // otherwise, instance_admin permission should be created
          Mockito.verify(permissionHandler).grantInstanceAdmin(any())
        }
      }

      @Throws(IOException::class, JsonValidationException::class, PermissionRedundantException::class)
      private fun verifyOrganizationPermissionCreation(
        ssoRealm: String?,
        isFirstOrgUser: Boolean,
      ) {
        // if the SSO Realm is null, no organization permission should be created
        if (ssoRealm == null) {
          Mockito.verify(permissionHandler, Mockito.never()).createPermission(
            argThat { permission: Permission? ->
              permission!!.getPermissionType() ==
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
              .withOrganizationId(organization.getOrganizationId())
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
      override fun provideArguments(context: ExtensionContext?): Stream<out Arguments?>? {
        val authProviders = Arrays.asList(*AuthProvider.entries.toTypedArray())
        val authRealms = mutableListOf("airbyte-realm", null)
        val initialUserEmails = Arrays.asList<String?>(null, "", "other@gmail.com", "new@gmail.com")
        val domainRestrictedToOrgIds = Arrays.asList<UUID?>(null, UUID.randomUUID(), organization.getOrganizationId())
        val initialUserConfigPresent = mutableListOf<Boolean?>(true, false)
        val isFirstOrgUser = mutableListOf<Boolean?>(true, false)
        val isDefaultWorkspaceForOrgPresent = mutableListOf<Boolean?>(true, false)

        // return all permutations of the above input lists so that we can test all combinations.
        return authProviders
          .stream()
          .flatMap { authProvider: AuthProvider? ->
            authRealms
              .stream()
              .flatMap { authRealm: String? ->
                initialUserEmails
                  .stream()
                  .flatMap { email: String? ->
                    initialUserConfigPresent
                      .stream()
                      .flatMap { initialUserPresent: Boolean? ->
                        isFirstOrgUser
                          .stream()
                          .flatMap { firstOrgUser: Boolean? ->
                            isDefaultWorkspaceForOrgPresent
                              .stream()
                              .flatMap { orgWorkspacePresent: Boolean? ->
                                domainRestrictedToOrgIds
                                  .stream()
                                  .flatMap { domainRestrictedToOrgId: UUID? ->
                                    Stream.of(
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
          }
      }
    }

    @JvmStatic
    private fun ssoSignInArgsProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>( // Existing user is already an SSO user (will error):
        Arguments.of(true, false), // Existing user is regular user (will migrate):
        Arguments.of(false, true),
        Arguments.of(false, false),
      )
  }
}
