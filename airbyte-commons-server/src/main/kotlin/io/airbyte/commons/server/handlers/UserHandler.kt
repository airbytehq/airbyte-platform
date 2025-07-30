/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationUserRead
import io.airbyte.api.model.generated.OrganizationUserReadList
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.UserAuthIdRequestBody
import io.airbyte.api.model.generated.UserEmailRequestBody
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.model.generated.UserIdRequestBody
import io.airbyte.api.model.generated.UserRead
import io.airbyte.api.model.generated.UserStatus
import io.airbyte.api.model.generated.UserUpdate
import io.airbyte.api.model.generated.UserWithPermissionInfoRead
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoRead
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoReadList
import io.airbyte.api.problems.model.generated.ProblemEmailData
import io.airbyte.api.problems.throwable.generated.SSORequiredProblem
import io.airbyte.api.problems.throwable.generated.UserAlreadyExistsProblem
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.config.InitialUserConfig
import io.airbyte.commons.auth.support.UserAuthenticationResolver
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.handlers.helpers.getDefaultWorkspaceName
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthUser
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Organization
import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.config.Permission
import io.airbyte.config.User
import io.airbyte.config.UserPermission
import io.airbyte.config.WorkspaceUserAccessInfo
import io.airbyte.config.helpers.AuthenticatedUserConverter.toAuthenticatedUser
import io.airbyte.config.helpers.AuthenticatedUserConverter.toUser
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.SQLOperationNotAllowedException
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.featureflag.EmailAttribute
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.RestrictLoginsForSSODomains
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import jakarta.validation.Valid
import org.jooq.exception.DataAccessException
import java.io.IOException
import java.util.Map
import java.util.Objects
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * UserHandler, provides basic CRUD operation access for users. Some are migrated from Cloud
 * UserHandler.
 */
@Singleton
open class UserHandler
  @VisibleForTesting
  constructor(
    private val userPersistence: UserPersistence,
    private val externalUserService: ExternalUserService,
    private val organizationPersistence: OrganizationPersistence,
    private val organizationEmailDomainService: OrganizationEmailDomainService,
    private val applicationService: Optional<ApplicationService>,
    private val permissionHandler: PermissionHandler,
    private val workspacesHandler: WorkspacesHandler,
    @param:Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
    private val userAuthenticationResolver: UserAuthenticationResolver,
    private val initialUserConfig: Optional<InitialUserConfig>,
    private val resourceBootstrapHandler: ResourceBootstrapHandlerInterface,
    private val featureFlagClient: FeatureFlagClient,
  ) {
    /**
     * Get a user by internal user ID.
     *
     * @param userIdRequestBody The internal user id to be queried.
     * @return The user.
     * @throws ConfigNotFoundException if unable to get the user.
     * @throws IOException if unable to get the user.
     * @throws JsonValidationException if unable to get the user.
     */
    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun getUser(userIdRequestBody: UserIdRequestBody): UserRead = buildUserRead(userIdRequestBody.userId)

    /**
     * Retrieves the user by auth ID.
     *
     * @param userAuthIdRequestBody The [UserAuthIdRequestBody] that contains the auth ID.
     * @return The user associated with the auth ID.
     * @throws IOException if unable to retrieve the user.
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun getUserByAuthId(userAuthIdRequestBody: UserAuthIdRequestBody): UserRead {
      val user = userPersistence.getUserByAuthId(userAuthIdRequestBody.authUserId)
      if (user.isPresent) {
        return buildUserRead(toUser(user.get()))
      } else {
        throw ConfigNotFoundException(ConfigNotFoundType.USER, String.format("User not found by auth request: %s", userAuthIdRequestBody))
      }
    }

    /**
     * Retrieves the user by email.
     *
     * @param userEmailRequestBody The [UserEmailRequestBody] that contains the email.
     * @return The user associated with the email.
     * @throws IOException if unable to retrieve the user.
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun getUserByEmail(userEmailRequestBody: UserEmailRequestBody): UserRead {
      val user = userPersistence.getUserByEmail(userEmailRequestBody.email)
      if (user.isPresent) {
        return buildUserRead(user.get())
      } else {
        throw ConfigNotFoundException(ConfigNotFoundType.USER, String.format("User not found by email request: %s", userEmailRequestBody))
      }
    }

    @Throws(ConfigNotFoundException::class, IOException::class)
    private fun buildUserRead(userId: UUID): UserRead {
      val user = userPersistence.getUser(userId)
      if (user.isEmpty) {
        throw ConfigNotFoundException(ConfigNotFoundType.USER, userId)
      }
      return buildUserRead(user.get())
    }

    private fun buildUserRead(user: User): UserRead =
      UserRead()
        .name(user.name)
        .userId(user.userId)
        .status(user.status?.convertTo<UserStatus>())
        .companyName(user.companyName)
        .email(user.email)
        .metadata(if (user.uiMetadata != null) user.uiMetadata else Map.of<Any, Any>())
        .news(user.news)
        .defaultWorkspaceId(user.defaultWorkspaceId)

    /**
     * Patch update a user object.
     *
     * @param userUpdate the user to update. Will only update requested fields as long as they are
     * supported.
     * @return Updated user.
     * @throws ConfigNotFoundException If user not found.
     * @throws IOException If user update was not successful.
     * @throws JsonValidationException If input json was not expected.
     */
    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun updateUser(userUpdate: UserUpdate): UserRead {
      val userRead = getUser(UserIdRequestBody().userId(userUpdate.userId))

      val user = buildUserInfo(userRead)

      // We do not allow update on these fields: userId and email
      var hasUpdate = false
      if (userUpdate.name != null) {
        user.name = userUpdate.name
        hasUpdate = true
      }

      if (userUpdate.companyName != null) {
        user.companyName = userUpdate.companyName
        hasUpdate = true
      }

      if (userUpdate.status != null) {
        user.status = userUpdate.status?.convertTo<User.Status>()
        hasUpdate = true
      }

      if (userUpdate.news != null) {
        user.news = userUpdate.news
        hasUpdate = true
      }

      if (userUpdate.defaultWorkspaceId != null) {
        user.defaultWorkspaceId = userUpdate.defaultWorkspaceId
        hasUpdate = true
      }

      if (userUpdate.metadata != null) {
        user.uiMetadata = Jsons.convertValue(userUpdate.metadata, JsonNode::class.java)
        hasUpdate = true
      }

      if (hasUpdate) {
        userPersistence.writeUser(user)
        return buildUserRead(userUpdate.userId)
      }
      throw IllegalArgumentException(
        "Patch update user is not successful because there is nothing to update, or requested updating fields are not supported.",
      )
    }

    private fun buildUserInfo(userRead: UserRead): User =
      User()
        .withName(userRead.name)
        .withUserId(userRead.userId)
        .withDefaultWorkspaceId(userRead.defaultWorkspaceId)
        .withStatus(
          userRead.status?.convertTo<User.Status>(),
        ).withCompanyName(userRead.companyName)
        .withEmail(userRead.email)
        .withUiMetadata(Jsons.jsonNode(if (userRead.metadata != null) userRead.metadata else Map.of<Any, Any>()))
        .withNews(userRead.news)

    /**
     * Deletes a User.
     *
     * @param userIdRequestBody The user to be deleted.
     * @throws IOException if unable to delete the user.
     * @throws ConfigNotFoundException if unable to delete the user.
     */
    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun deleteUser(userIdRequestBody: UserIdRequestBody) {
      val userRead = getUser(userIdRequestBody)
      deleteUser(userRead)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun deleteUser(userRead: UserRead) {
      val userUpdate =
        UserUpdate()
          .name(userRead.name)
          .userId(userRead.userId)
          .status(UserStatus.DISABLED)
          .companyName(userRead.companyName)
          .news(userRead.news)
      updateUser(userUpdate)
    }

    @Throws(IOException::class)
    fun listUsersInOrganization(organizationIdRequestBody: OrganizationIdRequestBody): OrganizationUserReadList {
      val organizationId = organizationIdRequestBody.organizationId
      val userPermissions = permissionHandler.listUsersInOrganization(organizationId)
      return buildOrganizationUserReadList(userPermissions, organizationId)
    }

    @Throws(IOException::class)
    fun listAccessInfoByWorkspaceId(workspaceIdRequestBody: WorkspaceIdRequestBody): WorkspaceUserAccessInfoReadList {
      val workspaceId = workspaceIdRequestBody.workspaceId
      val userAccessInfo = userPersistence.listWorkspaceUserAccessInfo(workspaceId)
      return buildWorkspaceUserAccessInfoReadList(userAccessInfo)
    }

    @Throws(IOException::class)
    fun listInstanceAdminUsers(): UserWithPermissionInfoReadList {
      val userPermissions = permissionHandler.listInstanceAdminUsers()
      return UserWithPermissionInfoReadList().users(
        userPermissions
          .stream()
          .map { userPermission: UserPermission ->
            UserWithPermissionInfoRead()
              .userId(userPermission.user.userId)
              .email(userPermission.user.email)
              .name(userPermission.user.name)
              .permissionId(userPermission.permission.permissionId)
          }.collect(Collectors.toList<@Valid UserWithPermissionInfoRead?>()),
      )
    }

    @Throws(IOException::class)
    private fun isAllowedDomain(email: String): Boolean {
      if (!featureFlagClient.boolVariation(
          RestrictLoginsForSSODomains,
          io.airbyte.featureflag.User(UUID.randomUUID(), EmailAttribute(email)),
        )
      ) {
        return true
      }

      val emailDomain = email.split("@".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[1]
      val restrictedForOrganizations = organizationEmailDomainService.findByEmailDomain(emailDomain)

      if (restrictedForOrganizations.isEmpty()) {
        return true
      }

      val currentSSOOrg = ssoOrganizationIfExists
      return currentSSOOrg.isPresent &&
        restrictedForOrganizations
          .stream()
          .anyMatch { orgEmailDomain: OrganizationEmailDomain -> orgEmailDomain.organizationId == currentSSOOrg.get().organizationId }
    }

    @Throws(IOException::class)
    private fun getExistingUserRealms(userId: UUID): List<String?> {
      val keycloakAuthUsers =
        userPersistence
          .listAuthUsersForUser(userId)
          .stream()
          .filter { authUser: AuthUser -> authUser.authProvider == AuthProvider.KEYCLOAK }
          .toList()

      // Note: it's important to reach out to keycloak here to validate that at least one auth user from
      // our db actually exists in keycloak.
      return keycloakAuthUsers
        .stream()
        .map { authUser: AuthUser -> externalUserService.getRealmByAuthUserId(authUser.authUserId) }
        .filter { obj: String? -> Objects.nonNull(obj) }
        .toList()
    }

    @Throws(IOException::class)
    private fun isAnyRealmSSO(realms: List<String?>): Boolean {
      for (realm in realms) {
        val ssoConfig = organizationPersistence.getSsoConfigByRealmName(realm)
        if (ssoConfig.isPresent) {
          return true
        }
      }

      return false
    }

    @Throws(IOException::class)
    private fun handleSSORestrictions(
      incomingJwtUser: AuthenticatedUser,
      authUserExists: Boolean,
    ) {
      val allowDomain = isAllowedDomain(incomingJwtUser.email)
      if (!allowDomain) {
        if (!authUserExists) {
          // Keep keycloak clean by deleting the user if it doesn't exist in our auth_user table is not
          // allowed to sign in
          val authRealm = userAuthenticationResolver.resolveRealm()
          if (authRealm != null) {
            externalUserService.deleteUserByExternalId(incomingJwtUser.authUserId, authRealm)
          }
        }
        throw SSORequiredProblem()
      }
    }

    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      PermissionRedundantException::class,
    )
    private fun handleNewUserLogin(incomingJwtUser: AuthenticatedUser): UserGetOrCreateByAuthIdResponse {
      val createdUser = createUserFromIncomingUser(incomingJwtUser)
      handleUserPermissionsAndWorkspace(createdUser)

      // refresh the user from the database in case anything changed during permission/workspace
      // modification
      val updatedUser =
        userPersistence
          .getUser(createdUser.userId)
          .orElseThrow {
            ConfigNotFoundException(
              ConfigNotFoundType.USER,
              createdUser.userId,
            )
          }

      return UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .authUserId(incomingJwtUser.authUserId)
        .authProvider(
          incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
        ).newUserCreated(true)
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    private fun handleRelinkAuthUser(
      existingUser: User,
      incomingJwtUser: AuthenticatedUser,
    ): UserGetOrCreateByAuthIdResponse {
      log.info { "Relinking auth user {} to orphaned existing user $incomingJwtUser.authUserId, existingUser.userId..." }
      userPersistence.replaceAuthUserForUserId(existingUser.userId, incomingJwtUser.authUserId, incomingJwtUser.authProvider)

      val updatedUser =
        userPersistence
          .getUser(existingUser.userId)
          .orElseThrow {
            ConfigNotFoundException(
              ConfigNotFoundType.USER,
              existingUser.userId,
            )
          }

      return UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .authUserId(incomingJwtUser.authUserId)
        .authProvider(
          incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
        ).newUserCreated(false)
    }

    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      PermissionRedundantException::class,
    )
    private fun handleFirstTimeSSOLogin(
      existingUser: User,
      incomingJwtUser: AuthenticatedUser,
    ): UserGetOrCreateByAuthIdResponse {
      log.info { "Migrating existing user $existingUser.userId to SSO..." }
      // (1) Revoke existing applications
      if (applicationService.isPresent) {
        val appService = applicationService.get()
        log.info { "Revoking existing applications for user $existingUser.userId..." }
        val authUsers = userPersistence.listAuthUsersForUser(existingUser.userId)
        for (authUser in authUsers) {
          val authedUser =
            toAuthenticatedUser(existingUser, authUser.authUserId, authUser.authProvider)
          val existingApplications = appService.listApplicationsByUser(authedUser)
          for (application in existingApplications) {
            appService.deleteApplication(authedUser, application.id)
            log.info(
              "Revoked application {} for user {} (auth user {})...",
              application.id,
              existingUser.userId,
              authUser.authUserId,
            )
          }
        }
      }

      // (2) Delete the user from other auth realms
      log.info { "Deleting user with email $existingUser.email from other auth realms..." }
      val newRealm = userAuthenticationResolver.resolveRealm()
      checkNotNull(newRealm) { "No new realm found for user " + existingUser.userId }
      externalUserService.deleteUserByEmailOnOtherRealms(existingUser.email, newRealm)

      // (3) Replace the existing auth user with the new one
      log.info { "Replacing existing auth users with new one ($incomingJwtUser.authUserId)..." }
      userPersistence.replaceAuthUserForUserId(existingUser.userId, incomingJwtUser.authUserId, incomingJwtUser.authProvider)

      log.info { "Done migrating user $existingUser.userId to SSO" }

      // (4) Return the user
      val userRead = buildUserRead(existingUser)
      handleUserPermissionsAndWorkspace(userRead)

      // refresh the user from the database in case anything changed during permission/workspace
      // modification
      val updatedUser =
        userPersistence
          .getUser(userRead.userId)
          .orElseThrow {
            ConfigNotFoundException(
              ConfigNotFoundType.USER,
              userRead.userId,
            )
          }

      return UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .authUserId(incomingJwtUser.authUserId)
        .authProvider(
          incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
        ).newUserCreated(false)
    }

    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      PermissionRedundantException::class,
    )
    fun getOrCreateUserByAuthId(userAuthIdRequestBody: UserAuthIdRequestBody): UserGetOrCreateByAuthIdResponse {
      val incomingJwtUser = resolveIncomingJwtUser(userAuthIdRequestBody)
      val existingAuthUser = userPersistence.getUserByAuthId(userAuthIdRequestBody.authUserId)

      // (1) Restrict logins for SSO domains
      handleSSORestrictions(incomingJwtUser, existingAuthUser.isPresent)

      // (2) Authenticate existing auth_user
      if (existingAuthUser.isPresent) {
        return UserGetOrCreateByAuthIdResponse()
          .userRead(buildUserRead(toUser(existingAuthUser.get())))
          .authUserId(userAuthIdRequestBody.authUserId)
          .authProvider(
            incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
          ).newUserCreated(false)
      }

      // (3) Handle non-existing auth_user
      var existingUserWithEmail = userPersistence.getUserByEmail(incomingJwtUser.email)
      if (existingUserWithEmail.isPresent && existingUserWithEmail.get().userId === DEFAULT_USER_ID) {
        // (Enterprise) If the email is already taken by the default user, we can safely clear it so the
        // real user can be created
        userPersistence.writeUser(existingUserWithEmail.get().withEmail(""))
        log.info { "Cleared email for default user on first login for $incomingJwtUser.email" }

        existingUserWithEmail = Optional.empty()
      }

      // (3a) Email has not been used before
      if (existingUserWithEmail.isEmpty) {
        return handleNewUserLogin(incomingJwtUser)
      }

      // (3b) A user with the same email already exists
      val existingUser = existingUserWithEmail.get()
      val existingUserRealms = getExistingUserRealms(existingUser.userId)

      // (3b0) The existing user does not exist in any auth realm, relink it
      // This can happen if, for example, keycloak state is cleared on an enterprise installation
      if (existingUserRealms.isEmpty()) {
        return handleRelinkAuthUser(existingUser, incomingJwtUser)
      }

      val isCurrentSignInSSO = ssoOrganizationIfExists.isPresent
      val isExistingUserSSOAuthed = isAnyRealmSSO(existingUserRealms)

      // (3b1) This is the first SSO sign in for the user, migrate it for SSO
      if (isCurrentSignInSSO && !isExistingUserSSOAuthed) {
        return handleFirstTimeSSOLogin(existingUser, incomingJwtUser)
      }

      // (3b2) This isn't a first-time SSO sign in and/or the user already exists
      val realm = userAuthenticationResolver.resolveRealm()
      if (realm != null) {
        externalUserService.deleteUserByExternalId(incomingJwtUser.authUserId, realm)
      }
      throw UserAlreadyExistsProblem(ProblemEmailData().email(existingUser.email))
    }

    private fun resolveIncomingJwtUser(userAuthIdRequestBody: UserAuthIdRequestBody): AuthenticatedUser {
      val authUserId = userAuthIdRequestBody.authUserId
      return userAuthenticationResolver.resolveUser(authUserId)
    }

    @Throws(ConfigNotFoundException::class, IOException::class)
    private fun createUserFromIncomingUser(incomingUser: AuthenticatedUser): UserRead {
      val userId = uuidGenerator.get()
      val user = incomingUser.withUserId(userId)

      log.debug { "Creating User: $user" }

      try {
        userPersistence.writeAuthenticatedUser(user)
      } catch (e: DataAccessException) {
        if (e.cause is SQLOperationNotAllowedException) {
          throw OperationNotAllowedException((e.cause as SQLOperationNotAllowedException).message)
        } else {
          throw IOException(e)
        }
      }
      return buildUserRead(userId)
    }

    @Throws(
      IOException::class,
      JsonValidationException::class,
      ConfigNotFoundException::class,
      PermissionRedundantException::class,
    )
    private fun handleUserPermissionsAndWorkspace(createdUser: UserRead) {
      createInstanceAdminPermissionIfInitialUser(createdUser)
      val ssoOrg = ssoOrganizationIfExists
      if (ssoOrg.isPresent) {
        // SSO users will have some additional logic but will ultimately call createDefaultWorkspaceForUser
        handleSsoUser(createdUser, ssoOrg.get())
      } else {
        // non-SSO users will just create a default workspace
        createDefaultWorkspaceForUser(createdUser, Optional.empty())
      }
    }

    @Throws(
      IOException::class,
      JsonValidationException::class,
      ConfigNotFoundException::class,
      PermissionRedundantException::class,
    )
    private fun handleSsoUser(
      user: UserRead,
      organization: Organization,
    ) {
      // look for any existing user permissions for this organization. exclude the default user that comes
      // with the Airbyte installation, since we want the first real SSO user to be the org admin.
      val orgPermissionsExcludingDefaultUser: List<UserPermission> =
        permissionHandler
          .listPermissionsForOrganization(organization.organizationId)
          .stream()
          .filter { userPermission: UserPermission -> userPermission.user.userId != DEFAULT_USER_ID }
          .toList()

      // If this is the first real user in the org, create a default workspace for them and make them an
      // org admin.
      if (orgPermissionsExcludingDefaultUser.isEmpty()) {
        createPermissionForUserAndOrg(user.userId, organization.organizationId, Permission.PermissionType.ORGANIZATION_ADMIN)
      } else {
        val userId = user.userId
        val hasOrgPermission =
          orgPermissionsExcludingDefaultUser
            .stream()
            .anyMatch { userPermission: UserPermission -> userPermission.user.userId == userId }
        // check to avoid creating duplicate permissions
        if (!hasOrgPermission) {
          createPermissionForUserAndOrg(userId, organization.organizationId, Permission.PermissionType.ORGANIZATION_MEMBER)
        }
      }

      // If this organization doesn't have a workspace yet, create one, and set it as the default
      // workspace for this user.
      val orgWorkspaces =
        workspacesHandler.listWorkspacesInOrganization(
          ListWorkspacesInOrganizationRequestBody().organizationId(organization.organizationId),
        )

      if (orgWorkspaces.workspaces.isEmpty()) {
        // Now calls bootstrap which includes all permissions and updates userRead.
        createDefaultWorkspaceForUser(user, Optional.of(organization))
      }
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    protected fun createDefaultWorkspaceForUser(
      user: UserRead,
      organization: Optional<Organization>,
    ) {
      // Only do this if the user doesn't already have a default workspace.

      if (user.defaultWorkspaceId != null) {
        return
      }

      // Logic stolen from workspaceHandler.createDefaultWorkspaceForUser
      val companyName = user.companyName
      val email = user.email
      val news = user.news
      // otherwise, create a default workspace for this user
      val workspaceCreate =
        WorkspaceCreateWithId()
          .name(getDefaultWorkspaceName(organization, companyName, email))
          .organizationId(organization.map { obj: Organization -> obj.organizationId }.orElse(null))
          .email(email)
          .news(news)
          .anonymousDataCollection(false)
          .securityUpdates(false)
          .displaySetupWizard(true)
          .id(uuidGenerator.get())

      val defaultWorkspace = resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(workspaceCreate)

      // set default workspace id in User table
      val userUpdateDefaultWorkspace =
        UserUpdate()
          .userId(user.userId)
          .defaultWorkspaceId(defaultWorkspace.workspaceId)
      updateUser(userUpdateDefaultWorkspace)
    }

    @get:Throws(IOException::class)
    private val ssoOrganizationIfExists: Optional<Organization>
      get() {
        val authRealm = userAuthenticationResolver.resolveRealm() ?: return Optional.empty()

        return organizationPersistence.getOrganizationBySsoConfigRealm(authRealm)
      }

    @Throws(IOException::class, JsonValidationException::class, PermissionRedundantException::class)
    private fun createPermissionForUserAndOrg(
      userId: UUID,
      orgId: UUID,
      permissionType: Permission.PermissionType,
    ) {
      permissionHandler.createPermission(
        Permission()
          .withOrganizationId(orgId)
          .withUserId(userId)
          .withPermissionType(permissionType),
      )
    }

    private fun createInstanceAdminPermissionIfInitialUser(createdUser: UserRead) {
      if (initialUserConfig.isEmpty) {
        // do nothing if initial_user bean is not present.
        return
      }

      val initialEmailFromConfig = initialUserConfig.get().email

      if (initialEmailFromConfig == null || initialEmailFromConfig.isEmpty()) {
        // do nothing if there is no initial_user email configured.
        return
      }

      // compare emails with case insensitivity because different email cases should be treated as the
      // same user.
      if (!initialEmailFromConfig.equals(createdUser.email, ignoreCase = true)) {
        return
      }

      log.info(
        "creating instance_admin permission for user ID {} because their email matches this instance's configured initial_user",
        createdUser.userId,
      )

      try {
        permissionHandler.grantInstanceAdmin(createdUser.userId)
      } catch (e: PermissionRedundantException) {
        throw ConflictException(e.message, e)
      }
    }

    private fun buildOrganizationUserReadList(
      userPermissions: List<UserPermission>,
      organizationId: UUID,
    ): OrganizationUserReadList {
      // we exclude the default user from this list because we don't want to expose it in the UI
      return OrganizationUserReadList().users(
        userPermissions
          .stream()
          .filter { userPermission: UserPermission -> userPermission.user.userId != DEFAULT_USER_ID }
          .map { userPermission: UserPermission ->
            OrganizationUserRead()
              .userId(userPermission.user.userId)
              .email(userPermission.user.email)
              .name(userPermission.user.name)
              .organizationId(organizationId)
              .permissionId(userPermission.permission.permissionId)
              .permissionType(
                userPermission.permission.permissionType
                  .value()
                  .toEnum<PermissionType>()!!,
              )
          }.collect(Collectors.toList<@Valid OrganizationUserRead?>()),
      )
    }

    private fun buildWorkspaceUserAccessInfoReadList(accessInfos: List<WorkspaceUserAccessInfo>): WorkspaceUserAccessInfoReadList {
      // we exclude the default user from this list because we don't want to expose it in the UI
      return WorkspaceUserAccessInfoReadList().usersWithAccess(
        accessInfos
          .stream()
          .filter { accessInfo: WorkspaceUserAccessInfo -> accessInfo.userId != DEFAULT_USER_ID }
          .map { accessInfo: WorkspaceUserAccessInfo -> this.buildWorkspaceUserAccessInfoRead(accessInfo) }
          .collect(Collectors.toList<@Valid WorkspaceUserAccessInfoRead?>()),
      )
    }

    private fun buildWorkspaceUserAccessInfoRead(accessInfo: WorkspaceUserAccessInfo): WorkspaceUserAccessInfoRead {
      val workspacePermissionRead =
        Optional
          .ofNullable(accessInfo.workspacePermission)
          .map { wp: Permission ->
            PermissionRead()
              .permissionId(wp.permissionId)
              .permissionType(
                wp.permissionType?.convertTo<PermissionType>(),
              ).userId(wp.userId)
              .workspaceId(wp.workspaceId)
          }.orElse(null)

      val organizationPermissionRead =
        Optional
          .ofNullable(accessInfo.organizationPermission)
          .map { op: Permission ->
            PermissionRead()
              .permissionId(op.permissionId)
              .permissionType(
                op.permissionType?.convertTo<PermissionType>(),
              ).userId(op.userId)
              .organizationId(op.organizationId)
          }.orElse(null)

      return WorkspaceUserAccessInfoRead()
        .userId(accessInfo.userId)
        .userEmail(accessInfo.userEmail)
        .userName(accessInfo.userName)
        .workspaceId(accessInfo.workspaceId)
        .workspacePermission(workspacePermissionRead)
        .organizationPermission(organizationPermissionRead)
    }

    companion object {
      private val log = KotlinLogging.logger {}
    }
  }
