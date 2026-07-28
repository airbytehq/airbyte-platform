/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
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
import io.airbyte.commons.annotation.InternalForTesting
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
import io.airbyte.config.persistence.SQLOperationNotAllowedException
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.SsoConfigService
import io.airbyte.domain.services.scim.ScimFirstLoginAttachmentResult
import io.airbyte.domain.services.scim.ScimFirstLoginService
import io.airbyte.featureflag.BypassSsoDomainValidationEnforcement
import io.airbyte.featureflag.ConfigurableSsoDefaultRole
import io.airbyte.featureflag.EmailAttribute
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.RestrictLoginsForSSODomains
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import java.io.IOException
import java.sql.Connection
import java.util.Locale
import java.util.Objects
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import io.airbyte.featureflag.Organization as FeatureFlagOrganization

/**
 * UserHandler, provides basic CRUD operation access for users. Some are migrated from Cloud
 * UserHandler.
 */
@Singleton
open class UserHandler
  @InternalForTesting
  constructor(
    private val userPersistence: UserPersistence,
    private val externalUserService: ExternalUserService,
    private val organizationService: OrganizationService,
    private val ssoConfigService: SsoConfigService,
    private val organizationEmailDomainService: OrganizationEmailDomainService,
    private val applicationService: Optional<ApplicationService>,
    private val permissionHandler: PermissionHandler,
    private val workspacesHandler: WorkspacesHandler,
    @param:Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
    private val userAuthenticationResolver: UserAuthenticationResolver,
    private val initialUserConfig: Optional<InitialUserConfig>,
    private val resourceBootstrapHandler: ResourceBootstrapHandlerInterface,
    private val featureFlagClient: FeatureFlagClient,
    private val scimFirstLoginService: ScimFirstLoginService,
    @param:Named("config") private val transactionOperations: TransactionOperations<Connection>,
  ) {
    private fun currentConfigContext(): DSLContext? =
      if (transactionOperations.hasConnection()) {
        DSL.using(transactionOperations.connection, SQLDialect.POSTGRES)
      } else {
        null
      }

    private fun persistedUser(userId: UUID?): Optional<User> =
      currentConfigContext()?.let { userPersistence.getUser(it, userId) }
        ?: userPersistence.getUser(userId)

    private fun persistedUserByAuthId(authUserId: String?): Optional<AuthenticatedUser> =
      currentConfigContext()?.let { userPersistence.getUserByAuthId(it, authUserId) }
        ?: userPersistence.getUserByAuthId(authUserId)

    private fun persistedUserByEmail(email: String?): Optional<User> =
      currentConfigContext()?.let { userPersistence.getUserByEmail(it, email) }
        ?: userPersistence.getUserByEmail(email)

    private fun persistUser(user: User) {
      currentConfigContext()?.let { userPersistence.writeUser(it, user) }
        ?: userPersistence.writeUser(user)
    }

    private fun persistAuthenticatedUser(user: AuthenticatedUser) {
      currentConfigContext()?.let { userPersistence.writeAuthenticatedUser(it, user) }
        ?: userPersistence.writeAuthenticatedUser(user)
    }

    private fun createAuthenticatedUserIfNoScimMapping(user: AuthenticatedUser): Boolean =
      currentConfigContext()?.let { userPersistence.createAuthenticatedUserIfNoScimMapping(it, user) }
        ?: userPersistence.createAuthenticatedUserIfNoScimMapping(user)

    private fun replaceAuthUserForUserId(
      userId: UUID,
      authUserId: String,
      authProvider: AuthProvider?,
    ): Boolean =
      currentConfigContext()?.let { userPersistence.replaceAuthUserForUserId(it, userId, authUserId, authProvider) }
        ?: userPersistence.replaceAuthUserForUserId(userId, authUserId, authProvider)

    private fun persistedAuthUsers(userId: UUID?): List<AuthUser> =
      currentConfigContext()?.let { userPersistence.listAuthUsersForUser(it, userId) }
        ?: userPersistence.listAuthUsersForUser(userId)

    /**
     * Get a user by internal user ID.
     *
     * @param userIdRequestBody The internal user id to be queried.
     * @return The user.
     * @throws ConfigNotFoundException if unable to get the user.
     * @throws IOException if unable to get the user.
     * @throws JsonValidationException if unable to get the user.
     */
    fun getUser(userIdRequestBody: UserIdRequestBody): UserRead = buildUserRead(userIdRequestBody.userId)

    /**
     * Retrieves the user by auth ID.
     *
     * @param userAuthIdRequestBody The [UserAuthIdRequestBody] that contains the auth ID.
     * @return The user associated with the auth ID.
     * @throws IOException if unable to retrieve the user.
     */
    fun getUserByAuthId(userAuthIdRequestBody: UserAuthIdRequestBody): UserRead {
      val user = persistedUserByAuthId(userAuthIdRequestBody.authUserId)
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
    fun getUserByEmail(userEmailRequestBody: UserEmailRequestBody): UserRead {
      val user = persistedUserByEmail(userEmailRequestBody.email)
      if (user.isPresent) {
        return buildUserRead(user.get())
      } else {
        throw ConfigNotFoundException(ConfigNotFoundType.USER, String.format("User not found by email request: %s", userEmailRequestBody))
      }
    }

    private fun buildUserRead(userId: UUID): UserRead {
      val user = persistedUser(userId)
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
        .metadata(if (user.uiMetadata != null) user.uiMetadata else emptyMap<Any, Any>())
        .news(user.news)
        .defaultWorkspaceId(user.defaultWorkspaceId)
        .agenticEnabledAt(user.agenticEnabledAt)

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
        persistUser(user)
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
        .withUiMetadata(Jsons.jsonNode(userRead.metadata ?: emptyMap<Any, Any>()))
        .withNews(userRead.news)

    /**
     * Deletes a User.
     *
     * @param userIdRequestBody The user to be deleted.
     * @throws IOException if unable to delete the user.
     * @throws ConfigNotFoundException if unable to delete the user.
     */
    fun deleteUser(userIdRequestBody: UserIdRequestBody) {
      val userRead = getUser(userIdRequestBody)
      deleteUser(userRead)
    }

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

    fun listUsersInOrganization(organizationIdRequestBody: OrganizationIdRequestBody): OrganizationUserReadList {
      val organizationId = organizationIdRequestBody.organizationId
      val userPermissions = permissionHandler.listUsersInOrganization(organizationId)
      return buildOrganizationUserReadList(userPermissions, organizationId)
    }

    fun listAccessInfoByWorkspaceId(workspaceIdRequestBody: WorkspaceIdRequestBody): WorkspaceUserAccessInfoReadList {
      val workspaceId = workspaceIdRequestBody.workspaceId
      val userAccessInfo = userPersistence.listWorkspaceUserAccessInfo(workspaceId)
      return buildWorkspaceUserAccessInfoReadList(userAccessInfo)
    }

    fun listInstanceAdminUsers(): UserWithPermissionInfoReadList {
      val userPermissions = permissionHandler.listInstanceAdminUsers()
      return UserWithPermissionInfoReadList().users(
        userPermissions
          .map { userPermission: UserPermission ->
            UserWithPermissionInfoRead()
              .userId(userPermission.user.userId)
              .email(userPermission.user.email)
              .name(userPermission.user.name)
              .permissionId(userPermission.permission.permissionId)
          },
      )
    }

    private fun isAllowedDomain(
      email: String,
      currentSSOOrg: Optional<Organization>,
    ): Boolean {
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

      return currentSSOOrg.isPresent &&
        restrictedForOrganizations
          .stream()
          .anyMatch { orgEmailDomain: OrganizationEmailDomain -> orgEmailDomain.organizationId == currentSSOOrg.get().organizationId }
    }

    private fun getExistingUserRealms(userId: UUID): List<String?> {
      val authUsers = persistedAuthUsers(userId)
      return getExistingUserRealms(authUsers)
    }

    private fun getExistingUserRealms(authUsers: List<AuthUser>): List<String?> {
      val keycloakAuthUsers =
        authUsers
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

    private fun isAnyRealmSSO(realms: List<String?>): Boolean {
      for (realm in realms) {
        if (realm != null) {
          val ssoConfig = ssoConfigService.getSsoConfigByRealmName(realm)
          if (ssoConfig != null) {
            return true
          }
        }
      }

      return false
    }

    private fun handleSSORestrictions(
      email: String,
      currentSSOOrg: Optional<Organization>,
    ) {
      val allowDomain = isAllowedDomain(email, currentSSOOrg)
      if (!allowDomain) {
        throw SSORequiredProblem()
      }
    }

    /**
     * Verifies that an SSO organization is authorized to assert the incoming user's email domain.
     *
     * A matching claimed domain is allowed immediately. Missing or mismatched claims are always logged
     * and tagged, then allowed only when the organization-level bypass resolves to `true`. Otherwise,
     * the login is rejected before user creation, migration, or relinking.
     *
     * This check does not mutate or delete identity state.
     *
     * @throws OperationNotAllowedException when an invalid domain claim is not bypassed
     */
    private fun validateSsoEmailDomainClaim(
      email: String,
      ssoOrganization: Organization,
    ) {
      val emailDomain = email.substringAfter("@").lowercase()
      val orgId = ssoOrganization.organizationId

      val claimedDomains = organizationEmailDomainService.findByOrganizationId(orgId)
      if (claimedDomains.isEmpty()) {
        log.warn {
          "SSO domain not verified: organization $orgId has no claimed domains, user email domain '$emailDomain'"
        }
        ApmTraceUtils.addTagsToTrace(
          mapOf(
            "sso.email_domain" to emailDomain,
            "sso.organization_id" to orgId.toString(),
            "sso.domain_validation" to "no_claimed_domains",
          ),
        )
      } else if (claimedDomains.none { it.emailDomain.equals(emailDomain, ignoreCase = true) }) {
        log.warn {
          "SSO domain mismatch: email domain '$emailDomain' is not claimed by organization $orgId " +
            "(claimed: ${claimedDomains.joinToString { it.emailDomain }})"
        }
        ApmTraceUtils.addTagsToTrace(
          mapOf(
            "sso.email_domain" to emailDomain,
            "sso.organization_id" to orgId.toString(),
            "sso.domain_validation" to "mismatch",
          ),
        )
      } else {
        // A matching claimed domain is authorized; bypass evaluation is unnecessary.
        return
      }

      // Only missing or mismatched domain claims reach this point.
      val bypassEnforcement =
        featureFlagClient.boolVariation(
          BypassSsoDomainValidationEnforcement,
          FeatureFlagOrganization(orgId),
        )
      if (bypassEnforcement) {
        return
      }

      throw OperationNotAllowedException(
        "SSO provider is not authorized to authenticate users with this email domain.",
      )
    }

    private fun handleNewUserLogin(userToCreate: AuthenticatedUser): LoginAction {
      val createdUser = createUserFromIncomingUser(userToCreate)
      return LoginAction.Bootstrap(createdUser, userToCreate, true)
    }

    private fun handleRelinkAuthUser(
      existingUser: User,
      incomingJwtUser: AuthenticatedUser,
      verifiedEmail: String?,
      organizationId: UUID?,
    ): LoginAction {
      log.info { "Relinking auth user {} to orphaned existing user $incomingJwtUser.authUserId, existingUser.userId..." }
      return LoginAction.Relink(
        existingUser,
        incomingJwtUser,
        persistedAuthUsers(existingUser.userId),
        verifiedEmail,
        organizationId,
      )
    }

    private fun finishRelinkAuthUser(action: LoginAction.Relink): UserGetOrCreateByAuthIdResponse {
      val response =
        transactionOperations.executeWrite {
          val ctx = currentConfigContext()
          ctx?.let { userPersistence.lockAuthUserReplacement(it, action.existingUser.userId) }

          val attachment =
            scimFirstLoginService.attachIfPreProvisioned(
              action.incomingJwtUser.email,
              action.verifiedEmail,
              action.incomingJwtUser.authUserId,
              action.incomingJwtUser.authProvider,
              action.organizationId,
            )
          val attachedUserId =
            when (attachment) {
              is ScimFirstLoginAttachmentResult.Attached -> attachment.userId
              is ScimFirstLoginAttachmentResult.AlreadyAttached -> attachment.userId
              is ScimFirstLoginAttachmentResult.ExistingIdentity -> attachment.userId
              ScimFirstLoginAttachmentResult.NoMatch -> action.existingUser.userId
              ScimFirstLoginAttachmentResult.AmbiguousIdentity,
              ScimFirstLoginAttachmentResult.EmailNotVerified,
              ScimFirstLoginAttachmentResult.Conflict,
              -> null
            }
          if (attachedUserId != action.existingUser.userId) {
            throw UserAlreadyExistsProblem(ProblemEmailData().email(action.incomingJwtUser.email))
          }

          val currentPreviousAuthUsers =
            persistedAuthUsers(action.existingUser.userId)
              .filter { it.authUserId != action.incomingJwtUser.authUserId }
          check(
            currentPreviousAuthUsers.map { it.authUserId to it.authProvider }.toSet() ==
              action.previousAuthUsers.map { it.authUserId to it.authProvider }.toSet(),
          ) {
            "Authentication identities changed while preparing orphan relink."
          }
          check(
            ctx?.let {
              userPersistence.lockAuthUsersForReplacement(
                it,
                action.existingUser.userId,
                currentPreviousAuthUsers.map { authUser -> authUser.authUserId },
                action.incomingJwtUser.authUserId,
              )
            } ?: true,
          ) {
            "Incoming authentication identity ownership changed during orphan relink."
          }
          check(
            ctx?.let {
              userPersistence.writeAuthUser(
                it,
                action.existingUser.userId,
                action.incomingJwtUser.authUserId,
                action.incomingJwtUser.authProvider,
              )
            } ?: userPersistence.writeAuthUser(
              action.existingUser.userId,
              action.incomingJwtUser.authUserId,
              action.incomingJwtUser.authProvider,
            ),
          ) {
            "Incoming authentication identity ownership changed during orphan relink."
          }
          if (applicationService.map(ApplicationService::deletesApplicationsTransactionally).orElse(false)) {
            revokeApplications(action.existingUser, currentPreviousAuthUsers)
          }

          val updatedUser =
            persistedUser(action.existingUser.userId)
              .orElseThrow {
                ConfigNotFoundException(
                  ConfigNotFoundType.USER,
                  action.existingUser.userId,
                )
              }
          UserGetOrCreateByAuthIdResponse()
            .userRead(buildUserRead(updatedUser))
            .authUserId(action.incomingJwtUser.authUserId)
            .authProvider(
              action.incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
            ).newUserCreated(false)
        }

      return finishExternalAuthUserCleanup(
        action.existingUser,
        action.incomingJwtUser,
        response,
        deleteExternalAuthUsers = false,
      )
    }

    private fun handleFirstTimeSSOLogin(
      existingUser: User,
      incomingJwtUser: AuthenticatedUser,
      verifiedEmail: String?,
      organizationId: UUID,
    ): LoginAction {
      log.info { "Migrating existing user $existingUser.userId to SSO..." }
      val previousAuthUsers = persistedAuthUsers(existingUser.userId)
      return LoginAction.Migrate(existingUser, incomingJwtUser, previousAuthUsers, verifiedEmail, organizationId)
    }

    private fun finishFirstTimeSSOLogin(action: LoginAction.Migrate): UserGetOrCreateByAuthIdResponse {
      val response =
        transactionOperations.executeWrite {
          val ctx = currentConfigContext()
          ctx?.let { userPersistence.lockAuthUserReplacement(it, action.existingUser.userId) }

          val currentAuthUsers = persistedAuthUsers(action.existingUser.userId)
          check(
            currentAuthUsers.map { it.authUserId to it.authProvider }.toSet() ==
              action.previousAuthUsers.map { it.authUserId to it.authProvider }.toSet(),
          ) {
            "Authentication identities changed while preparing SSO migration."
          }

          if (
            scimFirstLoginService.attachIfPreProvisioned(
              action.incomingJwtUser.email,
              action.verifiedEmail,
              action.incomingJwtUser.authUserId,
              action.incomingJwtUser.authProvider,
              action.organizationId,
            ) != ScimFirstLoginAttachmentResult.NoMatch
          ) {
            throw UserAlreadyExistsProblem(ProblemEmailData().email(action.incomingJwtUser.email))
          }

          check(
            ctx?.let {
              userPersistence.lockAuthUsersForReplacement(
                it,
                action.existingUser.userId,
                action.previousAuthUsers.map { authUser -> authUser.authUserId },
                action.incomingJwtUser.authUserId,
              )
            } ?: true,
          ) {
            "Incoming authentication identity ownership changed during SSO migration."
          }
          check(
            ctx?.let {
              userPersistence.writeAuthUser(
                it,
                action.existingUser.userId,
                action.incomingJwtUser.authUserId,
                action.incomingJwtUser.authProvider,
              )
            } ?: userPersistence.writeAuthUser(
              action.existingUser.userId,
              action.incomingJwtUser.authUserId,
              action.incomingJwtUser.authProvider,
            ),
          ) {
            "Incoming authentication identity ownership changed during SSO migration."
          }

          val bootstrapResponse =
            finishBootstrap(
              LoginAction.Bootstrap(
                buildUserRead(action.existingUser),
                action.incomingJwtUser,
                false,
              ),
            )
          if (applicationService.map(ApplicationService::deletesApplicationsTransactionally).orElse(false)) {
            revokeApplications(action.existingUser, action.previousAuthUsers)
          }
          bootstrapResponse
        }

      return finishExternalAuthUserCleanup(
        action.existingUser,
        action.incomingJwtUser,
        response,
        deleteExternalAuthUsers = true,
      )
    }

    private fun finishExternalAuthUserCleanup(
      existingUser: User,
      incomingJwtUser: AuthenticatedUser,
      response: UserGetOrCreateByAuthIdResponse,
      deleteExternalAuthUsers: Boolean,
    ): UserGetOrCreateByAuthIdResponse =
      transactionOperations.executeWrite {
        val ctx = currentConfigContext()
        ctx?.let { userPersistence.lockAuthUserReplacement(it, existingUser.userId) }
        val currentAuthUsers = persistedAuthUsers(existingUser.userId)
        check(currentAuthUsers.any { it.authUserId == incomingJwtUser.authUserId }) {
          "Incoming authentication identity ownership changed during replacement."
        }
        val previousAuthUsers = currentAuthUsers.filter { it.authUserId != incomingJwtUser.authUserId }
        if (previousAuthUsers.isEmpty()) {
          return@executeWrite response
        }

        check(
          ctx?.let {
            userPersistence.lockAuthUsersForReplacement(
              it,
              existingUser.userId,
              previousAuthUsers.map { authUser -> authUser.authUserId },
              incomingJwtUser.authUserId,
            )
          } ?: true,
        ) {
          "Incoming authentication identity ownership changed during replacement."
        }

        // Repeat transactional cleanup here to close the gap after phase one, and perform external
        // cleanup here because it cannot participate in the phase-one database transaction.
        revokeApplications(existingUser, previousAuthUsers)

        if (deleteExternalAuthUsers) {
          log.info { "Deleting user with email ${existingUser.email} from other auth realms..." }
          val newRealm = userAuthenticationResolver.resolveRealm()
          checkNotNull(newRealm) { "No new realm found for user ${existingUser.userId}" }
          externalUserService.deleteUserByEmailOnOtherRealms(existingUser.email, newRealm)
        }

        log.info { "Replacing existing auth users with new one (${incomingJwtUser.authUserId})..." }
        check(
          replaceAuthUserForUserId(
            existingUser.userId,
            incomingJwtUser.authUserId,
            incomingJwtUser.authProvider,
          ),
        ) {
          "Incoming authentication identity ownership changed during SSO migration."
        }

        log.info { "Done migrating user ${existingUser.userId} to SSO" }
        response
      }

    private fun revokeApplications(
      existingUser: User,
      previousAuthUsers: List<AuthUser>,
    ) {
      if (applicationService.isPresent) {
        val appService = applicationService.get()
        log.info { "Revoking existing applications for user $existingUser.userId..." }
        for (authUser in previousAuthUsers) {
          val authedUser =
            toAuthenticatedUser(existingUser, authUser.authUserId, authUser.authProvider)
          val existingApplications = appService.listApplicationsByUser(authedUser)
          for (application in existingApplications) {
            appService.deleteApplication(authedUser, application.id)
            log.info { "Revoked application ${application.id} for user ${existingUser.userId} (auth user ${authUser.authUserId})..." }
          }
        }
      }
    }

    private fun finishBootstrap(action: LoginAction.Bootstrap): UserGetOrCreateByAuthIdResponse {
      val userRead = action.userRead
      handleUserPermissionsAndWorkspace(userRead)

      // refresh the user from the database in case anything changed during permission/workspace
      // modification
      val updatedUser =
        persistedUser(userRead.userId)
          .orElseThrow {
            ConfigNotFoundException(
              ConfigNotFoundType.USER,
              userRead.userId,
            )
          }

      return UserGetOrCreateByAuthIdResponse()
        .userRead(buildUserRead(updatedUser))
        .authUserId(action.incomingJwtUser.authUserId)
        .authProvider(
          action.incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
        ).newUserCreated(action.newUserCreated)
    }

    fun getOrCreateUserByAuthId(userAuthIdRequestBody: UserAuthIdRequestBody): UserGetOrCreateByAuthIdResponse {
      val action =
        transactionOperations.executeWrite {
          getOrCreateUserByAuthIdInTransaction(userAuthIdRequestBody)
        }
      return when (action) {
        is LoginAction.Complete -> action.response
        is LoginAction.Bootstrap -> finishBootstrap(action)
        is LoginAction.Migrate -> finishFirstTimeSSOLogin(action)
        is LoginAction.Relink -> finishRelinkAuthUser(action)
        is LoginAction.Cleanup ->
          finishExternalAuthUserCleanup(
            action.existingUser,
            action.incomingJwtUser,
            action.response,
            action.deleteExternalAuthUsers,
          )
      }
    }

    private fun getOrCreateUserByAuthIdInTransaction(userAuthIdRequestBody: UserAuthIdRequestBody): LoginAction {
      val incomingJwtUser = resolveIncomingJwtUser(userAuthIdRequestBody)
      var existingAuthUser = persistedUserByAuthId(incomingJwtUser.authUserId)

      // SEC-14: Resolve the SSO organization for this request once, reused below.
      val ssoOrg = ssoOrganizationIfExists
      val verifiedEmail = userAuthenticationResolver.resolveVerifiedEmail()

      // Validate every email that can drive first-login attachment before it performs any identity write.
      if (ssoOrg.isPresent) {
        listOfNotNull(incomingJwtUser.email, verifiedEmail)
          .distinctBy { it.lowercase(Locale.ROOT) }
          .forEach { attachmentEmail ->
            validateSsoEmailDomainClaim(attachmentEmail, ssoOrg.get())
            handleSSORestrictions(attachmentEmail, ssoOrg)
          }
      }

      when (
        val attachment =
          scimFirstLoginService.attachIfPreProvisioned(
            incomingJwtUser.email,
            verifiedEmail,
            incomingJwtUser.authUserId,
            incomingJwtUser.authProvider,
            ssoOrg.map(Organization::getOrganizationId).orElse(null),
          )
      ) {
        is ScimFirstLoginAttachmentResult.Attached -> {
          existingAuthUser =
            persistedUserByAuthId(incomingJwtUser.authUserId)
          if (existingAuthUser.isEmpty || existingAuthUser.get().userId != attachment.userId) {
            throw UserAlreadyExistsProblem(ProblemEmailData().email(incomingJwtUser.email))
          }
        }
        is ScimFirstLoginAttachmentResult.AlreadyAttached -> {
          existingAuthUser =
            persistedUserByAuthId(incomingJwtUser.authUserId)
          if (existingAuthUser.isEmpty || existingAuthUser.get().userId != attachment.userId) {
            throw UserAlreadyExistsProblem(ProblemEmailData().email(incomingJwtUser.email))
          }
        }
        is ScimFirstLoginAttachmentResult.ExistingIdentity -> {
          existingAuthUser = persistedUserByAuthId(incomingJwtUser.authUserId)
          if (existingAuthUser.isEmpty || existingAuthUser.get().userId != attachment.userId) {
            throw UserAlreadyExistsProblem(ProblemEmailData().email(incomingJwtUser.email))
          }
        }
        ScimFirstLoginAttachmentResult.NoMatch -> Unit
        ScimFirstLoginAttachmentResult.AmbiguousIdentity,
        ScimFirstLoginAttachmentResult.EmailNotVerified,
        ScimFirstLoginAttachmentResult.Conflict,
        -> throw UserAlreadyExistsProblem(ProblemEmailData().email(incomingJwtUser.email))
      }

      // Restriction failures never delete a raw external subject: the subject may be shared by
      // another provider or user, and absence cannot be durably inferred without the lock above.
      if (ssoOrg.isEmpty) {
        handleSSORestrictions(incomingJwtUser.email, ssoOrg)
      }

      // (2) Authenticate existing auth_user
      if (existingAuthUser.isPresent) {
        val existingUser = existingAuthUser.get()

        // Support upgrading non-agentic users to agentic (one-way operation)
        // If user is not agentic yet AND request wants to make them agentic, upgrade them
        if (existingUser.agenticEnabledAt == null &&
          userAuthIdRequestBody.isAgenticUser == true &&
          incomingJwtUser.agenticEnabledAt != null
        ) {
          // Upgrade user: set agenticEnabledAt timestamp
          val upgradedUser = existingUser.withAgenticEnabledAt(incomingJwtUser.agenticEnabledAt)
          persistAuthenticatedUser(upgradedUser)
          log.info { "Upgraded user ${existingUser.userId} to agentic user" }

          return LoginAction.Complete(
            UserGetOrCreateByAuthIdResponse()
              .userRead(buildUserRead(toUser(upgradedUser)))
              .authUserId(userAuthIdRequestBody.authUserId)
              .authProvider(
                incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
              ).newUserCreated(false),
          )
        }

        // Otherwise, return existing user as-is (agenticEnabledAt is immutable once set)
        val response =
          UserGetOrCreateByAuthIdResponse()
            .userRead(buildUserRead(toUser(existingUser)))
            .authUserId(userAuthIdRequestBody.authUserId)
            .authProvider(
              incomingJwtUser.authProvider?.convertTo<io.airbyte.api.model.generated.AuthProvider>(),
            ).newUserCreated(false)
        val previousAuthUsers =
          persistedAuthUsers(existingUser.userId)
            .filter { it.authUserId != incomingJwtUser.authUserId }
        if (previousAuthUsers.isNotEmpty()) {
          if (ssoOrg.isPresent) {
            return LoginAction.Cleanup(toUser(existingUser), incomingJwtUser, response, true)
          }
          if (getExistingUserRealms(previousAuthUsers).isEmpty()) {
            return LoginAction.Cleanup(toUser(existingUser), incomingJwtUser, response, false)
          }
        }
        return LoginAction.Complete(response)
      }

      // (3) Handle non-existing auth_user

      var existingUserWithEmail = persistedUserByEmail(incomingJwtUser.email)
      if (existingUserWithEmail.isPresent && scimFirstLoginService.isScimManagedUser(existingUserWithEmail.get().userId)) {
        throw UserAlreadyExistsProblem(ProblemEmailData().email(incomingJwtUser.email))
      }
      if (existingUserWithEmail.isPresent && existingUserWithEmail.get().userId === DEFAULT_USER_ID) {
        // (Enterprise) If the email is already taken by the default user, we can safely clear it so the
        // real user can be created
        persistUser(existingUserWithEmail.get().withEmail(""))
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
        return handleRelinkAuthUser(
          existingUser,
          incomingJwtUser,
          verifiedEmail,
          ssoOrg.map(Organization::getOrganizationId).orElse(null),
        )
      }

      val isCurrentSignInSSO = ssoOrg.isPresent
      val isExistingUserSSOAuthed = isAnyRealmSSO(existingUserRealms)

      // (3b1) This is the first SSO sign in for the user, migrate it for SSO
      if (isCurrentSignInSSO && !isExistingUserSSOAuthed) {
        return handleFirstTimeSSOLogin(
          existingUser,
          incomingJwtUser,
          verifiedEmail,
          ssoOrg.get().organizationId,
        )
      }

      // (3b2) This isn't a first-time SSO sign in and/or the user already exists
      throw UserAlreadyExistsProblem(ProblemEmailData().email(existingUser.email))
    }

    private sealed interface LoginAction {
      data class Complete(
        val response: UserGetOrCreateByAuthIdResponse,
      ) : LoginAction

      data class Bootstrap(
        val userRead: UserRead,
        val incomingJwtUser: AuthenticatedUser,
        val newUserCreated: Boolean,
      ) : LoginAction

      data class Migrate(
        val existingUser: User,
        val incomingJwtUser: AuthenticatedUser,
        val previousAuthUsers: List<AuthUser>,
        val verifiedEmail: String?,
        val organizationId: UUID,
      ) : LoginAction

      data class Relink(
        val existingUser: User,
        val incomingJwtUser: AuthenticatedUser,
        val previousAuthUsers: List<AuthUser>,
        val verifiedEmail: String?,
        val organizationId: UUID?,
      ) : LoginAction

      data class Cleanup(
        val existingUser: User,
        val incomingJwtUser: AuthenticatedUser,
        val response: UserGetOrCreateByAuthIdResponse,
        val deleteExternalAuthUsers: Boolean,
      ) : LoginAction
    }

    private fun resolveIncomingJwtUser(userAuthIdRequestBody: UserAuthIdRequestBody): AuthenticatedUser {
      val authUserId = userAuthIdRequestBody.authUserId
      // Create fresh AuthenticatedUser from JWT claims (agenticEnabledAt is always null here)
      val user = userAuthenticationResolver.resolveUser(authUserId)

      // If isAgenticUser is true, set the timestamp to now on this fresh object
      // This applies to both new user creation and upgrading existing non-agentic users
      // Note: For existing users, the database value is checked separately (line 467)
      //       and this timestamp is only used if the DB value is null (upgrading)
      if (userAuthIdRequestBody.isAgenticUser == true) {
        return user.withAgenticEnabledAt(java.time.OffsetDateTime.now())
      }

      return user
    }

    private fun createUserFromIncomingUser(incomingUser: AuthenticatedUser): UserRead {
      val userId = uuidGenerator.get()
      val user = incomingUser.withUserId(userId)

      log.debug { "Creating User: $user" }

      try {
        if (!createAuthenticatedUserIfNoScimMapping(user)) {
          throw UserAlreadyExistsProblem(ProblemEmailData().email(user.email))
        }
      } catch (e: DataAccessException) {
        if (e.cause is SQLOperationNotAllowedException) {
          throw OperationNotAllowedException((e.cause as SQLOperationNotAllowedException).message)
        } else {
          throw IOException(e)
        }
      }
      return buildUserRead(userId)
    }

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
          createPermissionForUserAndOrg(userId, organization.organizationId, getSsoDefaultRole(organization.organizationId))
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

      val defaultWorkspace =
        currentConfigContext()?.let { ctx ->
          resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(ctx, workspaceCreate)
        } ?: resourceBootstrapHandler.bootStrapWorkspaceForCurrentUser(workspaceCreate)

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

        return organizationService.getOrganizationBySsoConfigRealm(authRealm)
      }

    private fun createPermissionForUserAndOrg(
      userId: UUID,
      orgId: UUID,
      permissionType: Permission.PermissionType,
    ) {
      try {
        permissionHandler.createPermission(
          Permission()
            .withOrganizationId(orgId)
            .withUserId(userId)
            .withPermissionType(permissionType),
        )
      } catch (e: io.micronaut.data.exceptions.DataAccessException) {
        // Bootstrap runs after the login identity transaction commits. A concurrent SCIM POST can
        // therefore grant baseline access after the permission pre-read but before this insert.
        // Treat the resulting unique-key race as success only after verifying that access now exists.
        if (permissionHandler.listPermissionsForUser(userId).none { it.organizationId == orgId }) {
          throw e
        }
      }
    }

    private fun getSsoDefaultRole(organizationId: UUID): Permission.PermissionType {
      // ConfigurableSsoDefaultRole (temporary, default OFF) dark-launches per-config SSO default roles.
      // While the flag is off for the org, ignore the configured role and fall back to ORGANIZATION_MEMBER,
      // matching pre-feature behavior so the deploy can be separated from the release.
      if (!featureFlagClient.boolVariation(ConfigurableSsoDefaultRole, io.airbyte.featureflag.Organization(organizationId))) {
        return Permission.PermissionType.ORGANIZATION_MEMBER
      }
      return ssoConfigService.getSsoConfig(organizationId)?.defaultRole ?: Permission.PermissionType.ORGANIZATION_MEMBER
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

      log.info {
        "creating instance_admin permission for user ID ${createdUser.userId} because their email matches this instance's configured initial_user"
      }

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
          },
      )
    }

    private fun buildWorkspaceUserAccessInfoReadList(accessInfos: List<WorkspaceUserAccessInfo>): WorkspaceUserAccessInfoReadList {
      // we exclude the default user from this list because we don't want to expose it in the UI
      return WorkspaceUserAccessInfoReadList().usersWithAccess(
        accessInfos
          .filter { accessInfo: WorkspaceUserAccessInfo -> accessInfo.userId != DEFAULT_USER_ID }
          .map { accessInfo: WorkspaceUserAccessInfo -> this.buildWorkspaceUserAccessInfoRead(accessInfo) },
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
