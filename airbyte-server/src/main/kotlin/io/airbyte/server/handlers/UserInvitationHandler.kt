/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Sets
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.model.generated.InviteCodeRequestBody
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.ScopeType
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody
import io.airbyte.api.model.generated.UserInvitationCreateResponse
import io.airbyte.api.model.generated.UserInvitationListRequestBody
import io.airbyte.api.model.generated.UserInvitationRead
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfigSchema
import io.airbyte.config.InvitationStatus
import io.airbyte.config.Permission
import io.airbyte.config.User
import io.airbyte.config.UserInvitation
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.services.InvitationDuplicateException
import io.airbyte.data.services.InvitationPermissionOverlapException
import io.airbyte.data.services.InvitationStatusUnexpectedException
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.notification.CustomerIoEmailConfig
import io.airbyte.notification.CustomerIoEmailNotificationSender
import io.airbyte.server.handlers.apidomainmapping.UserInvitationMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
class UserInvitationHandler(
  val service: UserInvitationService,
  val mapper: UserInvitationMapper,
  val customerIoEmailNotificationSender: CustomerIoEmailNotificationSender,
  val webUrlHelper: WebUrlHelper,
  val workspaceService: WorkspaceService,
  val organizationService: OrganizationService,
  val userPersistence: UserPersistence,
  val permissionHandler: PermissionHandler,
  val trackingClient: TrackingClient,
) {
  fun getByInviteCode(
    inviteCode: String,
    currentUser: AuthenticatedUser,
  ): UserInvitationRead {
    val invitation = service.getUserInvitationByInviteCode(inviteCode)

    if (invitation.invitedEmail != currentUser.email) {
      throw OperationNotAllowedException("Invited email does not match current user email.")
    }

    return mapper.toApi(invitation)
  }

  fun getPendingInvitations(invitationListRequestBody: UserInvitationListRequestBody): List<UserInvitationRead> {
    val scopeType = mapper.toDomain(invitationListRequestBody.scopeType)
    val invitations = service.getPendingInvitations(scopeType, invitationListRequestBody.scopeId)

    return invitations.map { domain -> mapper.toApi(domain) }
  }

  /**
   * Creates either a new [UserInvitation], or a new [Permission] for the invited email
   * address, depending on whether the email address is already associated with a User within the
   * relevant organization.
   */
  fun createInvitationOrPermission(
    req: UserInvitationCreateRequestBody,
    currentUser: AuthenticatedUser,
  ): UserInvitationCreateResponse {
    val response: UserInvitationCreateResponse
    val wasDirectAdd = attemptDirectAddEmailToOrg(req, currentUser)

    if (wasDirectAdd) {
      return UserInvitationCreateResponse().directlyAdded(true)
    } else {
      try {
        val invitation = createUserInvitationForNewOrgEmail(req, currentUser)
        response = UserInvitationCreateResponse().directlyAdded(false).inviteCode(invitation.inviteCode)
        trackUserInvited(req, currentUser)
        return response
      } catch (e: InvitationDuplicateException) {
        throw ConflictException(e.message)
      } catch (e: InvitationPermissionOverlapException) {
        throw ConflictException(e.message)
      }
    }
  }

  private fun trackUserInvited(
    requestBody: UserInvitationCreateRequestBody,
    currentUser: AuthenticatedUser,
  ) {
    try {
      when (requestBody.scopeType) {
        ScopeType.ORGANIZATION -> {
          // Implement once we support org-level invitations
        }

        ScopeType.WORKSPACE ->
          trackUserInvitedToWorkspace(
            requestBody.scopeId,
            requestBody.invitedEmail,
            currentUser.email,
            currentUser.userId,
            getInvitedResourceName(requestBody),
            requestBody.permissionType,
          )

        else -> throw IllegalArgumentException("Unexpected scope type: " + requestBody.scopeType)
      }
    } catch (e: Exception) {
      // log the error, but don't throw an exception to prevent a user-facing error
      log.error(e) { "${"Failed to track user invited"}" }
    }
  }

  private fun trackUserInvitedToWorkspace(
    workspaceId: UUID,
    email: String,
    inviterUserEmail: String,
    inviterUserId: UUID,
    workspaceName: String,
    permissionType: PermissionType,
  ) {
    trackingClient.track(
      workspaceId,
      io.airbyte.config.ScopeType.WORKSPACE,
      USER_INVITED,
      ImmutableMap
        .builder<String, Any?>()
        .put("email", email)
        .put("inviter_user_email", inviterUserEmail)
        .put("inviter_user_id", inviterUserId)
        .put("role", permissionType)
        .put("workspace_id", workspaceId)
        .put("workspace_name", workspaceName)
        .put(
          "invited_from",
          "unspecified",
        ) // Note: currently we don't have a way to specify this, carryover from old cloud-only invite system
        .build(),
    )
  }

  /**
   * Attempts to add the invited email address to the requested workspace/organization directly.
   * Searches for existing users with the invited email address, who are also currently members of the
   * requested organization. If any such users are found, a new permission is created for each user
   * via the [PermissionHandler], and an email notification is sent to the email.
   */
  private fun attemptDirectAddEmailToOrg(
    req: UserInvitationCreateRequestBody,
    currentUser: AuthenticatedUser,
  ): Boolean {
    val orgId = getOrgIdFromCreateRequest(req)
    if (orgId == null) {
      log.info { "${"No orgId found for scopeId {}, will not direct add."} ${req.scopeId}" }
      return false
    }

    val orgUserIdsWithEmail = getOrgUserIdsWithEmail(orgId, req.invitedEmail)

    if (orgUserIdsWithEmail.isEmpty()) {
      // indicates that there will be no 'direct add', so the invitation creation path should be
      // taken instead.
      log.info { "No existing org users with email, will not direct add." }
      return false
    }

    // TODO - simplify once we enforce email uniqueness in User table.
    for (userId in orgUserIdsWithEmail) {
      directAddPermissionForExistingUser(req, userId)
    }

    // TODO - update customer.io template to support organization-level invitations, right now the
    // template contains hardcoded language about workspaces.
    customerIoEmailNotificationSender.sendNotificationOnInvitingExistingUser(
      CustomerIoEmailConfig(req.invitedEmail),
      currentUser.name,
      getInvitedResourceName(req),
    )

    // indicates that the email was processed via the 'direct add' path, so no invitation will be
    // created.
    return true
  }

  private fun getOrgUserIdsWithEmail(
    orgId: UUID,
    email: String,
  ): Set<UUID> {
    log.info { "orgId: $orgId" }

    val userWithEmail = userPersistence.getUserByEmail(email)
    val userIdsWithEmail =
      userWithEmail
        .map { userInfo: User ->
          java.util.Set.of(
            userInfo.userId,
          )
        }.orElseGet { setOf() }

    log.info { "userIdsWithEmail: $userIdsWithEmail" }

    val existingOrgUserIds =
      permissionHandler
        .listUsersInOrganization(orgId)
        .map { it.user.userId }
        .toSet()

    log.info { "existingOrgUserIds: $existingOrgUserIds" }

    val intersection: Set<UUID> = Sets.intersection(userIdsWithEmail, existingOrgUserIds)

    log.info { "intersection: $intersection" }

    return intersection
  }

  private fun getOrgIdFromCreateRequest(req: UserInvitationCreateRequestBody): UUID? =
    when (req.scopeType!!) {
      ScopeType.ORGANIZATION -> req.scopeId
      ScopeType.WORKSPACE -> workspaceService.getOrganizationIdFromWorkspaceId(req.scopeId).orElse(null)
    }

  private fun directAddPermissionForExistingUser(
    req: UserInvitationCreateRequestBody,
    existingUserId: UUID,
  ) {
    val permissionCreate =
      Permission()
        .withUserId(existingUserId)
        .withPermissionType(Permission.PermissionType.valueOf(req.permissionType.name))

    when (req.scopeType) {
      ScopeType.ORGANIZATION -> permissionCreate.organizationId = req.scopeId
      ScopeType.WORKSPACE -> permissionCreate.workspaceId = req.scopeId
      else -> throw IllegalArgumentException("Unexpected scope type: " + req.scopeType)
    }

    permissionHandler.createPermission(permissionCreate)
  }

  /**
   * Creates a new [UserInvitation] for the invited email address, and sends an email that
   * contains a link that can be used to accept the invitation by its unique inviteCode. Note that
   * this method only handles the path where the invited email address is not already associated with
   * a User inside the relevant organization.
   */
  private fun createUserInvitationForNewOrgEmail(
    req: UserInvitationCreateRequestBody,
    currentUser: AuthenticatedUser,
  ): UserInvitation {
    val model = mapper.toDomain(req)

    model.inviterUserId = currentUser.userId

    // For now, inviteCodes are simply UUIDs that are converted to strings, to virtually guarantee
    // uniqueness. The column itself is a string, so if UUIDs prove to be cumbersome or too long,
    // we can always switch to a different method of generating shorter, unique inviteCodes.
    model.inviteCode = UUID.randomUUID().toString()

    // New UserInvitations are always created with a status of PENDING.
    model.status = InvitationStatus.PENDING

    // For now, new UserInvitations are created with a fixed expiration timestamp.
    model.expiresAt = OffsetDateTime.now().plusDays(INVITE_EXPIRATION_DAYS.toLong()).toEpochSecond()

    val saved = service.createUserInvitation(model)

    log.info { "${"created invitation {}"} $saved" }

    // send invite email to the user
    // the email content includes the name of the inviter and the invite link
    // the invite link should look like cloud.airbyte.com/accept-invite?inviteCode=randomCodeHere
    val inviteLink = webUrlHelper.baseUrl + ACCEPT_INVITE_PATH + saved.inviteCode
    customerIoEmailNotificationSender.sendInviteToUser(CustomerIoEmailConfig(req.invitedEmail), currentUser.name, inviteLink)

    return saved
  }

  /**
   * Returns either the Workspace name or Organization name, depending on the scope of the invite.
   */
  private fun getInvitedResourceName(req: UserInvitationCreateRequestBody): String =
    when (req.scopeType) {
      ScopeType.ORGANIZATION -> {
        organizationService
          .getOrganization(req.scopeId)
          .orElseThrow {
            ConfigNotFoundException(
              ConfigSchema.ORGANIZATION,
              req.scopeId,
            )
          }.name
      }

      ScopeType.WORKSPACE -> {
        workspaceService.getStandardWorkspaceNoSecrets(req.scopeId, false).name
      }

      else -> throw IllegalArgumentException("Unexpected scope type: " + req.scopeType)
    }

  fun accept(
    req: InviteCodeRequestBody,
    currentUser: AuthenticatedUser,
  ): UserInvitationRead {
    val invitation = service.getUserInvitationByInviteCode(req.inviteCode)

    if (!invitation.invitedEmail.equals(currentUser.email, ignoreCase = true)) {
      throw OperationNotAllowedException("Invited email does not match current user email.")
    }

    try {
      val accepted = service.acceptUserInvitation(req.inviteCode, currentUser.userId)
      return mapper.toApi(accepted)
    } catch (e: InvitationStatusUnexpectedException) {
      throw ConflictException(e.message)
    }
  }

  fun cancel(req: InviteCodeRequestBody): UserInvitationRead {
    try {
      val canceled = service.cancelUserInvitation(req.inviteCode)
      return mapper.toApi(canceled)
    } catch (e: InvitationStatusUnexpectedException) {
      throw ConflictException(e.message)
    }
  } // TODO implement `decline`

  companion object {
    const val ACCEPT_INVITE_PATH: String = "/accept-invite?inviteCode="
    const val INVITE_EXPIRATION_DAYS: Int = 7
    const val USER_INVITED: String = "User Invited"
  }
}
