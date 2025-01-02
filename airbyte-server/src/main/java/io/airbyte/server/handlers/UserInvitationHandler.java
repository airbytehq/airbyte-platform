/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.generated.InviteCodeRequestBody;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationCreateResponse;
import io.airbyte.api.model.generated.UserInvitationListRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.commons.server.errors.ConflictException;
import io.airbyte.commons.server.errors.OperationNotAllowedException;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.InvitationStatus;
import io.airbyte.config.Permission;
import io.airbyte.config.ScopeType;
import io.airbyte.config.User;
import io.airbyte.config.UserInvitation;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.InvitationDuplicateException;
import io.airbyte.data.services.InvitationPermissionOverlapException;
import io.airbyte.data.services.InvitationStatusUnexpectedException;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.UserInvitationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.notification.CustomerIoEmailConfig;
import io.airbyte.notification.CustomerIoEmailNotificationSender;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.server.handlers.api_domain_mapping.UserInvitationMapper;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@SuppressWarnings({"PMD.PreserveStackTrace", "PMD.ExceptionAsFlowControl"})
public class UserInvitationHandler {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String ACCEPT_INVITE_PATH = "/accept-invite?inviteCode=";
  static final int INVITE_EXPIRATION_DAYS = 7;
  static final String USER_INVITED = "User Invited";

  final UserInvitationService service;
  final UserInvitationMapper mapper;
  final WebUrlHelper webUrlHelper;
  final CustomerIoEmailNotificationSender customerIoEmailNotificationSender;
  final WorkspaceService workspaceService;
  final OrganizationService organizationService;
  final UserPersistence userPersistence;
  final PermissionPersistence permissionPersistence;
  final PermissionHandler permissionHandler;
  final TrackingClient trackingClient;

  public UserInvitationHandler(final UserInvitationService service,
                               final UserInvitationMapper mapper,
                               final CustomerIoEmailNotificationSender customerIoEmailNotificationSender,
                               final WebUrlHelper webUrlHelper,
                               final WorkspaceService workspaceService,
                               final OrganizationService organizationService,
                               final UserPersistence userPersistence,
                               final PermissionPersistence permissionPersistence,
                               final PermissionHandler permissionHandler,
                               final TrackingClient trackingClient) {
    this.service = service;
    this.mapper = mapper;
    this.webUrlHelper = webUrlHelper;
    this.customerIoEmailNotificationSender = customerIoEmailNotificationSender;
    this.workspaceService = workspaceService;
    this.organizationService = organizationService;
    this.userPersistence = userPersistence;
    this.permissionPersistence = permissionPersistence;
    this.permissionHandler = permissionHandler;
    this.trackingClient = trackingClient;
  }

  public UserInvitationRead getByInviteCode(final String inviteCode, final AuthenticatedUser currentUser) {
    final UserInvitation invitation = service.getUserInvitationByInviteCode(inviteCode);

    if (!invitation.getInvitedEmail().equals(currentUser.getEmail())) {
      throw new OperationNotAllowedException("Invited email does not match current user email.");
    }

    return mapper.toApi(invitation);
  }

  public List<UserInvitationRead> getPendingInvitations(final UserInvitationListRequestBody invitationListRequestBody) {
    final ScopeType scopeType = mapper.toDomain(invitationListRequestBody.getScopeType());
    final List<UserInvitation> invitations = service.getPendingInvitations(scopeType, invitationListRequestBody.getScopeId());

    return invitations.stream()
        .map(mapper::toApi)
        .collect(Collectors.toList());
  }

  /**
   * Creates either a new {@link UserInvitation}, or a new {@link Permission} for the invited email
   * address, depending on whether the email address is already associated with a User within the
   * relevant organization.
   */
  public UserInvitationCreateResponse createInvitationOrPermission(final UserInvitationCreateRequestBody req, final AuthenticatedUser currentUser)
      throws IOException, JsonValidationException, ConfigNotFoundException {

    final UserInvitationCreateResponse response;
    final boolean wasDirectAdd = attemptDirectAddEmailToOrg(req, currentUser);

    if (wasDirectAdd) {
      return new UserInvitationCreateResponse().directlyAdded(true);
    } else {
      try {
        final UserInvitation invitation = createUserInvitationForNewOrgEmail(req, currentUser);
        response = new UserInvitationCreateResponse().directlyAdded(false).inviteCode(invitation.getInviteCode());
        trackUserInvited(req, currentUser);
        return response;
      } catch (final InvitationDuplicateException | InvitationPermissionOverlapException e) {
        throw new ConflictException(e.getMessage());
      }
    }
  }

  private void trackUserInvited(final UserInvitationCreateRequestBody requestBody, final AuthenticatedUser currentUser) {
    try {
      switch (requestBody.getScopeType()) {
        case ORGANIZATION -> {
          // Implement once we support org-level invitations
        }
        case WORKSPACE -> trackUserInvitedToWorkspace(requestBody.getScopeId(),
            requestBody.getInvitedEmail(),
            currentUser.getEmail(),
            currentUser.getUserId(),
            getInvitedResourceName(requestBody),
            requestBody.getPermissionType());
        default -> throw new IllegalArgumentException("Unexpected scope type: " + requestBody.getScopeType());
      }
    } catch (final Exception e) {
      // log the error, but don't throw an exception to prevent a user-facing error
      log.error("Failed to track user invited", e);
    }
  }

  private void trackUserInvitedToWorkspace(final UUID workspaceId,
                                           final String email,
                                           final String inviterUserEmail,
                                           final UUID inviterUserId,
                                           final String workspaceName,
                                           final PermissionType permissionType) {
    trackingClient.track(workspaceId,
        ScopeType.WORKSPACE,
        USER_INVITED,
        ImmutableMap.<String, Object>builder()
            .put("email", email)
            .put("inviter_user_email", inviterUserEmail)
            .put("inviter_user_id", inviterUserId)
            .put("role", permissionType)
            .put("workspace_id", workspaceId)
            .put("workspace_name", workspaceName)
            .put("invited_from", "unspecified") // Note: currently we don't have a way to specify this, carryover from old cloud-only invite system
            .build());
  }

  /**
   * Attempts to add the invited email address to the requested workspace/organization directly.
   * Searches for existing users with the invited email address, who are also currently members of the
   * requested organization. If any such users are found, a new permission is created for each user
   * via the {@link PermissionHandler}, and an email notification is sent to the email.
   */
  private boolean attemptDirectAddEmailToOrg(final UserInvitationCreateRequestBody req, final AuthenticatedUser currentUser)
      throws IOException, JsonValidationException, ConfigNotFoundException {

    final Optional<UUID> orgId = getOrgIdFromCreateRequest(req);
    if (orgId.isEmpty()) {
      log.info("No orgId found for scopeId {}, will not direct add.", req.getScopeId());
      return false;
    }

    final Set<UUID> orgUserIdsWithEmail = getOrgUserIdsWithEmail(orgId.get(), req.getInvitedEmail());

    if (orgUserIdsWithEmail.isEmpty()) {
      // indicates that there will be no 'direct add', so the invitation creation path should be
      // taken instead.
      log.info("No existing org users with email, will not direct add.");
      return false;
    }

    // TODO - simplify once we enforce email uniqueness in User table.
    for (final UUID userId : orgUserIdsWithEmail) {
      directAddPermissionForExistingUser(req, userId);
    }

    // TODO - update customer.io template to support organization-level invitations, right now the
    // template contains hardcoded language about workspaces.
    customerIoEmailNotificationSender.sendNotificationOnInvitingExistingUser(
        new CustomerIoEmailConfig(req.getInvitedEmail()), currentUser.getName(), getInvitedResourceName(req));

    // indicates that the email was processed via the 'direct add' path, so no invitation will be
    // created.
    return true;
  }

  private Set<UUID> getOrgUserIdsWithEmail(final UUID orgId, final String email) throws IOException {
    log.info("orgId: " + orgId);

    final Optional<User> userWithEmail = userPersistence.getUserByEmail(email);
    final Set<UUID> userIdsWithEmail = userWithEmail.map(userInfo -> Set.of(userInfo.getUserId())).orElseGet(Set::of);

    log.info("userIdsWithEmail: " + userIdsWithEmail);

    final Set<UUID> existingOrgUserIds = permissionPersistence.listUsersInOrganization(orgId).stream()
        .map(userPermission -> userPermission.getUser().getUserId())
        .collect(Collectors.toSet());

    log.info("existingOrgUserIds: " + existingOrgUserIds);

    final Set<UUID> intersection = Sets.intersection(userIdsWithEmail, existingOrgUserIds);

    log.info("intersection: " + intersection);

    return intersection;
  }

  private Optional<UUID> getOrgIdFromCreateRequest(final UserInvitationCreateRequestBody req) throws IOException {
    return switch (req.getScopeType()) {
      case ORGANIZATION -> Optional.of(req.getScopeId());
      case WORKSPACE -> workspaceService.getOrganizationIdFromWorkspaceId(req.getScopeId());
    };
  }

  private void directAddPermissionForExistingUser(final UserInvitationCreateRequestBody req, final UUID existingUserId)
      throws JsonValidationException, IOException {
    final var permissionCreate = new PermissionCreate()
        .userId(existingUserId)
        .permissionType(req.getPermissionType());

    switch (req.getScopeType()) {
      case ORGANIZATION -> permissionCreate.setOrganizationId(req.getScopeId());
      case WORKSPACE -> permissionCreate.setWorkspaceId(req.getScopeId());
      default -> throw new IllegalArgumentException("Unexpected scope type: " + req.getScopeType());
    }

    permissionHandler.createPermission(permissionCreate);
  }

  /**
   * Creates a new {@link UserInvitation} for the invited email address, and sends an email that
   * contains a link that can be used to accept the invitation by its unique inviteCode. Note that
   * this method only handles the path where the invited email address is not already associated with
   * a User inside the relevant organization.
   */
  private UserInvitation createUserInvitationForNewOrgEmail(final UserInvitationCreateRequestBody req, final AuthenticatedUser currentUser)
      throws InvitationDuplicateException, InvitationPermissionOverlapException {
    final UserInvitation model = mapper.toDomain(req);

    model.setInviterUserId(currentUser.getUserId());

    // For now, inviteCodes are simply UUIDs that are converted to strings, to virtually guarantee
    // uniqueness. The column itself is a string, so if UUIDs prove to be cumbersome or too long,
    // we can always switch to a different method of generating shorter, unique inviteCodes.
    model.setInviteCode(UUID.randomUUID().toString());

    // New UserInvitations are always created with a status of PENDING.
    model.setStatus(InvitationStatus.PENDING);

    // For now, new UserInvitations are created with a fixed expiration timestamp.
    model.setExpiresAt(OffsetDateTime.now().plusDays(INVITE_EXPIRATION_DAYS).toEpochSecond());

    final UserInvitation saved = service.createUserInvitation(model);

    log.info("created invitation {}", saved);

    // send invite email to the user
    // the email content includes the name of the inviter and the invite link
    // the invite link should look like cloud.airbyte.com/accept-invite?inviteCode=randomCodeHere
    final String inviteLink = webUrlHelper.getBaseUrl() + ACCEPT_INVITE_PATH + saved.getInviteCode();
    customerIoEmailNotificationSender.sendInviteToUser(new CustomerIoEmailConfig(req.getInvitedEmail()), currentUser.getName(), inviteLink);

    return saved;
  }

  /**
   * Returns either the Workspace name or Organization name, depending on the scope of the invite.
   */
  private String getInvitedResourceName(final UserInvitationCreateRequestBody req)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    switch (req.getScopeType()) {
      case ORGANIZATION -> {
        return organizationService.getOrganization(req.getScopeId())
            .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.ORGANIZATION, req.getScopeId()))
            .getName();
      }
      case WORKSPACE -> {
        return workspaceService.getStandardWorkspaceNoSecrets(req.getScopeId(), false).getName();
      }
      default -> throw new IllegalArgumentException("Unexpected scope type: " + req.getScopeType());
    }
  }

  public UserInvitationRead accept(final InviteCodeRequestBody req, final AuthenticatedUser currentUser) {
    final UserInvitation invitation = service.getUserInvitationByInviteCode(req.getInviteCode());

    if (!invitation.getInvitedEmail().equalsIgnoreCase(currentUser.getEmail())) {
      throw new OperationNotAllowedException("Invited email does not match current user email.");
    }

    try {
      final UserInvitation accepted = service.acceptUserInvitation(req.getInviteCode(), currentUser.getUserId());
      return mapper.toApi(accepted);
    } catch (final InvitationStatusUnexpectedException e) {
      throw new ConflictException(e.getMessage());
    }
  }

  public UserInvitationRead cancel(final InviteCodeRequestBody req) {
    try {
      final UserInvitation canceled = service.cancelUserInvitation(req.getInviteCode());
      return mapper.toApi(canceled);
    } catch (final InvitationStatusUnexpectedException e) {
      throw new ConflictException(e.getMessage());
    }
  }

  // TODO implement `decline`

}
