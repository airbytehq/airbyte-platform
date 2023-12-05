/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRead.StatusEnum;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionDeleteUserFromWorkspaceRequestBody;
import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.PermissionReadList;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.PermissionUpdate;
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.server.errors.OperationNotAllowedException;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Permission;
import io.airbyte.config.helpers.PermissionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.SQLOperationNotAllowedException;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PermissionHandler, provides basic CRUD operation access for permissions. Some are migrated from
 * Cloud PermissionHandler {@link io.airbyte.cloud.server.handlers.PermissionHandler}.
 */
@Singleton
public class PermissionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PermissionHandler.class);
  private final Supplier<UUID> uuidGenerator;
  private final PermissionPersistence permissionPersistence;
  private final WorkspaceService workspaceService;

  public PermissionHandler(
                           final PermissionPersistence permissionPersistence,
                           final WorkspaceService workspaceService,
                           @Named("uuidGenerator") final Supplier<UUID> uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
    this.permissionPersistence = permissionPersistence;
    this.workspaceService = workspaceService;
  }

  /**
   * Creates a new permission.
   *
   * @param permissionCreate The new permission.
   * @return The created permission.
   * @throws IOException if unable to retrieve the existing permissions.
   * @throws ConfigNotFoundException if unable to build the response.
   * @throws JsonValidationException if unable to validate the existing permission data.
   */
  public PermissionRead createPermission(final PermissionCreate permissionCreate)
      throws IOException, JsonValidationException {

    // INSTANCE_ADMIN permissions are only created in special cases, so we block them here.
    if (permissionCreate.getPermissionType().equals(PermissionType.INSTANCE_ADMIN)) {
      throw new JsonValidationException("Cannot create INSTANCE_ADMIN permission record.");
    }

    final Optional<PermissionRead> existingPermission = getExistingPermission(permissionCreate);
    if (existingPermission.isPresent()) {
      return existingPermission.get();
    }

    final UUID permissionId = permissionCreate.getPermissionId() != null ? permissionCreate.getPermissionId() : uuidGenerator.get();

    final Permission permission = new Permission()
        .withPermissionId(permissionId)
        .withPermissionType(Enums.convertTo(permissionCreate.getPermissionType(), Permission.PermissionType.class))
        .withUserId(permissionCreate.getUserId())
        .withWorkspaceId(permissionCreate.getWorkspaceId())
        .withOrganizationId(permissionCreate.getOrganizationId());

    permissionPersistence.writePermission(permission);
    final PermissionRead result;
    try {
      result = buildPermissionRead(permissionId);
    } catch (final ConfigNotFoundException ex) {
      LOGGER.error("Config not found for permissionId: {} in CreatePermission.", permissionId);
      throw new IOException(ex);
    }
    return result;
  }

  private Permission getPermissionById(final UUID permissionId) throws ConfigNotFoundException, IOException {
    final Optional<Permission> permission =
        permissionPersistence.getPermission(permissionId);
    if (permission.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.PERMISSION, permissionId);
    }
    return permission.get();
  }

  private PermissionRead buildPermissionRead(final UUID permissionId) throws ConfigNotFoundException, IOException {
    final Permission permission = getPermissionById(permissionId);
    return buildPermissionRead(permission);
  }

  private static PermissionRead buildPermissionRead(final Permission permission) {
    return new PermissionRead()
        .permissionId(permission.getPermissionId())
        .userId(permission.getUserId())
        .permissionType(Enums.convertTo(permission.getPermissionType(), PermissionType.class))
        .workspaceId(permission.getWorkspaceId())
        .organizationId(permission.getOrganizationId());
  }

  private Optional<PermissionRead> getExistingPermission(final PermissionCreate permissionCreate) throws IOException {
    final List<PermissionRead> existingPermissions = permissionPersistence.listPermissionsByUser(permissionCreate.getUserId()).stream()
        .map(PermissionHandler::buildPermissionRead)
        .filter(permission -> checkPermissionsAreEqual(permission, permissionCreate))
        .sorted(Comparator.comparing(PermissionRead::getPermissionId))
        .collect(Collectors.toList());

    if (existingPermissions.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(existingPermissions.get(0));
  }

  private boolean checkPermissionsAreEqual(final PermissionRead permission, final PermissionCreate permissionCreate) {
    if (!permission.getPermissionType().equals(permissionCreate.getPermissionType())) {
      return false;
    }
    if (permission.getWorkspaceId() == null && permissionCreate.getWorkspaceId() != null) {
      return false;
    }
    if (permission.getWorkspaceId() != null && !permission.getWorkspaceId().equals(permissionCreate.getWorkspaceId())) {
      return false;
    }
    if (permission.getOrganizationId() == null && permissionCreate.getOrganizationId() != null) {
      return false;
    }
    if (permission.getOrganizationId() != null && !permission.getOrganizationId().equals(permissionCreate.getOrganizationId())) {
      return false;
    }
    return true;
  }

  /**
   * Gets a permission by permission Id.
   *
   * @param permissionIdRequestBody request body including permission id.
   * @return The queried permission.
   * @throws IOException if unable to get the permissions.
   * @throws ConfigNotFoundException if unable to get the permissions.
   * @throws JsonValidationException if unable to get the permissions.
   */
  public PermissionRead getPermission(final PermissionIdRequestBody permissionIdRequestBody)
      throws ConfigNotFoundException, IOException {
    return buildPermissionRead(permissionIdRequestBody.getPermissionId());
  }

  /**
   * Updates the permissions.
   * <p>
   * We only allow updating permission type between workspace level roles OR organization level roles.
   * <p>
   * Valid examples: 1. update "workspace_xxx" to "workspace_yyy" 2. update "organization_xxx" to
   * "organization_yyy" (only invalid when demoting the LAST organization_admin role in an org to
   * another organization level role)
   * <p>
   * Invalid examples: 1. update "instance_admin" to any other types 2. update "workspace_xxx" to
   * "organization_xxx"/"instance_admin" 3. update "organization_xxx" to
   * "workspace_xxx"/"instance_admin"
   *
   * @param permissionUpdate The permission update.
   * @return The updated permission.
   * @throws IOException if unable to update the permissions.
   * @throws ConfigNotFoundException if unable to update the permissions.
   * @throws OperationNotAllowedException if update is prevented by business logic.
   */
  public PermissionRead updatePermission(final PermissionUpdate permissionUpdate)
      throws IOException, ConfigNotFoundException, OperationNotAllowedException, JsonValidationException {

    // INSTANCE_ADMIN permissions are only created in special cases, so we block them here.
    if (permissionUpdate.getPermissionType().equals(PermissionType.INSTANCE_ADMIN)) {
      throw new JsonValidationException("Cannot update permission record to INSTANCE_ADMIN.");
    }

    final Permission existingPermission = getPermissionById(permissionUpdate.getPermissionId());

    final Permission updatedPermission = new Permission()
        .withPermissionId(permissionUpdate.getPermissionId())
        .withPermissionType(Enums.convertTo(permissionUpdate.getPermissionType(), Permission.PermissionType.class))
        .withOrganizationId(existingPermission.getOrganizationId()) // cannot be updated
        .withWorkspaceId(existingPermission.getWorkspaceId()) // cannot be updated
        .withUserId(existingPermission.getUserId()); // cannot be updated
    try {
      permissionPersistence.writePermission(updatedPermission);
    } catch (final DataAccessException e) {
      if (e.getCause() instanceof SQLOperationNotAllowedException) {
        throw new OperationNotAllowedException(e.getCause().getMessage(), e);
      } else {
        throw new IOException(e);
      }
    }
    return buildPermissionRead(permissionUpdate.getPermissionId());
  }

  /**
   * Checks the permissions associated with a user. All user permissions are fetched and each one is
   * checked against the requested permission. If any of the user's permissions meet the requirements
   * of the permission check, then the check succeeds.
   *
   * @param permissionCheckRequest The permission check request.
   * @return The result of the permission check.
   * @throws IOException if unable to check the permission.
   */
  public PermissionCheckRead checkPermissions(final PermissionCheckRequest permissionCheckRequest) throws IOException {
    final List<PermissionRead> userPermissions = listPermissionsByUser(permissionCheckRequest.getUserId()).getPermissions();

    final boolean anyMatch =
        userPermissions.stream().anyMatch(userPermission -> Exceptions.toRuntime(() -> checkPermissions(permissionCheckRequest, userPermission)));

    return new PermissionCheckRead().status(anyMatch ? StatusEnum.SUCCEEDED : StatusEnum.FAILED);
  }

  /**
   * Checks whether a particular user permission meets the requirements of a particular permission
   * check request. Organization-level user permissions grant workspace-level permissions as long as
   * the workspace in question belongs to the user's organization, so this method contains logic to
   * see if the requested permission is for a workspace that the user permission should grant access
   * to.
   */
  private boolean checkPermissions(final PermissionCheckRequest permissionCheckRequest, final PermissionRead userPermission)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    if (mismatchedUserIds(userPermission, permissionCheckRequest)) {
      return false;
    }

    // if the user is an instance admin, return true immediately, since instance admins have access to
    // everything by definition.
    if (userPermission.getPermissionType().equals(PermissionType.INSTANCE_ADMIN)) {
      return true;
    }

    if (mismatchedWorkspaceIds(userPermission, permissionCheckRequest)) {
      return false;
    }

    if (mismatchedOrganizationIds(userPermission, permissionCheckRequest)) {
      return false;
    }

    if (requestedWorkspaceNotInOrganization(userPermission, permissionCheckRequest)) {
      return false;
    }

    // by this point, we know we can directly compare the user permission's type to the requested
    // permission's type, because all underlying user/workspace/organization IDs are valid.
    return PermissionHelper.definedPermissionGrantsTargetPermission(
        Enums.convertTo(userPermission.getPermissionType(), Permission.PermissionType.class),
        Enums.convertTo(permissionCheckRequest.getPermissionType(), Permission.PermissionType.class));
  }

  // check if this permission request is for a user that doesn't match the user permission.
  // in practice, this shouldn't happen because we fetch user permissions based on the request.
  private boolean mismatchedUserIds(final PermissionRead userPermission, final PermissionCheckRequest request) {
    return !userPermission.getUserId().equals(request.getUserId());
  }

  // check if this permission request is for a workspace that doesn't match the user permission.
  private boolean mismatchedWorkspaceIds(final PermissionRead userPermission, final PermissionCheckRequest request) {
    return userPermission.getWorkspaceId() != null && !userPermission.getWorkspaceId().equals(request.getWorkspaceId());
  }

  // check if this permission request is for an organization that doesn't match the user permission.
  private boolean mismatchedOrganizationIds(final PermissionRead userPermission, final PermissionCheckRequest request) {
    return userPermission.getOrganizationId() != null
        && request.getOrganizationId() != null
        && !userPermission.getOrganizationId().equals(request.getOrganizationId());
  }

  // check if this permission request is for a workspace that belongs to a different organization than
  // the user permission.
  @SuppressWarnings("PMD.PreserveStackTrace")
  private boolean requestedWorkspaceNotInOrganization(final PermissionRead userPermission, final PermissionCheckRequest request)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    // if the user permission is for an organization, and the request is for a workspace, return true if
    // the workspace
    // does not belong to the organization.
    if (userPermission.getOrganizationId() != null && request.getWorkspaceId() != null) {
      final UUID requestedWorkspaceOrganizationId;
      try {
        requestedWorkspaceOrganizationId = workspaceService.getStandardWorkspaceNoSecrets(request.getWorkspaceId(), false).getOrganizationId();
      } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
        throw new ConfigNotFoundException(e.getType(), e.getConfigId());
      }
      return !requestedWorkspaceOrganizationId.equals(userPermission.getOrganizationId());
    }

    // else, not a workspace-level request with an org-level user permission, so return false.
    return false;
  }

  /**
   * Given multiple workspaceIds, checks whether the user has at least the given permissionType for
   * all workspaceIds.
   *
   * @param multiRequest The permissions check request with multiple workspaces
   * @return The result of the permission check.
   * @throws IOException If unable to check the permission.
   */
  @SuppressWarnings("LineLength")
  public PermissionCheckRead permissionsCheckMultipleWorkspaces(final PermissionsCheckMultipleWorkspacesRequest multiRequest) {

    // Turn the multiple-request into a list of individual requests, one per workspace
    final List<PermissionCheckRequest> permissionCheckRequests = multiRequest.getWorkspaceIds().stream()
        .map(workspaceId -> new PermissionCheckRequest()
            .userId(multiRequest.getUserId())
            .permissionType(multiRequest.getPermissionType())
            .workspaceId(workspaceId))
        .toList();

    // Perform the individual permission checks and store the results in a list
    final List<PermissionCheckRead> results = permissionCheckRequests.stream()
        .map(permissionCheckRequest -> {
          try {
            return checkPermissions(permissionCheckRequest);
          } catch (final IOException e) {
            LOGGER.error("Error checking permissions for request: {}", permissionCheckRequest);
            return new PermissionCheckRead().status(StatusEnum.FAILED);
          }
        }).toList();

    // If each individual workspace check succeeded, return an overall success. Otherwise, return an
    // overall failure.
    return results.stream().allMatch(result -> result.getStatus().equals(StatusEnum.SUCCEEDED))
        ? new PermissionCheckRead().status(StatusEnum.SUCCEEDED)
        : new PermissionCheckRead().status(StatusEnum.FAILED);
  }

  public Boolean isUserInstanceAdmin(final UUID userId) throws IOException {
    return permissionPersistence.isUserInstanceAdmin(userId);
  }

  /**
   * Lists the permissions for a workspace.
   *
   * @param workspaceId The workspace identifier.
   * @return The permissions for the given workspace.
   * @throws IOException if unable to retrieve the permissions for the user.
   * @throws JsonValidationException if unable to retrieve the permissions for the user.
   */
  public PermissionReadList listPermissionsByWorkspaceId(final UUID workspaceId) throws IOException {
    final List<Permission> permissions = permissionPersistence.listPermissionByWorkspace(workspaceId);
    return new PermissionReadList().permissions(permissions.stream()
        .map(PermissionHandler::buildPermissionRead)
        .collect(Collectors.toList()));
  }

  /**
   * Lists the permissions by user.
   *
   * @param userId The user ID.
   * @return The permissions for the given user.
   * @throws IOException if unable to retrieve the permissions for the user.
   * @throws JsonValidationException if unable to retrieve the permissions for the user.
   */
  public PermissionReadList listPermissionsByUser(final UUID userId) throws IOException {
    final List<Permission> permissions = permissionPersistence.listPermissionsByUser(userId);
    return new PermissionReadList().permissions(permissions.stream()
        .map(PermissionHandler::buildPermissionRead)
        .collect(Collectors.toList()));
  }

  /**
   * Deletes a permission.
   *
   * @param permissionIdRequestBody The permission to be deleted.
   * @throws IOException if unable to delete the permission.
   * @throws OperationNotAllowedException if deletion is prevented by business logic.
   */
  public void deletePermission(final PermissionIdRequestBody permissionIdRequestBody) throws IOException {
    try {
      permissionPersistence.deletePermissionById(permissionIdRequestBody.getPermissionId());
    } catch (final DataAccessException e) {
      if (e.getCause() instanceof SQLOperationNotAllowedException) {
        throw new OperationNotAllowedException(e.getCause().getMessage(), e);
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Delete all permission records that match a particular userId and workspaceId.
   */
  public void deleteUserFromWorkspace(final PermissionDeleteUserFromWorkspaceRequestBody deleteUserFromWorkspaceRequestBody) throws IOException {
    final UUID userId = deleteUserFromWorkspaceRequestBody.getUserIdToRemove();
    final UUID workspaceId = deleteUserFromWorkspaceRequestBody.getWorkspaceId();

    // delete all workspace-level permissions that match the userId and workspaceId
    permissionPersistence.listPermissionsByUser(userId).stream()
        .filter(permission -> permission.getWorkspaceId() != null && permission.getWorkspaceId().equals(workspaceId))
        .forEach(permission -> Exceptions.toRuntime(() -> permissionPersistence.deletePermissionById(permission.getPermissionId())));
  }

}
