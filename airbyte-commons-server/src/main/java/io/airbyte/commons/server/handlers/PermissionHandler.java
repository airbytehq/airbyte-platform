/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRead.StatusEnum;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.PermissionReadList;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.PermissionUpdate;
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Permission;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

  public PermissionHandler(
                           final PermissionPersistence permissionPersistence,
                           final Supplier<UUID> uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
    this.permissionPersistence = permissionPersistence;
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
      throws IOException {

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
    PermissionRead result;
    try {
      result = buildPermissionRead(permissionId);
    } catch (ConfigNotFoundException ex) {
      LOGGER.error("Config not found for permissionId: {} in CreatePermission.", permissionId);
      throw new IOException(ex);
    }
    return result;
  }

  private PermissionRead buildPermissionRead(final UUID permissionId) throws ConfigNotFoundException, IOException {
    final Optional<Permission> permission =
        permissionPersistence.getPermission(permissionId);
    if (permission.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.PERMISSION, permissionId);
    }

    return buildPermissionRead(permission.get());
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
   *
   * @param permissionUpdate The permission update.
   * @return The updated permission.
   * @throws IOException if unable to update the permissions.
   * @throws ConfigNotFoundException if unable to update the permissions.
   * @throws JsonValidationException if unable to update the permissions.
   */
  public PermissionRead updatePermission(final PermissionUpdate permissionUpdate)
      throws IOException, ConfigNotFoundException {
    final Permission permission = new Permission()
        .withPermissionId(permissionUpdate.getPermissionId())
        .withPermissionType(Enums.convertTo(permissionUpdate.getPermissionType(), Permission.PermissionType.class))
        .withUserId(permissionUpdate.getUserId())
        .withWorkspaceId(permissionUpdate.getWorkspaceId())
        .withOrganizationId(permissionUpdate.getOrganizationId());

    permissionPersistence.writePermission(permission);

    return buildPermissionRead(permissionUpdate.getPermissionId());
  }

  /**
   * Checks the permissions associated with a user.
   *
   * @param permissionCheckRequest The permission. check request.
   * @return The result of the permission check.
   * @throws IOException if unable to check the permission.
   * @throws JsonValidationException if unable to check the permission.
   */
  public PermissionCheckRead checkPermissions(final PermissionCheckRequest permissionCheckRequest) throws IOException {
    final List<PermissionRead> permissions = listPermissionsByUser(permissionCheckRequest.getUserId()).getPermissions();
    final boolean anyMatch = permissions.stream().anyMatch(p -> checkPermissions(permissionCheckRequest, p));
    return new PermissionCheckRead().status(anyMatch ? StatusEnum.SUCCEEDED : StatusEnum.FAILED);
  }

  private boolean checkPermissions(final PermissionCheckRequest permissionCheckRequest, final PermissionRead permissionRead) {
    if (!permissionCheckRequest.getUserId().equals(permissionRead.getUserId())) {
      return false;
    }

    // instance admin permissions have access to everything
    if (permissionRead.getPermissionType().equals(PermissionType.INSTANCE_ADMIN)) {
      return true;
    }

    if (permissionCheckRequest.getWorkspaceId() != null && !permissionCheckRequest.getWorkspaceId().equals(permissionRead.getWorkspaceId())) {
      return false;
    }

    if (permissionCheckRequest.getOrganizationId() != null
        && !permissionCheckRequest.getOrganizationId().equals(permissionRead.getOrganizationId())) {
      return false;
    }

    return permissionCheckRequest.getPermissionType().equals(permissionRead.getPermissionType());
  }

  /**
   * Given multiple workspaceIds, checks whether the user has at least the given permissionType for
   * all workspaceIds.
   *
   * @param permissionsCheckMultipleWorkspacesRequest The permissions check request
   * @return The result of the permission check.
   * @throws IOException If unable to check the permission.
   */
  @SuppressWarnings("LineLength")
  public PermissionCheckRead permissionsCheckMultipleWorkspaces(final PermissionsCheckMultipleWorkspacesRequest permissionsCheckMultipleWorkspacesRequest)
      throws IOException {
    final List<PermissionRead> permissions = listPermissionsByUser(permissionsCheckMultipleWorkspacesRequest.getUserId()).getPermissions();
    final List<UUID> permissionedWorkspaceIds = permissions.stream()
        .filter(
            (permission) -> permission.getPermissionType() != null
                && permission.getPermissionType().equals(permissionsCheckMultipleWorkspacesRequest.getPermissionType()))
        .map(PermissionRead::getWorkspaceId).toList();
    final boolean success =
        new HashSet<UUID>(permissionedWorkspaceIds).containsAll(new HashSet<UUID>(permissionsCheckMultipleWorkspacesRequest.getWorkspaceIds()));
    return success ? new PermissionCheckRead().status(StatusEnum.SUCCEEDED) : new PermissionCheckRead().status(StatusEnum.FAILED);
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
   * @throws ConfigNotFoundException if unable to delete the permission.
   */
  public void deletePermission(final PermissionIdRequestBody permissionIdRequestBody) throws IOException {
    permissionPersistence.deletePermissionById(permissionIdRequestBody.getPermissionId());
  }

}
