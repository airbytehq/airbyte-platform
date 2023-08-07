/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PermissionHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private PermissionHandler permissionHandler;
  private PermissionPersistence permissionPersistence;

  private final UUID userId = UUID.randomUUID();
  private final UUID workspaceId = UUID.randomUUID();
  private final UUID permissionId = UUID.randomUUID();
  private final Permission permission = new Permission()
      .withPermissionId(permissionId)
      .withUserId(userId)
      .withWorkspaceId(workspaceId)
      .withPermissionType(PermissionType.WORKSPACE_OWNER);

  @BeforeEach
  void setUp() {
    permissionPersistence = mock(PermissionPersistence.class);
    uuidSupplier = mock(Supplier.class);
    permissionHandler = new PermissionHandler(permissionPersistence, uuidSupplier);
  }

  @Test
  void testCreatePermission() throws IOException, JsonValidationException, ConfigNotFoundException {
    final List<Permission> existingPermissions = List.of();
    when(permissionPersistence.listPermissionsByUser(any())).thenReturn(existingPermissions);
    when(uuidSupplier.get()).thenReturn(permissionId);
    when(permissionPersistence.getPermission(any())).thenReturn(Optional.of(permission));
    final PermissionCreate permissionCreate = new PermissionCreate()
        .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_OWNER)
        .userId(userId)
        .workspaceId(workspaceId);
    final PermissionRead actualRead = permissionHandler.createPermission(permissionCreate);
    final PermissionRead expectedRead = new PermissionRead()
        .permissionId(permissionId)
        .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_OWNER)
        .userId(userId)
        .workspaceId(workspaceId);

    assertEquals(expectedRead, actualRead);

  }

}
