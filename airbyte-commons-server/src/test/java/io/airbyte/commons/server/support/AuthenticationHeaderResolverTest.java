/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.EMAIL_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.config.User;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AuthenticationHeaderResolverTest {

  private static final String AUTH_USER_ID = "authUserId";

  private WorkspaceHelper workspaceHelper;
  private PermissionHandler permissionHandler;
  private UserPersistence userPersistence;
  private AuthenticationHeaderResolver resolver;

  @BeforeEach
  void setup() {
    this.workspaceHelper = mock(WorkspaceHelper.class);
    this.permissionHandler = mock(PermissionHandler.class);
    this.userPersistence = mock(UserPersistence.class);
    this.resolver = new AuthenticationHeaderResolver(workspaceHelper, permissionHandler, userPersistence);
  }

  @Test
  void testResolvingFromWorkspaceId() {
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(WORKSPACE_ID_HEADER, workspaceId.toString());

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromConnectionId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(CONNECTION_ID_HEADER, connectionId.toString());
    when(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenReturn(workspaceId);

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromConnectionIds() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID connectionId2 = UUID.randomUUID();

    final Map<String, String> properties = Map.of(CONNECTION_IDS_HEADER, Jsons.serialize(List.of(connectionId.toString(), connectionId2.toString())));
    when(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenReturn(workspaceId);
    when(workspaceHelper.getWorkspaceForConnectionId(connectionId2)).thenReturn(workspaceId);

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId, workspaceId), result);
  }

  @Test
  void testResolvingFromSourceAndDestinationId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(DESTINATION_ID_HEADER, destinationId.toString(), SOURCE_ID_HEADER, sourceId.toString());
    when(workspaceHelper.getWorkspaceForConnection(sourceId, destinationId)).thenReturn(workspaceId);

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromDestinationId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(DESTINATION_ID_HEADER, destinationId.toString());
    when(workspaceHelper.getWorkspaceForDestinationId(destinationId)).thenReturn(workspaceId);

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromJobId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final Long jobId = System.currentTimeMillis();
    final Map<String, String> properties = Map.of(JOB_ID_HEADER, String.valueOf(jobId));
    when(workspaceHelper.getWorkspaceForJobId(jobId)).thenReturn(workspaceId);

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromSourceId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(SOURCE_ID_HEADER, sourceId.toString());
    when(workspaceHelper.getWorkspaceForSourceId(sourceId)).thenReturn(workspaceId);

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromOperationId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID operationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(OPERATION_ID_HEADER, operationId.toString());
    when(workspaceHelper.getWorkspaceForOperationId(operationId)).thenReturn(workspaceId);

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromNoMatchingProperties() {
    final Map<String, String> properties = Map.of();
    final List<UUID> workspaceId = resolver.resolveWorkspace(properties);
    assertNull(workspaceId);
  }

  @Test
  void testResolvingWithException() throws JsonValidationException, ConfigNotFoundException {
    final UUID connectionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(CONNECTION_ID_HEADER, connectionId.toString());
    when(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenThrow(new JsonValidationException("test"));

    final List<UUID> workspaceId = resolver.resolveWorkspace(properties);
    assertNull(workspaceId);
  }

  @Test
  void testResolvingMultiple() throws JsonValidationException, ConfigNotFoundException {
    final List<UUID> workspaceIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    final Map<String, String> properties = Map.of(WORKSPACE_IDS_HEADER, Jsons.serialize(workspaceIds));

    final List<UUID> resolvedWorkspaceIds = resolver.resolveWorkspace(properties);
    assertEquals(workspaceIds, resolvedWorkspaceIds);
  }

  @Test
  void testResolvingOrganizationDirectlyFromHeader() throws ConfigNotFoundException {
    final UUID organizationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(ORGANIZATION_ID_HEADER, organizationId.toString());

    final List<UUID> result = resolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

  @Test
  void testResolvingOrganizationFromWorkspaceHeader() throws ConfigNotFoundException {
    final UUID organizationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(WORKSPACE_ID_HEADER, workspaceId.toString());
    when(workspaceHelper.getOrganizationForWorkspace(workspaceId)).thenReturn(organizationId);

    final List<UUID> result = resolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

  @Test
  void testResolvingWorkspaceFromPermissionHeader() throws ConfigNotFoundException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID permissionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(PERMISSION_ID_HEADER, permissionId.toString());
    when(permissionHandler.getPermission(new PermissionIdRequestBody().permissionId(permissionId)))
        .thenReturn(new PermissionRead().workspaceId(workspaceId));

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingOrganizationFromPermissionHeader() throws ConfigNotFoundException, IOException {
    final UUID organizationId = UUID.randomUUID();
    final UUID permissionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(PERMISSION_ID_HEADER, permissionId.toString());
    when(permissionHandler.getPermission(new PermissionIdRequestBody().permissionId(permissionId)))
        .thenReturn(new PermissionRead().organizationId(organizationId));

    final List<UUID> result = resolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

  @Test
  void testResolvingAuthUserFromUserId() throws Exception {
    final UUID userId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(AIRBYTE_USER_ID_HEADER, userId.toString());
    final User expectedUser = new User().withUserId(userId).withAuthUserId(AUTH_USER_ID);
    when(userPersistence.getUser(userId)).thenReturn(Optional.of(expectedUser));

    final String resolvedAuthUserId = resolver.resolveUserAuthId(properties);
    assertEquals(expectedUser.getAuthUserId(), resolvedAuthUserId);
  }

  @Test
  void testResolvingAuthUserFromCreatorUserId() throws Exception {
    final UUID userId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(CREATOR_USER_ID_HEADER, userId.toString());
    final User expectedUser = new User().withUserId(userId).withAuthUserId(AUTH_USER_ID);
    when(userPersistence.getUser(userId)).thenReturn(Optional.of(expectedUser));

    final String resolvedAuthUserId = resolver.resolveUserAuthId(properties);
    assertEquals(expectedUser.getAuthUserId(), resolvedAuthUserId);
  }

  @Test
  void testResolvingAuthUserFromExternalAuthUserId() throws Exception {
    final UUID userId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(EXTERNAL_AUTH_ID_HEADER, AUTH_USER_ID);
    final User expectedUser = new User().withUserId(userId).withAuthUserId(AUTH_USER_ID);
    when(userPersistence.getUser(userId)).thenReturn(Optional.of(expectedUser));

    final String resolvedAuthUserId = resolver.resolveUserAuthId(properties);
    assertEquals(expectedUser.getAuthUserId(), resolvedAuthUserId);
  }

  @Test
  void testResolvingAuthUserFromEmail() throws Exception {
    final String email = "random@email.com";
    final Map<String, String> properties = Map.of(EMAIL_HEADER, email);
    final User expectedUser = new User().withEmail(email).withAuthUserId(AUTH_USER_ID);
    when(userPersistence.getUserByEmail(email)).thenReturn(Optional.of(expectedUser));

    final String resolvedAuthUserId = resolver.resolveUserAuthId(properties);
    assertEquals(expectedUser.getAuthUserId(), resolvedAuthUserId);
  }

}
