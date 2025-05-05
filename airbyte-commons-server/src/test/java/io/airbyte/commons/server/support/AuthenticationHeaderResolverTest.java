/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_TYPE_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @ValueSource(classes = {JsonValidationException.class, NumberFormatException.class, ConfigNotFoundException.class})
  void testResolvingWithException(final Class<Throwable> exceptionType)
      throws JsonValidationException, ConfigNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
      IllegalAccessException {
    final UUID connectionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(CONNECTION_ID_HEADER, connectionId.toString());
    final Throwable exception = exceptionType.equals(ConfigNotFoundException.class) ? new ConfigNotFoundException("type", "id")
        : exceptionType.getDeclaredConstructor(String.class).newInstance("test");
    when(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenThrow(exception);

    final List<UUID> workspaceId = resolver.resolveWorkspace(properties);
    assertNull(workspaceId);
  }

  @Test
  void testResolvingMultiple() {
    final List<UUID> workspaceIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    final Map<String, String> properties = Map.of(WORKSPACE_IDS_HEADER, Jsons.serialize(workspaceIds));

    final List<UUID> resolvedWorkspaceIds = resolver.resolveWorkspace(properties);
    assertEquals(workspaceIds, resolvedWorkspaceIds);
  }

  @Test
  void testResolvingOrganizationDirectlyFromHeader() {
    final UUID organizationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(ORGANIZATION_ID_HEADER, organizationId.toString());

    final List<UUID> result = resolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

  @Test
  void testResolvingOrganizationFromWorkspaceHeader() {
    final UUID organizationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(WORKSPACE_ID_HEADER, workspaceId.toString());
    when(workspaceHelper.getOrganizationForWorkspace(workspaceId)).thenReturn(organizationId);

    final List<UUID> result = resolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

  @Test
  void testResolvingWorkspaceFromPermissionHeader() throws IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID permissionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(PERMISSION_ID_HEADER, permissionId.toString());
    when(permissionHandler.getPermissionRead(new PermissionIdRequestBody().permissionId(permissionId)))
        .thenReturn(new PermissionRead().workspaceId(workspaceId));

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingOrganizationFromPermissionHeader() throws IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID organizationId = UUID.randomUUID();
    final UUID permissionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(PERMISSION_ID_HEADER, permissionId.toString());
    when(permissionHandler.getPermissionRead(new PermissionIdRequestBody().permissionId(permissionId)))
        .thenReturn(new PermissionRead().organizationId(organizationId));

    final List<UUID> result = resolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

  @Test
  void testResolvingAuthUserFromUserId() throws Exception {
    final UUID userId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(AIRBYTE_USER_ID_HEADER, userId.toString());
    final Set<String> expectedAuthUserIds = Set.of(AUTH_USER_ID, "some-other-id");
    when(userPersistence.listAuthUserIdsForUser(userId)).thenReturn(expectedAuthUserIds.stream().toList());

    final Set<String> resolvedAuthUserIds = resolver.resolveAuthUserIds(properties);

    assertEquals(expectedAuthUserIds, resolvedAuthUserIds);
  }

  @Test
  void testResolvingAuthUserFromCreatorUserId() throws Exception {
    final UUID userId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(CREATOR_USER_ID_HEADER, userId.toString());
    final Set<String> expectedAuthUserIds = Set.of(AUTH_USER_ID, "some-other-id");
    when(userPersistence.listAuthUserIdsForUser(userId)).thenReturn(expectedAuthUserIds.stream().toList());

    final Set<String> resolvedAuthUserIds = resolver.resolveAuthUserIds(properties);

    assertEquals(expectedAuthUserIds, resolvedAuthUserIds);
  }

  @Test
  void testResolvingAuthUserFromExternalAuthUserId() {
    final Map<String, String> properties = Map.of(EXTERNAL_AUTH_ID_HEADER, AUTH_USER_ID);

    final Set<String> resolvedAuthUserIds = resolver.resolveAuthUserIds(properties);

    assertEquals(Set.of(AUTH_USER_ID), resolvedAuthUserIds);
  }

  @Test
  void testResolvingWorkspaceIdFromScopeTypeAndScopeId() {
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(SCOPE_TYPE_HEADER, "workspace", SCOPE_ID_HEADER, workspaceId.toString());

    final List<UUID> result = resolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingOrganizationIdFromScopeTypeAndScopeId() {
    final UUID organizationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(SCOPE_TYPE_HEADER, "organization", SCOPE_ID_HEADER, organizationId.toString());

    final List<UUID> result = resolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

}
