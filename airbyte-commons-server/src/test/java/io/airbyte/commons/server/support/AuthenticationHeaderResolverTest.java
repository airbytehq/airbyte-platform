/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_DEFINITION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AuthenticationHeaderResolverTest {

  @Test
  void testResolvingFromWorkspaceId() {
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(WORKSPACE_ID_HEADER, workspaceId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromConnectionId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(CONNECTION_ID_HEADER, connectionId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenReturn(workspaceId);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromSourceAndDestinationId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(DESTINATION_ID_HEADER, destinationId.toString(), SOURCE_ID_HEADER, sourceId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForConnection(sourceId, destinationId)).thenReturn(workspaceId);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromDestinationId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(DESTINATION_ID_HEADER, destinationId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForDestinationId(destinationId)).thenReturn(workspaceId);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromJobId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final Long jobId = System.currentTimeMillis();
    final Map<String, String> properties = Map.of(JOB_ID_HEADER, String.valueOf(jobId));

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForJobId(jobId)).thenReturn(workspaceId);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromSourceId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(SOURCE_ID_HEADER, sourceId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForSourceId(sourceId)).thenReturn(workspaceId);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromSourceDefinitionId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(SOURCE_DEFINITION_ID_HEADER, sourceDefinitionId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForSourceId(sourceDefinitionId)).thenReturn(workspaceId);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromOperationId() throws JsonValidationException, ConfigNotFoundException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID operationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(OPERATION_ID_HEADER, operationId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForOperationId(operationId)).thenReturn(workspaceId);

    final List<UUID> result = workspaceResolver.resolveWorkspace(properties);
    assertEquals(List.of(workspaceId), result);
  }

  @Test
  void testResolvingFromNoMatchingProperties() {
    final Map<String, String> properties = Map.of();
    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);
    final List<UUID> workspaceId = workspaceResolver.resolveWorkspace(properties);
    assertNull(workspaceId);
  }

  @Test
  void testResolvingWithException() throws JsonValidationException, ConfigNotFoundException {
    final UUID connectionId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(CONNECTION_ID_HEADER, connectionId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    when(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenThrow(new JsonValidationException("test"));
    final List<UUID> workspaceId = workspaceResolver.resolveWorkspace(properties);
    assertNull(workspaceId);
  }

  @Test
  void testResolvingMultiple() throws JsonValidationException, ConfigNotFoundException {
    final List<UUID> workspaceIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    final Map<String, String> properties = Map.of(WORKSPACE_IDS_HEADER, Jsons.serialize(workspaceIds));

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    final List<UUID> resolvedWorkspaceIds = workspaceResolver.resolveWorkspace(properties);
    assertEquals(workspaceIds, resolvedWorkspaceIds);
  }

  @Test
  void testResolvingOrganizationDirectlyFromHeader() throws ConfigNotFoundException {
    final UUID organizationId = UUID.randomUUID();
    final Map<String, String> properties = Map.of(ORGANIZATION_ID_HEADER, organizationId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);

    final List<UUID> result = workspaceResolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

  @Test
  void testResolvingOrganizationFromWorkspaceHeader() throws ConfigNotFoundException {
    final UUID organizationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();

    final Map<String, String> properties = Map.of(WORKSPACE_ID_HEADER, workspaceId.toString());

    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final AuthenticationHeaderResolver workspaceResolver = new AuthenticationHeaderResolver(workspaceHelper);
    when(workspaceHelper.getOrganizationForWorkspace(workspaceId)).thenReturn(organizationId);
    final List<UUID> result = workspaceResolver.resolveOrganization(properties);
    assertEquals(List.of(organizationId), result);
  }

}
