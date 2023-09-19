/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONFIG_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_DEFINITION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves organization or workspace IDs from HTTP headers.
 */
@Slf4j
@Singleton
public class AuthenticationHeaderResolver {

  private final WorkspaceHelper workspaceHelper;

  public AuthenticationHeaderResolver(final WorkspaceHelper workspaceHelper) {
    this.workspaceHelper = workspaceHelper;
  }

  /**
   * Resolve corresponding organization ID. Currently we support two ways to resolve organization ID:
   * 1. If the organization ID is provided in the header, we will use it directly. 2. Otherwise, we
   * infer the workspace ID from the header and use the workspace ID to find the organization Id.
   */
  public List<UUID> resolveOrganization(final Map<String, String> properties) {
    log.debug("properties: {}", properties);

    if (properties.containsKey(ORGANIZATION_ID_HEADER)) {
      return List.of(UUID.fromString(properties.get(ORGANIZATION_ID_HEADER)));
    }
    // Else, determine the organization from workspace related fields.

    final List<UUID> workspaceIds = resolveWorkspace(properties);
    if (workspaceIds == null) {
      return null;
    }
    return workspaceIds.stream().map(workspaceId -> workspaceHelper.getOrganizationForWorkspace(workspaceId)).collect(Collectors.toList());
  }

  /**
   * Resolves workspaces from header.
   */
  @SuppressWarnings("PMD.CyclomaticComplexity") // This is an indication that the workspace ID as a group for auth needs refactoring
  public List<UUID> resolveWorkspace(final Map<String, String> properties) {
    log.debug("properties: {}", properties);
    try {
      if (properties.containsKey(WORKSPACE_ID_HEADER)) {
        final String workspaceId = properties.get(WORKSPACE_ID_HEADER);
        return List.of(UUID.fromString(workspaceId));
      } else if (properties.containsKey(CONNECTION_ID_HEADER)) {
        final String connectionId = properties.get(CONNECTION_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(connectionId)));
      } else if (properties.containsKey(SOURCE_ID_HEADER) && properties.containsKey(DESTINATION_ID_HEADER)) {
        final String destinationId = properties.get(DESTINATION_ID_HEADER);
        final String sourceId = properties.get(SOURCE_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForConnection(UUID.fromString(sourceId), UUID.fromString(destinationId)));
      } else if (properties.containsKey(DESTINATION_ID_HEADER)) {
        final String destinationId = properties.get(DESTINATION_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForDestinationId(UUID.fromString(destinationId)));
      } else if (properties.containsKey(JOB_ID_HEADER)) {
        final String jobId = properties.get(JOB_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForJobId(Long.valueOf(jobId)));
      } else if (properties.containsKey(SOURCE_ID_HEADER)) {
        final String sourceId = properties.get(SOURCE_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForSourceId(UUID.fromString(sourceId)));
      } else if (properties.containsKey(SOURCE_DEFINITION_ID_HEADER)) {
        final String sourceDefinitionId = properties.get(SOURCE_DEFINITION_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForSourceId(UUID.fromString(sourceDefinitionId)));
      } else if (properties.containsKey(OPERATION_ID_HEADER)) {
        final String operationId = properties.get(OPERATION_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForOperationId(UUID.fromString(operationId)));
      } else if (properties.containsKey(CONFIG_ID_HEADER)) {
        final String configId = properties.get(CONFIG_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(configId)));
      } else if (properties.containsKey(WORKSPACE_IDS_HEADER)) {
        return resolveWorkspaces(properties);
      } else {
        log.debug("Request does not contain any headers that resolve to a workspace ID.");
        return null;
      }
    } catch (final JsonValidationException | ConfigNotFoundException e) {
      log.debug("Unable to resolve workspace ID.", e);
      return null;
    }
  }

  private List<UUID> resolveWorkspaces(final Map<String, String> properties) {
    final String workspaceIds = properties.get(WORKSPACE_IDS_HEADER);
    log.debug("workspaceIds from header: {}", workspaceIds);
    if (workspaceIds != null) {
      final List<String> deserialized = Jsons.deserialize(workspaceIds, List.class);
      return deserialized.stream().map(UUID::fromString).toList();
    }
    log.debug("Request does not contain any headers that resolve to a list of workspace IDs.");
    return null;
  }

}
