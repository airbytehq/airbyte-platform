/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONFIG_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.IS_PUBLIC_API_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_TYPE_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER;

import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.config.ScopeType;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves organization or workspace IDs from HTTP headers.
 */
@Singleton
public class AuthenticationHeaderResolver {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final WorkspaceHelper workspaceHelper;
  private final PermissionHandler permissionHandler;
  private final UserPersistence userPersistence;

  public AuthenticationHeaderResolver(final WorkspaceHelper workspaceHelper,
                                      final PermissionHandler permissionHandler,
                                      final UserPersistence userPersistence) {
    this.workspaceHelper = workspaceHelper;
    this.permissionHandler = permissionHandler;
    this.userPersistence = userPersistence;
  }

  /**
   * Resolve corresponding organization ID. Currently we support two ways to resolve organization ID:
   * 1. If the organization ID is provided in the header, we will use it directly. 2. Otherwise, we
   * infer the workspace ID from the header and use the workspace ID to find the organization Id.
   */
  public @Nullable List<UUID> resolveOrganization(final Map<String, String> properties) {
    log.debug("properties: {}", properties);
    try {
      if (properties.containsKey(ORGANIZATION_ID_HEADER)) {
        return List.of(UUID.fromString(properties.get(ORGANIZATION_ID_HEADER)));
      } else if (properties.containsKey(SCOPE_TYPE_HEADER) && properties.containsKey(SCOPE_ID_HEADER)
          && properties.get(SCOPE_TYPE_HEADER).equalsIgnoreCase(ScopeType.ORGANIZATION.value())) {
        // if the scope type is organization, we can use the scope id directly to resolve an organization
        // id.
        final String organizationId = properties.get(SCOPE_ID_HEADER);
        return List.of(UUID.fromString(organizationId));
      } else {
        // resolving by permission id requires a database fetch, so we
        // handle it last and with a dedicated check to minimize latency.
        final UUID organizationId = resolveOrganizationIdFromPermissionHeader(properties);
        if (organizationId != null) {
          return List.of(organizationId);
        }
      }
      // Else, determine the organization from workspace related fields.
      final List<UUID> workspaceIds = resolveWorkspace(properties);
      if (workspaceIds == null) {
        return null;
      }

      final List<UUID> organizationIds = new ArrayList<>();
      for (final UUID workspaceId : workspaceIds) {
        try {
          organizationIds.add(workspaceHelper.getOrganizationForWorkspace(workspaceId));
        } catch (final Exception e) {
          log.debug("Unable to resolve organization ID for workspace ID: {}", workspaceId, e);
        }
      }
      return organizationIds;
    } catch (final IllegalArgumentException | ConfigNotFoundException | io.airbyte.config.persistence.ConfigNotFoundException e) {
      log.debug("Unable to resolve organization ID.", e);
      return null;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Resolves workspaces from header.
   */
  @SuppressWarnings("PMD.CyclomaticComplexity") // This is an indication that the workspace ID as a group for auth needs refactoring
  public @Nullable List<UUID> resolveWorkspace(final Map<String, String> properties) {
    log.debug("properties: {}", properties);
    try {
      if (properties.containsKey(WORKSPACE_ID_HEADER)) {
        final String workspaceId = properties.get(WORKSPACE_ID_HEADER);
        return List.of(UUID.fromString(workspaceId));
      } else if (properties.containsKey(CONNECTION_ID_HEADER)) {
        final String connectionId = properties.get(CONNECTION_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(connectionId)));
      } else if (properties.containsKey(CONNECTION_IDS_HEADER)) {
        return resolveConnectionIds(properties);
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
      } else if (properties.containsKey(OPERATION_ID_HEADER)) {
        final String operationId = properties.get(OPERATION_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForOperationId(UUID.fromString(operationId)));
      } else if (properties.containsKey(CONFIG_ID_HEADER)) {
        final String configId = properties.get(CONFIG_ID_HEADER);
        return List.of(workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(configId)));
      } else if (properties.containsKey(WORKSPACE_IDS_HEADER)) {
        // If workspaceIds were passed in as empty list [], they apparently don't show up in the headers so
        // this will be skipped
        // The full list of workspace ID permissions is handled below in the catch-all.
        return resolveWorkspaces(properties);
      } else if (properties.containsKey(SCOPE_TYPE_HEADER) && properties.containsKey(SCOPE_ID_HEADER)
          && properties.get(SCOPE_TYPE_HEADER).equalsIgnoreCase(ScopeType.WORKSPACE.value())) {
        // if the scope type is workspace, we can use the scope id directly to resolve a workspace id.
        final String workspaceId = properties.get(SCOPE_ID_HEADER);
        return List.of(UUID.fromString(workspaceId));
      } else if (!properties.containsKey(WORKSPACE_IDS_HEADER) && properties.containsKey(IS_PUBLIC_API_HEADER)) {
        // If the WORKSPACE_IDS_HEADER is missing and this is a public API request, we should return empty
        // list so that we pass through
        // the permission check and the controller/handler can either pull all workspaces the user has
        // access to or fail.
        return Collections.emptyList();
      } else {
        // resolving by permission id requires a database fetch, so we
        // handle it last and with a dedicated check to minimize latency.
        final UUID workspaceId = resolveWorkspaceIdFromPermissionHeader(properties);
        if (workspaceId != null) {
          return List.of(workspaceId);
        }

        log.debug("Request does not contain any headers that resolve to a workspace ID.");
        return null;
      }
    } catch (final IllegalArgumentException | JsonValidationException | ConfigNotFoundException
        | io.airbyte.config.persistence.ConfigNotFoundException e) {
      log.debug("Unable to resolve workspace ID.", e);
      return null;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public @Nullable Set<String> resolveAuthUserIds(final Map<String, String> properties) {
    log.debug("properties: {}", properties);
    try {
      if (properties.containsKey(EXTERNAL_AUTH_ID_HEADER)) {
        final String authUserId = properties.get(EXTERNAL_AUTH_ID_HEADER);
        return Set.of(authUserId);
      } else if (properties.containsKey(AIRBYTE_USER_ID_HEADER)) {
        return resolveAirbyteUserIdToAuthUserIds(properties.get(AIRBYTE_USER_ID_HEADER));
      } else if (properties.containsKey(CREATOR_USER_ID_HEADER)) {
        return resolveAirbyteUserIdToAuthUserIds(properties.get(CREATOR_USER_ID_HEADER));
      } else {
        log.debug("Request does not contain any headers that resolve to a user ID.");
        return null;
      }
    } catch (final Exception e) {
      log.debug("Unable to resolve user ID.", e);
      return null;
    }
  }

  private Set<String> resolveAirbyteUserIdToAuthUserIds(final String airbyteUserId) throws IOException {
    final List<String> authUserIds = userPersistence.listAuthUserIdsForUser(UUID.fromString(airbyteUserId));

    if (authUserIds.isEmpty()) {
      throw new IllegalArgumentException(String.format("Could not find any authUserIds for userId %s", airbyteUserId));
    }

    return new HashSet<>(authUserIds);
  }

  private UUID resolveWorkspaceIdFromPermissionHeader(final Map<String, String> properties)
      throws ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    if (!properties.containsKey(PERMISSION_ID_HEADER)) {
      return null;
    }
    final PermissionRead permission = permissionHandler.getPermissionRead(
        new PermissionIdRequestBody().permissionId(UUID.fromString(properties.get(PERMISSION_ID_HEADER))));
    return permission.getWorkspaceId();
  }

  private UUID resolveOrganizationIdFromPermissionHeader(final Map<String, String> properties)
      throws ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    if (!properties.containsKey(PERMISSION_ID_HEADER)) {
      return null;
    }
    final PermissionRead permission = permissionHandler.getPermissionRead(
        new PermissionIdRequestBody().permissionId(UUID.fromString(properties.get(PERMISSION_ID_HEADER))));
    return permission.getOrganizationId();
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

  private List<UUID> resolveConnectionIds(final Map<String, String> properties) {
    final String connectionIds = properties.get(CONNECTION_IDS_HEADER);
    if (connectionIds != null) {
      final List<String> deserialized = Jsons.deserialize(connectionIds, List.class);
      return deserialized.stream().map(connectionId -> {
        try {
          return workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(connectionId));
        } catch (final JsonValidationException | ConfigNotFoundException e) {
          throw new RuntimeException(e);
        }
      }).toList();
    }
    return null;
  }

}
