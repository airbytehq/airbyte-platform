/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling;

import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.config.Geography;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.ShouldRunOnExpandedGkeDataplane;
import io.airbyte.featureflag.ShouldRunOnGkeDataplane;
import io.airbyte.featureflag.Workspace;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Decides which Task Queue should be used for a given connection's sync operations, based on the
 * configured {@link Geography}.
 */
@Singleton
@Slf4j
public class RouterService {

  private final ConfigRepository configRepository;
  private final TaskQueueMapper taskQueueMapper;

  private final FeatureFlagClient featureFlagClient;

  private static final Set<TemporalJobType> WORKSPACE_ROUTING_JOB_TYPE_SET =
      Set.of(TemporalJobType.DISCOVER_SCHEMA, TemporalJobType.CHECK_CONNECTION);

  public RouterService(final ConfigRepository configRepository, final TaskQueueMapper taskQueueMapper, final FeatureFlagClient featureFlagClient) {
    this.configRepository = configRepository;
    this.taskQueueMapper = taskQueueMapper;
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Given a connectionId, look up the connection's configured {@link Geography} in the config DB and
   * use it to determine which Task Queue should be used for this connection's sync.
   */
  public String getTaskQueue(final UUID connectionId, final TemporalJobType jobType) throws IOException, ConfigNotFoundException {
    final Geography geography = configRepository.getGeographyForConnection(connectionId);
    final UUID workspaceId = configRepository.getStandardWorkspaceFromConnection(connectionId, false).getWorkspaceId();
    if (featureFlagClient.boolVariation(ShouldRunOnGkeDataplane.INSTANCE, new Workspace(workspaceId))) {
      if (featureFlagClient.boolVariation(ShouldRunOnExpandedGkeDataplane.INSTANCE, new Workspace(workspaceId))) {
        return taskQueueMapper.getTaskQueueExpanded(geography, jobType);
      } else {
        return taskQueueMapper.getTaskQueueFlagged(geography, jobType);
      }
    } else {
      return taskQueueMapper.getTaskQueue(geography, jobType);
    }
  }

  /**
   * This function is only getting called for discover/check functions. Today (02.07) they are behind
   * feature flag so even the geography might be in EU they will still be directed to US.
   *
   * @param workspaceId workspace id
   * @param jobType job type
   * @return task queue
   * @throws IOException while interacting with temporal or db
   */
  public String getTaskQueueForWorkspace(final UUID workspaceId, final TemporalJobType jobType) throws IOException {
    if (!WORKSPACE_ROUTING_JOB_TYPE_SET.contains(jobType)) {
      throw new RuntimeException("Jobtype not expected to call - getTaskQueueForWorkspace - " + jobType);
    }

    final Geography geography = configRepository.getGeographyForWorkspace(workspaceId);
    if (featureFlagClient.boolVariation(ShouldRunOnGkeDataplane.INSTANCE, new Workspace(workspaceId))) {
      // Routing logic to route dataplane jobs to expanded dataplane
      if (featureFlagClient.boolVariation(ShouldRunOnExpandedGkeDataplane.INSTANCE, new Workspace(workspaceId))) {
        return taskQueueMapper.getTaskQueueExpanded(geography, jobType);
      } else {
        return taskQueueMapper.getTaskQueueFlagged(geography, jobType);
      }
    } else {
      return taskQueueMapper.getTaskQueue(geography, jobType);
    }

  }

}
