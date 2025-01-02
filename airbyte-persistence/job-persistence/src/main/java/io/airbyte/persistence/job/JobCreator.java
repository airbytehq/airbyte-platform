/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.persistence.domain.StreamRefresh;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Abstraction to hide implementation of enqueuing a job.
 */
public interface JobCreator {

  /**
   * Create a sync job.
   *
   * @param source db model representing where data comes from
   * @param destination db model representing where data goes
   * @param standardSync sync options
   * @param sourceDockerImage docker image to use for the source
   * @param destinationDockerImage docker image to use for the destination
   * @param workspaceId workspace id
   * @return the new job if no other conflicting job was running, otherwise empty
   * @throws IOException if something wrong happens
   */
  Optional<Long> createSyncJob(SourceConnection source,
                               DestinationConnection destination,
                               StandardSync standardSync,
                               String sourceDockerImage,
                               Boolean sourceDockerImageIsDefault,
                               Version sourceProtocolVersion,
                               String destinationDockerImage,
                               Boolean destinationDockerImageIsDefault,
                               Version destinationProtocolVersion,
                               List<StandardSyncOperation> standardSyncOperations,
                               @Nullable JsonNode webhookOperationConfigs,
                               StandardSourceDefinition sourceDefinition,
                               StandardDestinationDefinition destinationDefinition,
                               ActorDefinitionVersion sourceDefinitionVersion,
                               ActorDefinitionVersion destinationDefinitionVersion,
                               UUID workspaceId)
      throws IOException;

  /**
   * Create a reset job.
   *
   * @param destination db model representing where data goes
   * @param standardSync sync options
   * @param destinationDockerImage docker image to use for the destination
   * @param streamsToReset select which streams should be reset
   * @return the new job if no other conflicting job was running, otherwise empty
   * @throws IOException if something wrong happens
   */
  Optional<Long> createResetConnectionJob(DestinationConnection destination,
                                          StandardSync standardSync,
                                          StandardDestinationDefinition destinationDefinition,
                                          ActorDefinitionVersion destinationDefinitionVersion,
                                          String destinationDockerImage,
                                          Version destinationProtocolVersion,
                                          boolean isCustom,
                                          List<StandardSyncOperation> standardSyncOperations,
                                          List<StreamDescriptor> streamsToReset,
                                          UUID workspaceId)
      throws IOException;

  Optional<Long> createRefreshConnection(final StandardSync standardSync,
                                         final String sourceDockerImageName,
                                         final Version sourceProtocolVersion,
                                         final String destinationDockerImageName,
                                         final Version destinationProtocolVersion,
                                         final List<StandardSyncOperation> standardSyncOperations,
                                         @Nullable final JsonNode webhookOperationConfigs,
                                         final StandardSourceDefinition sourceDefinition,
                                         final StandardDestinationDefinition destinationDefinition,
                                         final ActorDefinitionVersion sourceDefinitionVersion,
                                         final ActorDefinitionVersion destinationDefinitionVersion,
                                         final UUID workspaceId,
                                         final List<StreamRefresh> streamsToRefresh)
      throws IOException;

}
