/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.WorkloadPriority;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import java.io.IOException;
import java.util.UUID;

/**
 * Exposes a way of executing short-lived jobs as RPC calls. Blocks until the job completes. No
 * metadata will be stored in the Jobs table for jobs triggered via this client.
 */
public interface SynchronousSchedulerClient {

  SynchronousResponse<StandardCheckConnectionOutput> createSourceCheckConnectionJob(SourceConnection source,
                                                                                    ActorDefinitionVersion sourceVersion,
                                                                                    boolean isCustomConnector,
                                                                                    ResourceRequirements actorDefinitionResourceRequirements)
      throws IOException;

  SynchronousResponse<StandardCheckConnectionOutput> createDestinationCheckConnectionJob(DestinationConnection destination,
                                                                                         ActorDefinitionVersion destinationVersion,
                                                                                         boolean isCustomConnector,
                                                                                         ResourceRequirements actorDefinitionResourceRequirements)
      throws IOException;

  SynchronousResponse<UUID> createDiscoverSchemaJob(SourceConnection source,
                                                    ActorDefinitionVersion sourceVersion,
                                                    boolean isCustomConnector,
                                                    ResourceRequirements actorDefinitionResourceRequirements,
                                                    WorkloadPriority priority)
      throws IOException;

  SynchronousResponse<UUID> createDestinationDiscoverJob(DestinationConnection destination,
                                                         StandardDestinationDefinition destinationDefinition,
                                                         ActorDefinitionVersion destinationVersion)
      throws IOException;

  SynchronousResponse<ConnectorSpecification> createGetSpecJob(String dockerImage, boolean isCustomConnector, final UUID workspaceId)
      throws IOException;

}
