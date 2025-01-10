/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.input

import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceActorConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.ReplicationActivityInput
import jakarta.inject.Singleton

/**
 * Converts ReplicationActivityInput to ReplicationInput by mapping basic files. Does NOT perform
 * any hydration. Does not copy unhydrated config.
 */
@Singleton
class ReplicationInputMapper {
  fun toReplicationInput(replicationActivityInput: ReplicationActivityInput): ReplicationInput {
    // TODO: Remove any introspection of connector configs. Determine whether to use 'file transfer' mode another way.
    val sourceConfiguration: SourceActorConfig = Jsons.`object`(replicationActivityInput.sourceConfiguration, SourceActorConfig::class.java)
    val useFileTransfer =
      sourceConfiguration.useFileTransfer || (
        sourceConfiguration.deliveryMethod != null &&
          "use_file_transfer".equals(
            sourceConfiguration.deliveryMethod.deliveryType,
          )
      )

    return ReplicationInput()
      .withNamespaceDefinition(replicationActivityInput.namespaceDefinition)
      .withNamespaceFormat(replicationActivityInput.namespaceFormat)
      .withPrefix(replicationActivityInput.prefix)
      .withSourceId(replicationActivityInput.sourceId)
      .withDestinationId(replicationActivityInput.destinationId)
      .withSyncResourceRequirements(replicationActivityInput.syncResourceRequirements)
      .withWorkspaceId(replicationActivityInput.workspaceId)
      .withConnectionId(replicationActivityInput.connectionId)
      .withIsReset(replicationActivityInput.isReset)
      .withJobRunConfig(replicationActivityInput.jobRunConfig)
      .withSourceLauncherConfig(replicationActivityInput.sourceLauncherConfig)
      .withDestinationLauncherConfig(replicationActivityInput.destinationLauncherConfig)
      .withSignalInput(replicationActivityInput.signalInput)
      .withSourceConfiguration(replicationActivityInput.sourceConfiguration)
      .withDestinationConfiguration(replicationActivityInput.destinationConfiguration)
      .withConnectionContext(replicationActivityInput.connectionContext)
      .withUseFileTransfer(useFileTransfer)
      .withNetworkSecurityTokens(replicationActivityInput.networkSecurityTokens)
  }
}
