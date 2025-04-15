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
    val useFileTransfer = extractUseFileTransfer(replicationActivityInput)

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

  private fun extractUseFileTransfer(replicationActivityInput: ReplicationActivityInput): Boolean {
    if (replicationActivityInput.includesFiles == true) {
      return true
    }

    // TODO: Delete this introspection of connector configs once new file + metadata flow rolled out.
    val sourceConfiguration: SourceActorConfig = Jsons.`object`(replicationActivityInput.sourceConfiguration, SourceActorConfig::class.java)
    return sourceConfiguration.useFileTransfer ||
      (
        sourceConfiguration.deliveryMethod != null &&
          "use_file_transfer".equals(
            sourceConfiguration.deliveryMethod.deliveryType,
          )
      )
  }
}
