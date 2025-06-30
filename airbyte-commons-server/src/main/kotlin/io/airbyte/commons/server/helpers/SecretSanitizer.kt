/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.processConfigSecrets
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class SecretSanitizer(
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val destinationService: DestinationService,
  private val sourceService: SourceService,
  private val secretPersistenceService: SecretPersistenceService,
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
  private val secretStorageService: SecretStorageService,
) {
  /**
   * Wrapper around {@link SecretsRepositoryWriter#createEphemeralFromConfig}.
   *
   * @param workspaceId workspaceId
   * @param connectionConfiguration connectionConfiguration
   * @param connectorSpecification connector specification
   * @return config with secrets replaced with secret json
   */
  fun sanitizePartialConfig(
    workspaceId: UUID,
    connectionConfiguration: JsonNode,
    connectorSpecification: ConnectorSpecification,
  ): JsonNode {
    val secretStorageId = secretStorageService.getByWorkspaceId(WorkspaceId(workspaceId))?.id
    val configWithProcessedSecrets = processConfigSecrets(connectionConfiguration, connectorSpecification.connectionSpecification, secretStorageId)
    val secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(WorkspaceId(workspaceId))
    return secretsRepositoryWriter.createEphemeralFromConfig(configWithProcessedSecrets, secretPersistence)
  }

  /**
   * Wrapper around {@link SecretsRepositoryWriter#createEphemeralFromConfig}.
   *
   * @param actorDefinitionId actorDefinitionId
   * @param workspaceId workspaceId
   * @param connectionConfiguration connectionConfiguration
   * @return config with secrets replaced with secret json
   */
  fun sanitizePartialConfig(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    connectionConfiguration: JsonNode,
  ): JsonNode {
    val connectionSpec = getConnectionSpec(actorDefinitionId, workspaceId)
    return sanitizePartialConfig(workspaceId, connectionConfiguration, connectionSpec)
  }

  private fun getConnectionSpec(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): ConnectorSpecification {
    try {
      return getSourceConnectionSpec(actorDefinitionId, workspaceId)
    } catch (e: ConfigNotFoundException) {
      return getDestinationConnectionSpec(actorDefinitionId, workspaceId)
    }
  }

  private fun getSourceConnectionSpec(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): ConnectorSpecification {
    val sourceDef = sourceService.getStandardSourceDefinition(actorDefinitionId)
    val sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, workspaceId)
    return sourceVersion.spec
  }

  private fun getDestinationConnectionSpec(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): ConnectorSpecification {
    val destDef = destinationService.getStandardDestinationDefinition(actorDefinitionId)
    val destVersion = actorDefinitionVersionHelper.getDestinationVersion(destDef, workspaceId)
    return destVersion.spec
  }
}
