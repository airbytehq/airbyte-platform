/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ConnectorRollout
import java.util.UUID

/**
 * A service that manages connector rollouts.
 */
interface ConnectorRolloutService {
  /**
   * Get a connector rollout by its id.
   */
  fun getConnectorRollout(id: UUID): ConnectorRollout

  /**
   * Insert a connector rollout.
   */
  fun insertConnectorRollout(connectorRollout: ConnectorRollout): ConnectorRollout

  /**
   * Write (create or update) a connector rollout.
   */
  fun writeConnectorRollout(connectorRollout: ConnectorRollout): ConnectorRollout

  /**
   * List all connector rollouts
   */
  fun listConnectorRollouts(): List<ConnectorRollout>

  /**
   * List all connector rollouts matching the provided actor definition ID
   */
  fun listConnectorRollouts(actorDefinitionId: UUID): List<ConnectorRollout>

  /**
   * List all connector rollouts matching the provided actor definition ID & release candidate versions
   */
  fun listConnectorRollouts(
    actorDefinitionId: UUID,
    releaseCandidateVersionId: UUID,
  ): List<ConnectorRollout>
}
