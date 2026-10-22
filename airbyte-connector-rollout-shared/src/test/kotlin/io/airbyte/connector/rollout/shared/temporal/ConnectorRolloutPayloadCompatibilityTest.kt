/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared.temporal

import com.google.protobuf.ByteString
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.converter.AirbyteTemporalDataConverter
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutWorkflowInput
import io.temporal.api.common.v1.Payload
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Connector rollout workflow inputs and activity outputs are stored in Temporal history and replayed by
 * running workflows. These fixtures are the JSON the Java-generated API models wrote, so a workflow
 * started before a model change must still decode, and re-encode to the same document.
 */
class ConnectorRolloutPayloadCompatibilityTest {
  private val converter = AirbyteTemporalDataConverter()

  @Test
  fun `a stored workflow input decodes and re-encodes unchanged`() {
    assertRoundTrip(WORKFLOW_INPUT, ConnectorRolloutWorkflowInput::class.java)
  }

  @Test
  fun `a stored activity output decodes and re-encodes unchanged`() {
    assertRoundTrip(ROLLOUT_OUTPUT, ConnectorRolloutOutput::class.java)
  }

  private fun <T> assertRoundTrip(
    json: String,
    type: Class<T>,
  ) {
    val payload =
      Payload
        .newBuilder()
        .putMetadata("encoding", ByteString.copyFromUtf8("json/plain"))
        .setData(ByteString.copyFromUtf8(json))
        .build()
    val decoded = converter.fromPayload(payload, type, type)
    val reencoded =
      converter
        .toPayload(decoded)
        .get()
        .data
        .toStringUtf8()
    assertEquals(Jsons.deserialize(json), Jsons.deserialize(reencoded))
  }

  companion object {
    private const val ACTOR_ID = "1f0b6c62-0c6b-4b5e-9a3f-6c2a8a3c1d10"

    private val ACTOR_SELECTION_INFO =
      """{"num_actors":10,"num_actors_eligible_or_already_pinned":8,"num_pinned_to_connector_rollout":3}"""

    private val ACTOR_SYNCS =
      """{"$ACTOR_ID":{"actor_id":"$ACTOR_ID","num_succeeded":4,"num_failed":1,"num_connections":2}}"""

    // as written by the Temporal data converter: the workflow classes write their null fields, the API
    // models omit theirs
    val WORKFLOW_INPUT =
      """
      {
        "dockerRepository": "airbyte/source-faker",
        "dockerImageTag": "6.2.0",
        "actorDefinitionId": "dfd88b22-b603-4c3d-aad7-3701784586b1",
        "rolloutId": "a8f2c1d4-1111-4b5e-9a3f-6c2a8a3c1d10",
        "updatedBy": null,
        "rolloutStrategy": null,
        "initialVersionDockerImageTag": null,
        "connectorRollout": null,
        "actorSelectionInfo": $ACTOR_SELECTION_INFO,
        "actorSyncs": $ACTOR_SYNCS,
        "migratePins": true,
        "waitBetweenRolloutSeconds": null,
        "waitBetweenSyncResultsQueriesSeconds": null,
        "rolloutExpirationSeconds": null
      }
      """.trimIndent()

    val ROLLOUT_OUTPUT =
      """
      {
        "id": "a8f2c1d4-1111-4b5e-9a3f-6c2a8a3c1d10",
        "workflowRunId": null,
        "actorDefinitionId": null,
        "releaseCandidateVersionId": null,
        "initialVersionId": null,
        "state": "in_progress",
        "initialRolloutPct": null,
        "currentTargetRolloutPct": 25,
        "finalTargetRolloutPct": null,
        "hasBreakingChanges": null,
        "rolloutStrategy": null,
        "maxStepWaitTimeMins": null,
        "updatedBy": null,
        "createdAt": null,
        "updatedAt": null,
        "completedAt": null,
        "expiresAt": null,
        "errorMsg": null,
        "failedReason": null,
        "pausedReason": null,
        "actorSelectionInfo": $ACTOR_SELECTION_INFO,
        "actorSyncs": $ACTOR_SYNCS
      }
      """.trimIndent()
  }
}
