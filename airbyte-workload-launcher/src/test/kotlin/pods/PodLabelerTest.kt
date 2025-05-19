/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.workers.pod.Metadata.CHECK_JOB
import io.airbyte.workers.pod.Metadata.DISCOVER_JOB
import io.airbyte.workers.pod.Metadata.IMAGE_NAME
import io.airbyte.workers.pod.Metadata.IMAGE_VERSION
import io.airbyte.workers.pod.Metadata.JOB_TYPE_KEY
import io.airbyte.workers.pod.Metadata.ORCHESTRATOR_REPLICATION_STEP
import io.airbyte.workers.pod.Metadata.READ_STEP
import io.airbyte.workers.pod.Metadata.REPLICATION_STEP
import io.airbyte.workers.pod.Metadata.SPEC_JOB
import io.airbyte.workers.pod.Metadata.SYNC_JOB
import io.airbyte.workers.pod.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.pod.Metadata.WRITE_STEP
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream
import io.airbyte.workload.launcher.pods.ORCHESTRATOR_IMAGE_NAME as REPL_ORCHESTRATOR_IMAGE_NAME

internal class PodLabelerTest {
  private lateinit var mPodNetworkSecurityLabeler: PodNetworkSecurityLabeler

  @BeforeEach
  fun setUp() {
    mPodNetworkSecurityLabeler = mockk()
    every { mPodNetworkSecurityLabeler.getLabels(any(), any()) } returns emptyMap()
  }

  @Test
  fun getSourceLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getSourceLabels()

    assertEquals(mapOf(SYNC_STEP_KEY to READ_STEP), result)
  }

  @Test
  fun getDestinationLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getDestinationLabels()

    assertEquals(mapOf(SYNC_STEP_KEY to WRITE_STEP), result)
  }

  @Test
  fun getReplicationOrchestratorLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getReplicationOrchestratorLabels(ORCHESTRATOR_IMAGE_NAME)

    val expected =
      mapOf(
        IMAGE_NAME to "an image",
        IMAGE_VERSION to "latest",
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to ORCHESTRATOR_REPLICATION_STEP,
      )
    assertEquals(expected, result)
  }

  @Test
  fun getCheckLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getCheckLabels()

    assertEquals(mapOf(JOB_TYPE_KEY to CHECK_JOB), result)
  }

  @Test
  fun getDiscoverLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getDiscoverLabels()

    assertEquals(mapOf(JOB_TYPE_KEY to DISCOVER_JOB), result)
  }

  @Test
  fun getSpecLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getSpecLabels()

    assertEquals(mapOf(JOB_TYPE_KEY to SPEC_JOB), result)
  }

  @ParameterizedTest
  @MethodSource("randomStringMatrix")
  fun getWorkloadLabels(workloadId: String) {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getWorkloadLabels(workloadId)

    assertEquals(mapOf(WORKLOAD_ID to workloadId), result)
  }

  @ParameterizedTest
  @MethodSource("randomStringMatrix")
  fun getMutexLabels(key: String) {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getMutexLabels(key)

    assertEquals(mapOf(MUTEX_KEY to key), result)
  }

  @Test
  fun getAutoIdLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val id = UUID.randomUUID()
    val result = labeler.getAutoIdLabels(id)

    assertEquals(mapOf(AUTO_ID to id.toString()), result)
  }

  @ParameterizedTest
  @MethodSource("replInputWorkloadIdMatrix")
  fun getSharedLabels(
    workloadId: String?,
    mutexKey: String?,
    passThroughLabels: Map<String, String>,
    autoId: UUID,
  ) {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val result = labeler.getSharedLabels(workloadId, mutexKey, passThroughLabels, autoId, null, emptyList())

    val expected =
      passThroughLabels +
        labeler.getWorkloadLabels(workloadId) +
        labeler.getMutexLabels(mutexKey) +
        labeler.getAutoIdLabels(autoId) +
        labeler.getPodSweeperLabels()

    assertEquals(expected, result)
  }

  @Test
  fun getSharedLabelsWithNetworkSecurityLabels() {
    val podNetworkSecurityLabeler: PodNetworkSecurityLabeler = mockk()
    val labeler = PodLabeler(podNetworkSecurityLabeler)
    val workloadId = UUID.randomUUID().toString()
    val mutexKey = UUID.randomUUID().toString()
    val passThroughLabels = mapOf("random labels1" to "from input msg1")
    val autoId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val networkSecurityTokens = listOf("token1")

    every { podNetworkSecurityLabeler.getLabels(workspaceId, networkSecurityTokens) } returns mapOf("networkSecurityTokenHash" to "hashedToken1")
    val result = labeler.getSharedLabels(workloadId, mutexKey, passThroughLabels, autoId, workspaceId, networkSecurityTokens)

    val expected =
      passThroughLabels +
        labeler.getWorkloadLabels(workloadId) +
        labeler.getMutexLabels(mutexKey) +
        labeler.getAutoIdLabels(autoId) +
        labeler.getPodSweeperLabels() +
        mapOf("networkSecurityTokenHash" to "hashedToken1")

    assertEquals(expected, result)
  }

  @Test
  internal fun testGetReplicationLabels() {
    val labeler = PodLabeler(mPodNetworkSecurityLabeler)
    val version = "dev"
    val orchestrationImageName = "orchestrator-image-name:$version"
    val sourceImageName = "source-image-name:$version"
    val destinationImageName = "destination-image-name:$version"
    val replicationLabels =
      labeler.getReplicationLabels(
        orchestratorImageName = orchestrationImageName,
        sourceImageName = sourceImageName,
        destImageName = destinationImageName,
      )
    assertEquals(8, replicationLabels.size)

    val expected =
      mapOf(
        REPL_ORCHESTRATOR_IMAGE_NAME to orchestrationImageName.replace(":$version", ""),
        ORCHESTRATOR_IMAGE_VERSION to version,
        SOURCE_IMAGE_NAME to sourceImageName.replace(":$version", ""),
        SOURCE_IMAGE_VERSION to version,
        DESTINATION_IMAGE_NAME to destinationImageName.replace(":$version", ""),
        DESTINATION_IMAGE_VERSION to version,
        JOB_TYPE_KEY to SYNC_JOB,
        SYNC_STEP_KEY to REPLICATION_STEP,
      )

    assertEquals(expected, replicationLabels)
  }

  companion object {
    const val ORCHESTRATOR_IMAGE_NAME: String = "an image"

    @JvmStatic
    private fun replInputWorkloadIdMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          mapOf("random labels1" to "from input msg1"),
          UUID.randomUUID().toString(),
        ),
        Arguments.of(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          mapOf("random labels2" to "from input msg2"),
          UUID.randomUUID().toString(),
        ),
        Arguments.of(
          UUID.randomUUID().toString(),
          null,
          mapOf("random labels3" to "from input msg3"),
          UUID.randomUUID().toString(),
        ),
        Arguments.of(
          null,
          null,
          mapOf("random labels3" to "from input msg3"),
          UUID.randomUUID().toString(),
        ),
      )

    @JvmStatic
    private fun randomStringMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of("random string id 1"),
        Arguments.of("RANdoM strIng Id 2"),
        Arguments.of("literally anything"),
        Arguments.of("89127421"),
        Arguments.of("false"),
        Arguments.of("{}"),
      )
  }
}
