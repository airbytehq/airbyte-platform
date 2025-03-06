/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class PodNameGeneratorTest {
  private val namespace = "namespace"
  private lateinit var podNameGenerator: PodNameGenerator

  @BeforeEach
  internal fun setUp() {
    podNameGenerator = PodNameGenerator(namespace = namespace)
  }

  @Test
  internal fun testGetReplicationPodName() {
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getReplicationPodName(jobId = jobId, attemptId = attemptId)
    assertEquals("replication-job-$jobId-attempt-$attemptId", podName)
  }

  @Test
  internal fun testGetCheckPodName() {
    val image = "image-name"
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getCheckPodName(jobId = jobId, attemptId = attemptId, image = image)
    assertEquals(true, podName.matches("$image-check-$jobId-$attemptId-$RANDOM_SUFFIX_PATTERN".toRegex()))
  }

  @Test
  internal fun testGetDiscoverPodName() {
    val image = "image-name"
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getDiscoverPodName(jobId = jobId, attemptId = attemptId, image = image)
    assertEquals(true, podName.matches("$image-discover-$jobId-$attemptId-$RANDOM_SUFFIX_PATTERN".toRegex()))
  }

  @Test
  internal fun testGetSpecPodName() {
    val image = "image-name"
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getSpecPodName(jobId = jobId, attemptId = attemptId, image = image)
    assertEquals(true, podName.matches("$image-spec-$jobId-$attemptId-$RANDOM_SUFFIX_PATTERN".toRegex()))
  }

  companion object {
    const val RANDOM_SUFFIX_PATTERN = "[a-z]{5}"
  }
}
