/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.workload.launcher.constants.PodConstants.KUBE_NAME_LEN_LIMIT
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

const val RANDOM_SUFFIX_PATTERN = "[a-z]{5}"

internal class PodNameGeneratorTest {
  private val namespace = "namespace"
  private lateinit var podNameGenerator: PodNameGenerator

  @BeforeEach
  fun setUp() {
    podNameGenerator = PodNameGenerator(namespace = namespace)
  }

  @Test
  fun testGetReplicationPodName() {
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getReplicationPodName(jobId = jobId, attemptId = attemptId)
    assertEquals("replication-job-$jobId-attempt-$attemptId", podName)
  }

  @Test
  fun testGetCheckPodName() {
    val image = "image-name"
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getCheckPodName(jobId = jobId, attemptId = attemptId, image = image)
    assertEquals(true, podName.matches("$image-check-$jobId-$attemptId-$RANDOM_SUFFIX_PATTERN".toRegex()))
  }

  @Test
  fun testGetDiscoverPodName() {
    val image = "image-name"
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getDiscoverPodName(jobId = jobId, attemptId = attemptId, image = image)
    assertEquals(true, podName.matches("$image-discover-$jobId-$attemptId-$RANDOM_SUFFIX_PATTERN".toRegex()))
  }

  @Test
  fun testGetSpecPodName() {
    val image = "image-name"
    val jobId = "12345"
    val attemptId = 0L
    val podName = podNameGenerator.getSpecPodName(jobId = jobId, attemptId = attemptId, image = image)
    assertEquals(true, podName.matches("$image-spec-$jobId-$attemptId-$RANDOM_SUFFIX_PATTERN".toRegex()))
  }

  @ParameterizedTest
  @CsvSource(
    value = [
      "hello:1,check,100,0,hello-check-100-0-",
      "registry.internal:1234/foo/bar:1,sync,1,3,bar-sync-1-3-",
      "really-really-really-long-name-to-cause-overflow,job-type,12345,6789,ly-really-long-name-to-cause-overflow-job-type-12345-6789-",
      "non_compliant/Image_Name:dev,discover,1,0,image-name-discover-1-0-",
    ],
  )
  fun testCreateProcessName(
    imagePath: String,
    jobType: String,
    jobId: String,
    attempt: Int,
    expectedPrefix: String,
  ) {
    val actual = createProcessName(imagePath, jobType, jobId, attempt)
    assertTrue(actual.startsWith(expectedPrefix), actual)
    assertEquals(expectedPrefix.length + 5, actual.length)
    assertTrue(actual.length <= KUBE_NAME_LEN_LIMIT)
  }

  @ParameterizedTest
  @CsvSource(
    value = [
      "hello:1,hello",
      "hello/world:2,world",
      "foo/bar/fizz/buzz:3,buzz",
      "hello,hello",
      "registry.internal:1234/foo/bar:1,bar",
    ],
  )
  fun testShortImageName(
    fullName: String,
    expected: String,
  ) {
    assertEquals(expected, shortImageName(fullName))
  }
}
