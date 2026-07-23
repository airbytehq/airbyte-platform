/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.helpers.ResourceRequirementsUtils.getResourceRequirementsForJobType
import io.airbyte.config.helpers.ResourceRequirementsUtils.mergeResourceRequirements
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

/**
 * Auto converted to kotlin with some minor manual cleanup.
 */
internal class ResourceRequirementsUtilsTest {
  @Test
  fun testNoReqsSet() {
    val result =
      getResourceRequirementsForJobType(
        null,
        null,
        null,
        null,
        JobTypeResourceLimit.JobType.SYNC,
      )
    Assertions.assertEquals(ResourceRequirements(), result)
  }

  @Test
  fun testWorkerDefaultReqsSet() {
    val workerDefaultReqs = ResourceRequirements().withCpuRequest("1").withCpuLimit("1")
    val reqs =
      getResourceRequirementsForJobType(
        null,
        null,
        null,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )
    Assertions.assertEquals(workerDefaultReqs, reqs)
  }

  @Test
  fun testDefinitionDefaultReqsOverrideWorker() {
    val workerDefaultReqs = ResourceRequirements().withCpuRequest("1").withCpuLimit("1")
    val definitionDefaultReqs = ResourceRequirements().withCpuLimit("2").withMemoryRequest("100Mi")
    val definitionReqs = ScopedResourceRequirements().withDefault(definitionDefaultReqs)
    val result =
      getResourceRequirementsForJobType(
        null,
        null,
        definitionReqs,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )
    val expectedReqs =
      ResourceRequirements().withCpuRequest("1").withCpuLimit("2").withMemoryRequest("100Mi")
    Assertions.assertEquals(expectedReqs, result)
  }

  @Test
  fun testJobSpecificReqsOverrideDefault() {
    val workerDefaultReqs = ResourceRequirements().withCpuRequest("1").withCpuLimit("1")
    val definitionDefaultReqs = ResourceRequirements().withCpuLimit("2").withMemoryRequest("100Mi")
    val jobTypeResourceLimit =
      JobTypeResourceLimit()
        .withJobType(JobTypeResourceLimit.JobType.SYNC)
        .withResourceRequirements(
          ResourceRequirements()
            .withCpuRequest("2")
            .withMemoryRequest("200Mi")
            .withMemoryLimit("300Mi"),
        )
    val definitionReqs =
      ScopedResourceRequirements()
        .withDefault(definitionDefaultReqs)
        .withJobSpecific(listOf(jobTypeResourceLimit))
    val result =
      getResourceRequirementsForJobType(
        null,
        null,
        definitionReqs,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )
    val expectedReqs =
      ResourceRequirements()
        .withCpuRequest("2")
        .withCpuLimit("2")
        .withMemoryRequest("200Mi")
        .withMemoryLimit("300Mi")
    Assertions.assertEquals(expectedReqs, result)
  }

  @Test
  fun testConnectionResourceRequirementsOverrideDefault() {
    val workerDefaultReqs = ResourceRequirements().withCpuRequest("1")
    val definitionDefaultReqs = ResourceRequirements().withCpuLimit("2").withCpuRequest("2")
    val jobTypeResourceLimit =
      JobTypeResourceLimit()
        .withJobType(JobTypeResourceLimit.JobType.SYNC)
        .withResourceRequirements(
          ResourceRequirements().withCpuLimit("3").withMemoryRequest("200Mi"),
        )
    val definitionReqs =
      ScopedResourceRequirements()
        .withDefault(definitionDefaultReqs)
        .withJobSpecific(listOf(jobTypeResourceLimit))
    val connectionResourceRequirements =
      ResourceRequirements().withMemoryRequest("400Mi").withMemoryLimit(FIVE_HUNDRED_MEM)
    val result =
      getResourceRequirementsForJobType(
        connectionResourceRequirements,
        null,
        definitionReqs,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )
    val expectedReqs =
      ResourceRequirements()
        .withCpuRequest("2")
        .withCpuLimit("3")
        .withMemoryRequest("400Mi")
        .withMemoryLimit(FIVE_HUNDRED_MEM)
    Assertions.assertEquals(expectedReqs, result)
  }

  @Test
  fun testConnectionResourceRequirementsOverrideWorker() {
    val workerDefaultReqs =
      ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("1")
        .withEphemeralStorageLimit("2G")
        .withEphemeralStorageRequest("4G")
    val connectionResourceRequirements =
      ResourceRequirements()
        .withCpuLimit("2")
        .withMemoryLimit(FIVE_HUNDRED_MEM)
        .withEphemeralStorageLimit("1G")
        .withEphemeralStorageRequest("5G")
    val result = mergeResourceRequirements(connectionResourceRequirements, workerDefaultReqs)
    val expectedReqs =
      ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("2")
        .withMemoryLimit(FIVE_HUNDRED_MEM)
        .withEphemeralStorageLimit("1G")
        .withEphemeralStorageRequest("5G")
    Assertions.assertEquals(expectedReqs, result)
  }

  @Test
  fun `test actor job specific reqs override actor definition reqs`() {
    val workerDefaultReqs = ResourceRequirements().withCpuRequest("1").withCpuLimit("1")
    val definitionDefaultReqs = ResourceRequirements().withCpuLimit("2").withMemoryRequest("100Mi")

    val actorJobTypeResourceLimit =
      JobTypeResourceLimit()
        .withJobType(JobTypeResourceLimit.JobType.SYNC)
        .withResourceRequirements(
          ResourceRequirements()
            .withCpuRequest("3")
            .withMemoryRequest("300Mi")
            .withMemoryLimit("400Mi"),
        )

    val actorReqs = ScopedResourceRequirements().withJobSpecific(listOf(actorJobTypeResourceLimit))

    val definitionJobTypeResourceLimit =
      JobTypeResourceLimit()
        .withJobType(JobTypeResourceLimit.JobType.SYNC)
        .withResourceRequirements(
          ResourceRequirements()
            .withCpuRequest("2")
            .withMemoryRequest("200Mi")
            .withMemoryLimit("300Mi"),
        )

    val definitionReqs =
      ScopedResourceRequirements()
        .withDefault(definitionDefaultReqs)
        .withJobSpecific(listOf(definitionJobTypeResourceLimit))

    val result =
      getResourceRequirementsForJobType(
        null,
        actorReqs,
        definitionReqs,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )

    val expectedReqs =
      ResourceRequirements()
        .withCpuRequest("3")
        .withCpuLimit("2") // from definition default since actor didn't specify it
        .withMemoryRequest("300Mi")
        .withMemoryLimit("400Mi")

    Assertions.assertEquals(expectedReqs, result)
  }

  @Test
  fun `test full precedence order with all resource types`() {
    // 5. Worker default (lowest precedence)
    val workerDefaultReqs =
      ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("1")
        .withMemoryRequest("100Mi")
        .withMemoryLimit("100Mi")

    // 4. Actor definition default
    val definitionDefaultReqs = ResourceRequirements().withCpuLimit("2").withMemoryRequest("200Mi")

    // 3. Actor definition job-specific
    val definitionJobTypeResourceLimit =
      JobTypeResourceLimit()
        .withJobType(JobTypeResourceLimit.JobType.SYNC)
        .withResourceRequirements(
          ResourceRequirements().withCpuRequest("2").withMemoryLimit("300Mi"),
        )

    val definitionReqs =
      ScopedResourceRequirements()
        .withDefault(definitionDefaultReqs)
        .withJobSpecific(listOf(definitionJobTypeResourceLimit))

    // 2. Actor job-specific
    val actorJobTypeResourceLimit =
      JobTypeResourceLimit()
        .withJobType(JobTypeResourceLimit.JobType.SYNC)
        .withResourceRequirements(
          ResourceRequirements().withCpuRequest("3").withMemoryRequest("400Mi"),
        )

    val actorReqs = ScopedResourceRequirements().withJobSpecific(listOf(actorJobTypeResourceLimit))

    // 1. Connection-level (highest precedence)
    val connectionReqs = ResourceRequirements().withMemoryLimit("500Mi").withCpuLimit("4")

    val result =
      getResourceRequirementsForJobType(
        connectionReqs,
        actorReqs,
        definitionReqs,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )

    // Each value should come from the highest precedence source that specified it
    val expectedReqs =
      ResourceRequirements()
        // from actor job-specific
        .withCpuRequest("3")
        // from connection
        .withCpuLimit("4")
        // from actor job-specific
        .withMemoryRequest("400Mi")
        // from connection
        .withMemoryLimit("500Mi")

    Assertions.assertEquals(expectedReqs, result)
  }

  @Test
  fun `test actor job specific reqs for different job type are ignored`() {
    val actorJobTypeResourceLimit =
      JobTypeResourceLimit()
        .withJobType(JobTypeResourceLimit.JobType.RESET_CONNECTION)
        .withResourceRequirements(
          ResourceRequirements().withCpuRequest("3").withMemoryRequest("300Mi"),
        )

    val actorReqs = ScopedResourceRequirements().withJobSpecific(listOf(actorJobTypeResourceLimit))

    val workerDefaultReqs = ResourceRequirements().withCpuRequest("1").withMemoryRequest("100Mi")

    val result =
      getResourceRequirementsForJobType(
        null,
        actorReqs,
        null,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )

    // Should fall back to worker defaults since actor reqs were for different job type
    val expectedReqs = ResourceRequirements().withCpuRequest("1").withMemoryRequest("100Mi")

    Assertions.assertEquals(expectedReqs, result)
  }

  companion object {
    private const val FIVE_HUNDRED_MEM = "500Mi"
  }
}
