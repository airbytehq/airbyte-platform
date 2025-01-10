/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.helpers

import io.airbyte.config.ActorDefinitionResourceRequirements
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.helpers.ResourceRequirementsUtils.getResourceRequirements
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
      getResourceRequirements(
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
      getResourceRequirements(
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
    val definitionReqs = ActorDefinitionResourceRequirements().withDefault(definitionDefaultReqs)
    val result =
      getResourceRequirements(
        null,
        definitionReqs,
        workerDefaultReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )
    val expectedReqs =
      ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("2")
        .withMemoryRequest("100Mi")
    Assertions.assertEquals(expectedReqs, result)
  }

  @Test
  fun testJobSpecificReqsOverrideDefault() {
    val workerDefaultReqs = ResourceRequirements().withCpuRequest("1").withCpuLimit("1")
    val definitionDefaultReqs = ResourceRequirements().withCpuLimit("2").withMemoryRequest("100Mi")
    val jobTypeResourceLimit =
      JobTypeResourceLimit().withJobType(JobTypeResourceLimit.JobType.SYNC).withResourceRequirements(
        ResourceRequirements().withCpuRequest("2").withMemoryRequest("200Mi").withMemoryLimit("300Mi"),
      )
    val definitionReqs =
      ActorDefinitionResourceRequirements()
        .withDefault(definitionDefaultReqs)
        .withJobSpecific(listOf(jobTypeResourceLimit))
    val result =
      getResourceRequirements(
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
      JobTypeResourceLimit().withJobType(JobTypeResourceLimit.JobType.SYNC).withResourceRequirements(
        ResourceRequirements().withCpuLimit("3").withMemoryRequest("200Mi"),
      )
    val definitionReqs =
      ActorDefinitionResourceRequirements()
        .withDefault(definitionDefaultReqs)
        .withJobSpecific(listOf(jobTypeResourceLimit))
    val connectionResourceRequirements = ResourceRequirements().withMemoryRequest("400Mi").withMemoryLimit(FIVE_HUNDRED_MEM)
    val result =
      getResourceRequirements(
        connectionResourceRequirements,
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

  companion object {
    private const val FIVE_HUNDRED_MEM = "500Mi"
  }
}
