/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingFailureReason
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingSuccessOutcome
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.getMetricAttributes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class ApplyDefinitionMetricsHelperTest {
  @Test
  fun `get metric attributes for no op success`() {
    val successOutcome = DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED
    val attributes = getMetricAttributes(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, successOutcome)
    Assertions.assertEquals(2, attributes.size)
    Assertions.assertEquals("status", attributes[0].key)
    Assertions.assertEquals("ok", attributes[0].value)
    Assertions.assertEquals("outcome", attributes[1].key)
    Assertions.assertEquals(successOutcome.toString(), attributes[1].value)
  }

  @Test
  fun `get metric attributes for non no op success outcomes`() {
    val successOutcomes = DefinitionProcessingSuccessOutcome.entries.filter { it != DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED }
    successOutcomes.forEach { successOutcome ->
      val attributes = getMetricAttributes(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, successOutcome)
      Assertions.assertEquals(4, attributes.size, "Unexpected number of attributes for $successOutcome")
      Assertions.assertEquals("status", attributes[0].key)
      Assertions.assertEquals("ok", attributes[0].value)
      Assertions.assertEquals("outcome", attributes[1].key)
      Assertions.assertEquals(successOutcome.toString(), attributes[1].value)
      Assertions.assertEquals("docker_repository", attributes[2].key)
      Assertions.assertEquals(DOCKER_REPOSITORY, attributes[2].value)
      Assertions.assertEquals("docker_image_tag", attributes[3].key)
      Assertions.assertEquals(DOCKER_IMAGE_TAG, attributes[3].value)
    }
  }

  @Test
  fun `get metric attributes for failure outcomes`() {
    DefinitionProcessingFailureReason.entries.forEach { failureReason ->
      val attributes = getMetricAttributes(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, failureReason)
      Assertions.assertEquals(4, attributes.size)
      Assertions.assertEquals("status", attributes[0].key)
      Assertions.assertEquals("failed", attributes[0].value)
      Assertions.assertEquals("outcome", attributes[1].key)
      Assertions.assertEquals(failureReason.toString(), attributes[1].value)
      Assertions.assertEquals("docker_repository", attributes[2].key)
      Assertions.assertEquals(DOCKER_REPOSITORY, attributes[2].value)
      Assertions.assertEquals("docker_image_tag", attributes[3].key)
      Assertions.assertEquals(DOCKER_IMAGE_TAG, attributes[3].value)
    }
  }

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-test"
    private const val DOCKER_IMAGE_TAG = "1.0.0"
  }
}
