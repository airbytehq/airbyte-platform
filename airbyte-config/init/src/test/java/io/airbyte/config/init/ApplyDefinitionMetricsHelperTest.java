/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingFailureReason;
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingSuccessOutcome;
import io.airbyte.metrics.lib.MetricAttribute;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ApplyDefinitionMetricsHelperTest {

  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "1.0.0";

  @Test
  void testGetMetricAttributesForNoOpSuccess() {
    final DefinitionProcessingSuccessOutcome successOutcome = DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED;
    final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getMetricAttributes(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, successOutcome);
    assertEquals(2, attributes.length);
    assertEquals("status", attributes[0].key());
    assertEquals("ok", attributes[0].value());
    assertEquals("outcome", attributes[1].key());
    assertEquals(successOutcome.toString(), attributes[1].value());
  }

  @Test
  void testGetMetricAttributesForNonNoOpSuccessOutcomes() {
    final List<DefinitionProcessingSuccessOutcome> successOutcomes = Arrays.asList(DefinitionProcessingSuccessOutcome.values());
    successOutcomes.stream().filter((successOutcome) -> !successOutcome.equals(DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED))
        .forEach((successOutcome) -> {
          final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getMetricAttributes(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, successOutcome);
          assertEquals(4, attributes.length);
          assertEquals("status", attributes[0].key());
          assertEquals("ok", attributes[0].value());
          assertEquals("outcome", attributes[1].key());
          assertEquals(successOutcome.toString(), attributes[1].value());
          assertEquals("docker_repository", attributes[2].key());
          assertEquals(DOCKER_REPOSITORY, attributes[2].value());
          assertEquals("docker_image_tag", attributes[3].key());
          assertEquals(DOCKER_IMAGE_TAG, attributes[3].value());
        });
  }

  @Test
  void testGetMetricAttributesForFailureOutcomes() {
    final List<DefinitionProcessingFailureReason> failureReasons = Arrays.asList(DefinitionProcessingFailureReason.values());
    failureReasons.forEach((failureReason) -> {
      final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getMetricAttributes(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, failureReason);
      assertEquals(4, attributes.length);
      assertEquals("status", attributes[0].key());
      assertEquals("failed", attributes[0].value());
      assertEquals("outcome", attributes[1].key());
      assertEquals(failureReason.toString(), attributes[1].value());
      assertEquals("docker_repository", attributes[2].key());
      assertEquals(DOCKER_REPOSITORY, attributes[2].value());
      assertEquals("docker_image_tag", attributes[3].key());
      assertEquals(DOCKER_IMAGE_TAG, attributes[3].value());
    });
  }

}
