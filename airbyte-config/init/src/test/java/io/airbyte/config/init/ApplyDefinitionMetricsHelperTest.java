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

  @Test
  void testGetMetricAttributesForNoOpSuccess() {
    final DefinitionProcessingSuccessOutcome successOutcome = DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED;
    final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getMetricAttributes("airbyte/source-test", successOutcome);
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
          final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getMetricAttributes("airbyte/source-test", successOutcome);
          assertEquals(3, attributes.length);
          assertEquals("status", attributes[0].key());
          assertEquals("ok", attributes[0].value());
          assertEquals("outcome", attributes[1].key());
          assertEquals(successOutcome.toString(), attributes[1].value());
          assertEquals("docker_repository", attributes[2].key());
          assertEquals("airbyte/source-test", attributes[2].value());
        });
  }

  @Test
  void testGetMetricAttributesForFailureOutcomes() {
    final List<DefinitionProcessingFailureReason> failureReasons = Arrays.asList(DefinitionProcessingFailureReason.values());
    failureReasons.forEach((failureReason) -> {
      final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getMetricAttributes("airbyte/source-test", failureReason);
      assertEquals(3, attributes.length);
      assertEquals("status", attributes[0].key());
      assertEquals("failed", attributes[0].value());
      assertEquals("outcome", attributes[1].key());
      assertEquals(failureReason.toString(), attributes[1].value());
      assertEquals("docker_repository", attributes[2].key());
      assertEquals("airbyte/source-test", attributes[2].value());
    });
  }

}
