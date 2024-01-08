/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
  void testGetSuccessAttributes() {
    final List<DefinitionProcessingSuccessOutcome> successOutcomes = Arrays.asList(DefinitionProcessingSuccessOutcome.values());
    successOutcomes.forEach((successOutcome) -> {
      final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getSuccessAttributes(successOutcome);
      assertEquals(2, attributes.length);
      assertEquals("status", attributes[0].key());
      assertEquals("ok", attributes[0].value());
      assertEquals("success_outcome", attributes[1].key());
      assertEquals(successOutcome.toString(), attributes[1].value());
    });
  }

  @Test
  void testGetFailureAttributes() {
    final List<DefinitionProcessingFailureReason> failureReasons = Arrays.asList(DefinitionProcessingFailureReason.values());
    failureReasons.forEach((failureReason) -> {
      final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getFailureAttributes("airbyte/source-test", failureReason);
      assertEquals(3, attributes.length);
      assertEquals("status", attributes[0].key());
      assertEquals("failed", attributes[0].value());
      assertEquals("failure_reason", attributes[1].key());
      assertEquals(failureReason.toString(), attributes[1].value());
      assertEquals("docker_repository", attributes[2].key());
      assertEquals("airbyte/source-test", attributes[2].value());
    });
  }

}
